# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Lakehouse & Entity Resolution
# MAGIC
# MAGIC A single, runnable notebook that builds both proofs of concept end to end:
# MAGIC
# MAGIC - **Part 1 — Medallion pipeline** over `customers` and `transactions` (bronze → silver → gold).
# MAGIC - **Part 2 — Entity resolution** over a third `contacts` source, producing golden records.
# MAGIC
# MAGIC **Prerequisites (do these first):**
# MAGIC 1. Run `provision_azure.sh` to create the Azure resources.
# MAGIC 2. In the workspace UI, create the storage **credential** (`cred_poc`) and **external location** (`loc_poc_landing`).
# MAGIC 3. Upload `customers.csv`, `transactions.csv`, and `contacts.csv` to the landing volume (created below).
# MAGIC
# MAGIC **Compute:** serverless notebook compute is sufficient. Requires **DBR 14.3 LTS or later**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Unity Catalog objects and shared setup
# MAGIC
# MAGIC Create the catalog, one schema per medallion layer, and the external volume that
# MAGIC notebooks read from. Then define the shared imports and landing path used everywhere below.

# COMMAND ----------

# Create the catalog, schemas, and the external landing volume.
spark.sql("CREATE CATALOG IF NOT EXISTS poc MANAGED LOCATION 'abfss://poc-landing@stpoclakehouse.dfs.core.windows.net/managed/poc'")

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS poc.{schema}")

spark.sql("""
CREATE EXTERNAL VOLUME IF NOT EXISTS poc.bronze.landing
LOCATION 'abfss://poc-landing@stpoclakehouse.dfs.core.windows.net/landing'
""")

# COMMAND ----------

# Shared imports and the landing path.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

LANDING = "/Volumes/poc/bronze/landing"

# Confirm the three source files are present.
display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1 — The Medallion Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Bronze: raw ingestion
# MAGIC
# MAGIC Make a faithful copy of each source with `inferSchema=False` (everything stays text), adding
# MAGIC minimal lineage columns. Nothing is interpreted, typed, deduplicated, or fixed here.

# COMMAND ----------

def ingest_bronze(file_name: str, target_table: str):
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)          # everything stays as text
        .load(f"{LANDING}/{file_name}")
        .withColumn("_source_file", F.col("_metadata.file_name"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").saveAsTable(target_table)
    return df

ingest_bronze("customers.csv",    "poc.bronze.customers_raw")
ingest_bronze("transactions.csv", "poc.bronze.transactions_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Silver: cleanse, type, deduplicate, flag
# MAGIC
# MAGIC Silver makes the data trustworthy: trimmed, typed, deduplicated, with quality problems
# MAGIC **flagged** rather than deleted.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 A reusable date parser
# MAGIC Tries ISO first, then Australian `dd/MM/yyyy`. Invalid dates (e.g. 31 Feb) become NULL.

# COMMAND ----------

def parse_date(col_name: str):
    """Try ISO first, then Australian d/m/y. Anything invalid becomes NULL."""
    return F.coalesce(
        F.try_to_date(F.col(col_name), F.lit("yyyy-MM-dd")),
        F.try_to_date(F.col(col_name), F.lit("dd/MM/yyyy")),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Silver customers
# MAGIC Trim / normalise casing / type columns, deduplicate to the latest row per `customer_id`,
# MAGIC and add `is_valid_email` and `has_valid_dob` flags.

# COMMAND ----------

typed = (
    spark.table("poc.bronze.customers_raw")
    .select(
        F.upper(F.trim("customer_id")).alias("customer_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        parse_date("dob").alias("dob"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("state")).alias("state"),
        F.try_to_timestamp(F.col("created_at")).alias("created_at"),
        "_source_file", "_ingested_at",
    )
)

w = Window.partitionBy("customer_id").orderBy(
    F.col("created_at").desc(), F.col("_ingested_at").desc()
)

silver_customers = (
    typed
    .withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .drop("rn")
    .withColumn("is_valid_email",
                F.col("email").isNotNull()
                & F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
    .withColumn("has_valid_dob", F.col("dob").isNotNull())
)

silver_customers.write.mode("overwrite").saveAsTable("poc.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Silver transactions
# MAGIC Type amounts as `decimal`, tidy the channel vocabulary, deduplicate, and flag referential
# MAGIC integrity (`has_known_customer`) and business validity (`is_valid_amount`).

# COMMAND ----------

typed_txn = (
    spark.table("poc.bronze.transactions_raw")
    .select(
        F.upper(F.trim("txn_id")).alias("txn_id"),
        F.upper(F.trim("customer_id")).alias("customer_id"),
        parse_date("txn_date").alias("txn_date"),
        F.col("amount").try_cast("decimal(12,2)").alias("amount"),
        F.upper(F.trim("currency")).alias("currency"),
        F.lower(F.trim("channel")).alias("channel"),   # tidy to: online | branch | mobile
        "_source_file", "_ingested_at",
    )
)

w_txn = Window.partitionBy("txn_id").orderBy(F.col("_ingested_at").desc())
known_customers = spark.table("poc.silver.customers").select("customer_id")

silver_txns = (
    typed_txn
    .withColumn("rn", F.row_number().over(w_txn))
    .where("rn = 1").drop("rn")
    .join(known_customers.withColumn("_known", F.lit(True)), "customer_id", "left")
    .withColumn("has_known_customer", F.coalesce("_known", F.lit(False)))
    .drop("_known")
    .withColumn("is_valid_amount",
                F.col("amount").isNotNull() & (F.col("amount") > 0))
)

silver_txns.write.mode("overwrite").saveAsTable("poc.silver.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Gold: curation
# MAGIC
# MAGIC Gold shapes the data for consumption: a conformed **dimension** and a pre-aggregated **fact**.
# MAGIC This is the one place invalid rows are excluded.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Customer dimension
# MAGIC Business-named fields computed once (`customer_name`, `age_band`); invalid emails withheld;
# MAGIC engineering flags dropped.

# COMMAND ----------

age_years = F.floor(F.datediff(F.current_date(), F.col("dob")) / 365.25)

dim_customer = (
    spark.table("poc.silver.customers")
    .select(
        "customer_id",
        F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
        "dob",
        F.when(F.col("dob").isNull(), "Unknown")
         .when(age_years < 30, "Under 30")
         .when(age_years < 50, "30–49")
         .otherwise("50+").alias("age_band"),
        "state",
        F.when(F.col("is_valid_email"), F.col("email")).alias("email"),
    )
)
dim_customer.write.mode("overwrite").saveAsTable("poc.gold.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Monthly-spend fact table
# MAGIC The `where` clause is the single, visible place invalid rows are removed. Grain: one row per
# MAGIC `customer × month × channel`.

# COMMAND ----------

fct_monthly_spend = (
    spark.table("poc.silver.transactions")
    .where("has_known_customer AND is_valid_amount AND txn_date IS NOT NULL")
    .groupBy("customer_id",
             F.date_trunc("month", "txn_date").alias("txn_month"),
             "channel")
    .agg(F.count("*").alias("txn_count"),
         F.sum("amount").alias("total_spend"))
)
fct_monthly_spend.write.mode("overwrite").saveAsTable("poc.gold.fct_monthly_spend")

# COMMAND ----------

# MAGIC %md
# MAGIC **Optional:** the rows excluded from gold are still fully queryable in silver.

# COMMAND ----------

display(spark.table("poc.silver.transactions")
        .where("NOT (has_known_customer AND is_valid_amount)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Consumption: the AI/BI dashboard
# MAGIC
# MAGIC Dashboards are built in the UI (**Dashboards → Create dashboard**). Create a dataset from the
# MAGIC SQL below — note it is a *pure join*, since all business logic already lives in the pipeline.
# MAGIC Then add charts: spend by state, spend over time by channel, customers ranked by spend.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dashboard dataset (preview here; use the same query in the dashboard's Data tab)
# MAGIC SELECT d.customer_name, d.state, d.age_band,
# MAGIC        f.txn_month, f.channel, f.txn_count, f.total_spend
# MAGIC FROM poc.gold.fct_monthly_spend f
# MAGIC JOIN poc.gold.dim_customer d USING (customer_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2 — Entity Resolution

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Ingest the third table (bronze + silver)
# MAGIC
# MAGIC Onboarding a new source reuses the same bronze/silver pattern — no new concepts. Silver stays
# MAGIC source-aligned (contacts remain a separate table from customers).

# COMMAND ----------

ingest_bronze("contacts.csv", "poc.bronze.contacts_raw")

w_ct = Window.partitionBy("contact_id").orderBy(F.col("_ingested_at").desc())

silver_contacts = (
    spark.table("poc.bronze.contacts_raw")
    .select(
        F.upper(F.trim("contact_id")).alias("contact_id"),
        F.trim("full_name").alias("full_name"),
        F.try_to_date(F.col("date_of_birth"), F.lit("yyyy-MM-dd")).alias("dob"),
        F.lower(F.trim("email")).alias("email"),
        F.initcap(F.trim("suburb")).alias("suburb"),
        F.upper(F.trim("state")).alias("state"),
        "_source_file", "_ingested_at",
    )
    .withColumn("rn", F.row_number().over(w_ct)).where("rn = 1").drop("rn")
)
silver_contacts.write.mode("overwrite").saveAsTable("poc.silver.contacts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — ER Stage 1: Standardisation
# MAGIC
# MAGIC Bring both systems into one common `person_source` schema. Names are uppercased, stripped of
# MAGIC punctuation, split into tokens, **sorted alphabetically**, and rejoined — which is what makes
# MAGIC different name orders comparable. Both raw and standardised names are kept.

# COMMAND ----------

def standardise_name(col_name: str):
    """UPPER, strip punctuation, split, drop empties, sort tokens, rejoin."""
    tokens = F.split(F.upper(F.regexp_replace(F.col(col_name), "[^A-Za-z ]", "")), " +")
    tokens = F.filter(tokens, lambda t: F.length(t) > 0)
    return F.array_join(F.array_sort(tokens), " ")

crm = (
    spark.table("poc.silver.customers")
    .select(
        F.lit("CRM").alias("source"),
        F.col("customer_id").alias("source_id"),
        F.concat_ws(" ", "first_name", "last_name").alias("raw_name"),
        "dob",
        F.when(F.col("is_valid_email"), F.col("email")).alias("email"),
        "state",
    )
)

case_sys = (
    spark.table("poc.silver.contacts")
    .select(
        F.lit("CASE").alias("source"),
        F.col("contact_id").alias("source_id"),
        F.col("full_name").alias("raw_name"),
        "dob", "email", "state",
    )
)

person_source = (
    crm.unionByName(case_sys)
    .withColumn("name_std", standardise_name("raw_name"))
)
person_source.write.mode("overwrite").saveAsTable("poc.silver.person_source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — ER Stages 2–4: Candidates, scoring, match rules
# MAGIC
# MAGIC Nominate candidate pairs (shared email, shared DOB, or overlapping soundex codes), score name
# MAGIC similarity with normalised Levenshtein, then apply ordered deterministic rules. The scored
# MAGIC pairs table doubles as the ER audit log.

# COMMAND ----------

ps = spark.table("poc.silver.person_source")

a = ps.where("source = 'CRM'").select(
    F.col("source_id").alias("crm_id"),  F.col("name_std").alias("crm_name"),
    F.col("dob").alias("crm_dob"),       F.col("email").alias("crm_email"))

b = ps.where("source = 'CASE'").select(
    F.col("source_id").alias("case_id"), F.col("name_std").alias("case_name"),
    F.col("dob").alias("case_dob"),      F.col("email").alias("case_email"))

def soundex_tokens(col_name: str):
    return F.transform(F.split(F.col(col_name), " "), lambda t: F.soundex(t))

candidate_cond = (
    (a["crm_email"].isNotNull() & (a["crm_email"] == b["case_email"]))
    | (a["crm_dob"].isNotNull() & (a["crm_dob"] == b["case_dob"]))
    | F.arrays_overlap(soundex_tokens("crm_name"), soundex_tokens("case_name"))
)

name_sim = F.round(
    1 - F.levenshtein("crm_name", "case_name")
        / F.greatest(F.length("crm_name"), F.length("case_name")),
    3,
)

scored_pairs = (
    a.join(b, candidate_cond)
    .withColumn("name_sim", name_sim)
    .withColumn(
        "match_decision",
        F.when(a["crm_email"].isNotNull() & (F.col("crm_email") == F.col("case_email")),
               "MATCH: exact email")
         .when((F.col("crm_dob") == F.col("case_dob")) & (F.col("name_sim") >= 0.70),
               "MATCH: dob + fuzzy name")
         .when(F.col("name_sim") >= 0.85, "REVIEW: name only")
         .otherwise("NO MATCH"),
    )
)
scored_pairs.write.mode("overwrite").saveAsTable("poc.silver.er_scored_pairs")

# COMMAND ----------

# Inspect the pair decisions (name/DOB/email evidence and the rule that fired).
display(spark.table("poc.silver.er_scored_pairs").orderBy("case_id", F.col("name_sim").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — ER Stage 5: Clustering, cross-reference, golden record
# MAGIC
# MAGIC Anchor clustering: every CRM customer mints an entity, a matched contact inherits its
# MAGIC customer's entity, and an unmatched contact mints its own. Two outputs: `entity_xref`
# MAGIC (record → entity mapping) and `entity_master` (one golden record per entity via survivorship).

# COMMAND ----------

# Best MATCH per contact (defensive: if a contact matched two customers, keep highest similarity).
w_best = Window.partitionBy("case_id").orderBy(F.col("name_sim").desc())

resolved = (
    spark.table("poc.silver.er_scored_pairs")
    .where(F.col("match_decision").startswith("MATCH"))
    .withColumn("rn", F.row_number().over(w_best))
    .where("rn = 1")
    .select("case_id", "crm_id")
)

ps = spark.table("poc.silver.person_source")

xref_crm = (
    ps.where("source = 'CRM'")
    .select(F.concat(F.lit("E-"), "source_id").alias("entity_id"), "source", "source_id")
)

xref_case = (
    ps.where("source = 'CASE'")
    .join(resolved, ps["source_id"] == resolved["case_id"], "left")
    .select(
        F.coalesce(F.concat(F.lit("E-"), "crm_id"),
                   F.concat(F.lit("E-"), "source_id")).alias("entity_id"),
        "source", "source_id",
    )
)

entity_xref = xref_crm.unionByName(xref_case)
entity_xref.write.mode("overwrite").saveAsTable("poc.gold.entity_xref")

# COMMAND ----------

# Golden record with survivorship rules: prefer the CRM display name, keep both name variants,
# union emails and states, take the known DOB, and count contributing records.
crm_name_agg  = F.max(F.when(F.col("source") == "CRM",
                             F.initcap(F.lower("raw_name")))).alias("crm_name")
case_name_agg = F.max(F.when(F.col("source") == "CASE",
                             F.initcap(F.lower("raw_name")))).alias("case_name")

entity_master = (
    spark.table("poc.gold.entity_xref")
    .join(ps, ["source", "source_id"])
    .groupBy("entity_id")
    .agg(
        crm_name_agg,
        case_name_agg,
        F.max("dob").alias("dob"),
        F.array_distinct(F.collect_list("email")).alias("emails"),
        F.array_distinct(F.collect_list("state")).alias("states"),
        F.collect_list(F.struct(F.col("source").alias("source"),
                                F.col("source_id").alias("id"))).alias("source_records"),
        F.count("*").alias("record_count"),
    )
    .withColumn("display_name", F.coalesce("crm_name", "case_name"))
)
entity_master.write.mode("overwrite").saveAsTable("poc.gold.entity_master")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Consumption: entity search
# MAGIC
# MAGIC A search over the golden records that also looks at both name variants and all emails.
# MAGIC First a Python helper, then the same logic registered as a governed Unity Catalog function so
# MAGIC dashboards and Genie can reuse it.

# COMMAND ----------

def search_entity(term: str):
    t_up, t_low = term.upper(), term.lower()
    return (
        spark.table("poc.gold.entity_master")
        .where(
            F.upper("display_name").contains(t_up)
            | F.upper(F.coalesce("crm_name",  F.lit(""))).contains(t_up)
            | F.upper(F.coalesce("case_name", F.lit(""))).contains(t_up)
            | F.exists("emails", lambda e: e.contains(t_low))
        )
        .select("entity_id", "display_name", "dob", "emails",
                "record_count", "source_records")
    )

display(search_entity("smith"))
display(search_entity("nguyen"))
display(search_entity("king"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Register the same search as a reusable Unity Catalog table function.
# MAGIC CREATE OR REPLACE FUNCTION poc.gold.search_entity(search_term STRING)
# MAGIC RETURNS TABLE (entity_id STRING, display_name STRING, dob DATE,
# MAGIC                emails ARRAY<STRING>, record_count BIGINT,
# MAGIC                source_records ARRAY<STRUCT<source: STRING, id: STRING>>)
# MAGIC RETURN
# MAGIC   SELECT entity_id, display_name, dob, emails, record_count, source_records
# MAGIC   FROM poc.gold.entity_master
# MAGIC   WHERE contains(upper(display_name), upper(search_term))
# MAGIC      OR contains(upper(coalesce(crm_name,  '')), upper(search_term))
# MAGIC      OR contains(upper(coalesce(case_name, '')), upper(search_term))
# MAGIC      OR exists(emails, e -> contains(e, lower(search_term)))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the function.
# MAGIC SELECT * FROM poc.gold.search_entity('smith');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Consuming the function: dashboard search box and Genie
# MAGIC
# MAGIC These are UI steps, both powered by the one `poc.gold.search_entity` function.
# MAGIC
# MAGIC **Dashboard search box:** create a dashboard, add a dataset `SELECT * FROM poc.gold.search_entity(:search_box)`,
# MAGIC give `:search_box` a default of `''`, bind a text **Filter** widget to the parameter, and show the
# MAGIC results in a **Table** widget.
# MAGIC
# MAGIC **Genie space:** create a Genie space, point it at `poc.gold.entity_master`, add `poc.gold.search_entity`
# MAGIC as an available function, and instruct it to use that function for name/email lookups. Then ask in
# MAGIC plain English, e.g. *"find people named Smith"*.
