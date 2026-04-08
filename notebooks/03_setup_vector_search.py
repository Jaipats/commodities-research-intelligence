# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search Setup
# MAGIC Creates embeddings for news articles and sets up a Databricks Vector Search index
# MAGIC for semantic retrieval. Run AFTER notebooks 01 and 02.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

CATALOG = "commodities_research_catalog"
SCHEMA = "news_research"
VS_ENDPOINT = "commodities_vs_endpoint"
VS_INDEX = f"{CATALOG}.{SCHEMA}.news_articles_index"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.news_articles"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Source Table with Text Column for Embedding

# COMMAND ----------

# Create a combined text column for embedding
spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.news_articles_vs AS
    SELECT
        article_id,
        source,
        headline,
        body,
        CONCAT(
            'Source: ', source, '. ',
            'Headline: ', headline, '. ',
            'Sector: ', sector, '. ',
            'Region: ', region, '. ',
            'Commodity: ', primary_commodity, '. ',
            'Article: ', body
        ) AS content_for_embedding,
        published_at,
        primary_commodity,
        region,
        sector,
        sentiment_score,
        relevance_score
    FROM {SOURCE_TABLE}
""")

# Enable Change Data Feed for Delta Sync
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.news_articles_vs
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

row_count = spark.table(f"{CATALOG}.{SCHEMA}.news_articles_vs").count()
print(f"Prepared {row_count} articles for vector search")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint

# COMMAND ----------

vsc = VectorSearchClient()

# Check if endpoint exists, create if not
try:
    endpoint_info = vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint '{VS_ENDPOINT}' already exists. Status: {endpoint_info.get('endpoint_status', {}).get('state')}")
except Exception:
    print(f"Creating vector search endpoint: {VS_ENDPOINT}")
    vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    print("Endpoint creation initiated. Waiting for it to be ready...")

# Wait for endpoint to be ready
for i in range(60):
    try:
        endpoint_info = vsc.get_endpoint(VS_ENDPOINT)
        state = endpoint_info.get("endpoint_status", {}).get("state", "UNKNOWN")
        if state == "ONLINE":
            print(f"Endpoint is ONLINE!")
            break
        print(f"  [{i+1}/60] Endpoint state: {state}. Waiting...")
    except Exception as e:
        print(f"  [{i+1}/60] Checking... {e}")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Index

# COMMAND ----------

# Check if index exists
try:
    index_info = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    print(f"Index already exists: {VS_INDEX}")
    print(f"Status: {index_info.describe()}")
except Exception:
    print(f"Creating vector search index: {VS_INDEX}")
    vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=f"{CATALOG}.{SCHEMA}.news_articles_vs",
        pipeline_type="TRIGGERED",
        primary_key="article_id",
        embedding_source_column="content_for_embedding",
        embedding_model_endpoint_name="databricks-gte-large-en"
    )
    print("Index creation initiated!")

# COMMAND ----------

# Wait for index to be ready
for i in range(60):
    try:
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
        status = index.describe().get("status", {})
        state = status.get("ready", False)
        if state:
            print(f"Index is READY!")
            break
        detailed = status.get("detailed_state", "UNKNOWN")
        print(f"  [{i+1}/60] Index state: {detailed}. Waiting...")
    except Exception as e:
        print(f"  [{i+1}/60] Checking... {e}")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Vector Search

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

# Test query
results = index.similarity_search(
    query_text="oil prices impact from Strait of Hormuz crisis",
    columns=["article_id", "headline", "source", "primary_commodity", "region", "sector"],
    num_results=5
)

print("Test Query: 'oil prices impact from Strait of Hormuz crisis'")
print("=" * 80)
for row in results.get("result", {}).get("data_array", []):
    print(f"  [{row[5]}] {row[2]}: {row[1]}")
    print(f"    Commodity: {row[3]} | Region: {row[4]}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC The following resources have been created:
# MAGIC - **Vector Search Endpoint**: `commodities_vs_endpoint`
# MAGIC - **Vector Search Index**: `commodities_research_catalog.news_research.news_articles_index`
# MAGIC - **Source Table**: `commodities_research_catalog.news_research.news_articles_vs`
# MAGIC
# MAGIC The index uses `databricks-gte-large-en` for embeddings and supports semantic search
# MAGIC over all news articles with their full content.
