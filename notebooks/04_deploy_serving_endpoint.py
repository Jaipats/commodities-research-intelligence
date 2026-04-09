# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Research API as Model Serving Endpoint
# MAGIC Creates a custom MLflow pyfunc model that wraps the full commodities research
# MAGIC pipeline and deploys it as a Databricks Model Serving endpoint.
# MAGIC
# MAGIC **Endpoint Input:**
# MAGIC ```json
# MAGIC {"dataframe_split": {"columns": ["breaking_news"], "data": [["OPEC+ cuts production by 2M bpd"]]}}
# MAGIC ```
# MAGIC
# MAGIC **Endpoint Output:** JSON with research_paper, articles, supply_chain, prices, metadata

# COMMAND ----------

# MAGIC %pip install mlflow databricks-sdk databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import json
import os
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

# COMMAND ----------

CATALOG = "commodities_research_catalog"
SCHEMA = "news_research"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.commodities_research_model"
ENDPOINT_NAME = "commodities-research-api"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Research Pipeline Model

# COMMAND ----------

class CommoditiesResearchModel(mlflow.pyfunc.PythonModel):
    """
    MLflow pyfunc model that wraps the full commodities research pipeline.
    On each prediction call it:
      1. Performs vector search for related articles
      2. Retrieves supply chain impact data via SQL
      3. Retrieves commodity price data via SQL
      4. Generates a research paper using Claude Sonnet
    """

    VS_INDEX = "commodities_research_catalog.news_research.news_articles_index"
    CATALOG = "commodities_research_catalog"
    SCHEMA = "news_research"
    LLM_ENDPOINT = "databricks-claude-sonnet-4"
    WAREHOUSE_ID = "3f5fda9e5ef763b6"

    def load_context(self, context):
        from databricks.sdk import WorkspaceClient
        self.w = WorkspaceClient()

    def _search_articles(self, query, num_results=10):
        """Vector search for related articles."""
        try:
            response = self.w.vector_search_indexes.query_index(
                index_name=self.VS_INDEX,
                columns=["article_id", "headline", "source", "primary_commodity",
                         "region", "sector", "published_at", "body"],
                query_text=query,
                num_results=num_results
            )
            columns = [c.name for c in (response.manifest.columns or [])]
            articles = []
            for row in (response.result.data_array or []):
                row_dict = dict(zip(columns, row))
                articles.append({
                    "article_id": row_dict.get("article_id", ""),
                    "headline": row_dict.get("headline", ""),
                    "source": row_dict.get("source", ""),
                    "primary_commodity": row_dict.get("primary_commodity", ""),
                    "region": row_dict.get("region", ""),
                    "sector": row_dict.get("sector", ""),
                    "published_at": str(row_dict.get("published_at", "")),
                    "body": row_dict.get("body", ""),
                    "similarity_score": float(row_dict.get("score", 0))
                })
            return articles
        except Exception as e:
            return [{"error": str(e)}]

    def _run_sql(self, statement):
        """Execute SQL via the statement execution API."""
        try:
            result = self.w.statement_execution.execute_statement(
                warehouse_id=self.WAREHOUSE_ID,
                statement=statement,
                wait_timeout="30s"
            )
            rows = []
            if result.result and result.result.data_array:
                cols = [c.name for c in result.manifest.schema.columns]
                for row in result.result.data_array:
                    rows.append(dict(zip(cols, row)))
            return rows
        except Exception as e:
            return [{"error": str(e)}]

    def _get_supply_chain(self, article_ids):
        """Get supply chain impact data for matched articles."""
        if not article_ids:
            return []
        ids_str = ",".join([f"'{aid}'" for aid in article_ids])
        return self._run_sql(f"""
            SELECT commodity, disruption_type, impact_level, severity_score,
                   affected_route, affected_facility, price_change_pct,
                   volume_disrupted_pct, mitigation_strategy,
                   downstream_impact_description
            FROM {self.CATALOG}.{self.SCHEMA}.supply_chain_impacts
            WHERE article_id IN ({ids_str})
            ORDER BY severity_score DESC
            LIMIT 20
        """)

    def _get_prices(self, commodities):
        """Get recent price history."""
        if not commodities:
            return []
        commodities_str = ",".join([f"'{c}'" for c in commodities])
        return self._run_sql(f"""
            SELECT commodity, trade_date, close_price
            FROM {self.CATALOG}.{self.SCHEMA}.commodity_prices
            WHERE commodity IN ({commodities_str})
            ORDER BY commodity, trade_date DESC
            LIMIT 100
        """)

    def _generate_paper(self, breaking_news, articles, supply_chain, prices):
        """Generate research paper using Claude Sonnet."""
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        from datetime import datetime

        articles_context = "\n\n".join([
            f"**{a['source']}** — {a['headline']}\n"
            f"Commodity: {a['primary_commodity']} | Region: {a['region']} | Sector: {a['sector']}\n"
            f"{a.get('body','')[:800]}..."
            for a in articles[:8] if "error" not in a
        ])

        sc_context = "\n".join([
            f"- {s.get('commodity','?')}: {s.get('impact_level','?')} impact ({s.get('disruption_type','?')}). "
            f"Price change: {s.get('price_change_pct','?')}%, Volume disrupted: {s.get('volume_disrupted_pct','?')}%. "
            f"Route: {s.get('affected_route','?')}. Mitigation: {s.get('mitigation_strategy','?')}"
            for s in supply_chain[:15] if "error" not in s
        ])

        price_context = ""
        if prices:
            by_commodity = {}
            for p in prices:
                if "error" in p:
                    continue
                by_commodity.setdefault(p.get("commodity",""), []).append(p)
            for commodity, plist in by_commodity.items():
                sorted_prices = sorted(plist, key=lambda x: str(x.get("trade_date","")))
                if len(sorted_prices) >= 2:
                    try:
                        first = float(sorted_prices[0]["close_price"])
                        last = float(sorted_prices[-1]["close_price"])
                        change = ((last - first) / first) * 100 if first else 0
                        price_context += f"- {commodity}: ${last:.2f} (10-day change: {change:+.1f}%)\n"
                    except (ValueError, TypeError):
                        pass

        prompt = f"""You are a senior commodities research analyst at a major investment bank.
Generate a comprehensive research paper analyzing the impact of the following breaking news
on commodity markets, supply chains, and prices.

## BREAKING NEWS
{breaking_news}

## RELATED NEWS ARTICLES (from Bloomberg, Reuters, S&P Global, CNBC, Financial Times)
{articles_context}

## SUPPLY CHAIN IMPACT DATA
{sc_context}

## RECENT PRICE MOVEMENTS
{price_context}

---

Write a detailed research paper with the following structure:

# Commodities Research Report: [Title based on the breaking news]
**Date: {datetime.now().strftime('%B %d, %Y')}**
**Classification: For Institutional Clients Only**

## Executive Summary
(2-3 paragraphs summarizing the key findings and implications)

## Market Impact Analysis
### Price Impact
### Supply Chain Disruption Assessment
### Volume and Trade Flow Impact

## Commodity-Specific Analysis

## Regional Impact Assessment

## Risk Factors and Scenarios
### Bull Case
### Base Case
### Bear Case

## Mitigation Strategies and Recommendations

## Conclusion

Use specific data points. Be quantitative. Professional investment research tone. 1500-2500 words.
"""
        try:
            response = self.w.serving_endpoints.query(
                name=self.LLM_ENDPOINT,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.SYSTEM,
                        content="You are a senior commodities research analyst producing institutional-grade research reports."
                    ),
                    ChatMessage(role=ChatMessageRole.USER, content=prompt)
                ],
                max_tokens=4096,
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error generating research paper: {str(e)}"

    def predict(self, context, model_input):
        """
        Main prediction method.
        Input: DataFrame with column 'breaking_news'
        Output: List of JSON strings with full research results
        """
        from datetime import datetime

        results = []
        for _, row in model_input.iterrows():
            breaking_news = str(row["breaking_news"])

            # Step 1: Vector search
            articles = self._search_articles(breaking_news)

            # Step 2: Supply chain data
            article_ids = [a["article_id"] for a in articles if "error" not in a]
            supply_chain = self._get_supply_chain(article_ids)

            # Step 3: Price data
            commodities = list(set(
                [a["primary_commodity"] for a in articles if "error" not in a] +
                [s.get("commodity","") for s in supply_chain if "error" not in s]
            ))
            prices = self._get_prices(commodities)

            # Step 4: Generate paper
            paper = self._generate_paper(breaking_news, articles, supply_chain, prices)

            results.append(json.dumps({
                "research_paper": paper,
                "related_articles": articles,
                "supply_chain_impacts": supply_chain,
                "price_data": prices,
                "metadata": {
                    "query": breaking_news,
                    "num_articles_found": len([a for a in articles if "error" not in a]),
                    "num_supply_chain_records": len([s for s in supply_chain if "error" not in s]),
                    "commodities_analyzed": commodities,
                    "generated_at": datetime.now().isoformat()
                }
            }))

        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model in Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

signature = ModelSignature(
    inputs=Schema([ColSpec("string", "breaking_news")]),
    outputs=Schema([ColSpec("string", "research_output")])
)

with mlflow.start_run(run_name="commodities_research_model") as run:
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=CommoditiesResearchModel(),
        pip_requirements=[
            "databricks-sdk>=0.38.0",
            "databricks-vectorsearch>=0.42",
            "mlflow>=2.10.0"
        ],
        signature=signature,
        registered_model_name=MODEL_NAME
    )
    run_id = run.info.run_id
    print(f"Model logged. Run ID: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Latest Model Version

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()
versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(versions, key=lambda v: int(v.version)).version
print(f"Latest model version: {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)
import time

w = WorkspaceClient()

served_entities = [
    ServedEntityInput(
        entity_name=MODEL_NAME,
        entity_version=str(latest_version),
        workload_size="Small",
        scale_to_zero_enabled=True,
        environment_vars={
            "DATABRICKS_HOST": "{{secrets/commodities-research/db-host}}",
            "DATABRICKS_TOKEN": "{{secrets/commodities-research/db-token}}"
        }
    )
]

# Check if endpoint exists
try:
    existing = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Endpoint '{ENDPOINT_NAME}' exists. Updating...")
    w.serving_endpoints.update_config(
        name=ENDPOINT_NAME,
        served_entities=served_entities
    )
except Exception:
    print(f"Creating endpoint '{ENDPOINT_NAME}'...")
    endpoint_config = EndpointCoreConfigInput(
        name=ENDPOINT_NAME,
        served_entities=served_entities
    )
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=endpoint_config
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Endpoint to be Ready

# COMMAND ----------

for i in range(60):
    try:
        ep = w.serving_endpoints.get(ENDPOINT_NAME)
        state = ep.state.ready
        config_state = ep.state.config_update
        print(f"[{i+1}/60] Ready: {state} | Config: {config_state}")
        if str(state) == "READY":
            print(f"\nEndpoint is READY!")
            print(f"URL: https://{w.config.host}/serving-endpoints/{ENDPOINT_NAME}/invocations")
            break
    except Exception as e:
        print(f"[{i+1}/60] Checking... {e}")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Endpoint

# COMMAND ----------

import requests

# Test via the SDK
response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_split={
        "columns": ["breaking_news"],
        "data": [["OPEC+ announces surprise production cut of 2 million barrels per day"]]
    }
)

# Parse the result
result = json.loads(response.predictions[0])
print("=" * 60)
print("ENDPOINT TEST SUCCESSFUL")
print("=" * 60)
print(f"Articles found: {result['metadata']['num_articles_found']}")
print(f"Supply chain records: {result['metadata']['num_supply_chain_records']}")
print(f"Commodities: {', '.join(result['metadata']['commodities_analyzed'])}")
print(f"\nResearch paper preview (first 500 chars):")
print(result['research_paper'][:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples
# MAGIC
# MAGIC ### Python SDK
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC import json
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC response = w.serving_endpoints.query(
# MAGIC     name="commodities-research-api",
# MAGIC     dataframe_split={
# MAGIC         "columns": ["breaking_news"],
# MAGIC         "data": [["Iran closes Strait of Hormuz"]]
# MAGIC     }
# MAGIC )
# MAGIC result = json.loads(response.predictions[0])
# MAGIC print(result["research_paper"])
# MAGIC ```
# MAGIC
# MAGIC ### curl
# MAGIC ```bash
# MAGIC curl -X POST "https://<workspace>/serving-endpoints/commodities-research-api/invocations" \
# MAGIC   -H "Authorization: Bearer <token>" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{"dataframe_split": {"columns": ["breaking_news"], "data": [["OPEC+ cuts production by 2M bpd"]]}}'
# MAGIC ```
# MAGIC
# MAGIC ### Batch (multiple queries)
# MAGIC ```python
# MAGIC response = w.serving_endpoints.query(
# MAGIC     name="commodities-research-api",
# MAGIC     dataframe_split={
# MAGIC         "columns": ["breaking_news"],
# MAGIC         "data": [
# MAGIC             ["Iran closes Strait of Hormuz"],
# MAGIC             ["Major copper mine collapse in Chile"],
# MAGIC             ["China bans rare earth exports"]
# MAGIC         ]
# MAGIC     }
# MAGIC )
# MAGIC for pred in response.predictions:
# MAGIC     result = json.loads(pred)
# MAGIC     print(result["metadata"]["query"], "->", result["metadata"]["num_articles_found"], "articles")
# MAGIC ```
