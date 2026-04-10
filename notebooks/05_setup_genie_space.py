# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Space Setup
# MAGIC Creates and configures a Genie Space for the commodities research data.
# MAGIC The Genie Space enables natural language querying of supply chain impacts
# MAGIC and commodity price data via the Data Analyst Agent.
# MAGIC
# MAGIC Run AFTER notebooks 01 and 02 (tables must exist).

# COMMAND ----------

import json
import uuid
import requests

# COMMAND ----------

CATALOG = "commodities_research_catalog"
SCHEMA = "news_research"
GENIE_SPACE_NAME = "Commodities Research Data Analyst"

# Get workspace host and token for REST API calls
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

base_url = f"https://{workspace_host}/api/2.0"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Exist

# COMMAND ----------

sc_count = spark.table(f"{CATALOG}.{SCHEMA}.supply_chain_impacts").count()
price_count = spark.table(f"{CATALOG}.{SCHEMA}.commodity_prices").count()

print(f"supply_chain_impacts: {sc_count} rows")
print(f"commodity_prices: {price_count} rows")
assert sc_count > 0, "supply_chain_impacts table is empty — run notebook 02 first"
assert price_count > 0, "commodity_prices table is empty — run notebook 02 first"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get SQL Warehouse ID

# COMMAND ----------

# Use the same warehouse as the app
WAREHOUSE_ID = "3f5fda9e5ef763b6"

# Verify warehouse exists
wh_resp = requests.get(
    f"{base_url}/sql/warehouses/{WAREHOUSE_ID}",
    headers=headers
)
if wh_resp.status_code == 200:
    wh_info = wh_resp.json()
    print(f"Warehouse: {wh_info.get('name')} (ID: {WAREHOUSE_ID})")
    print(f"State: {wh_info.get('state')}")
else:
    print(f"Warning: Could not verify warehouse {WAREHOUSE_ID}: {wh_resp.status_code}")
    print("You may need to update the WAREHOUSE_ID variable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space

# COMMAND ----------

def hex_id():
    """Generate a 32-character lowercase hex ID for Genie Space config."""
    return uuid.uuid4().hex

def sorted_by_id(items):
    """Sort a list of dicts by their 'id' field (required by Genie API)."""
    return sorted(items, key=lambda x: x["id"])

# Pre-generate sample questions and sort by id
sample_questions = sorted_by_id([
    {"id": hex_id(), "question": ["What are the critical and high severity supply chain impacts for Crude Oil?"]},
    {"id": hex_id(), "question": ["Show me the last 10 days of closing prices for Natural Gas and Copper"]},
    {"id": hex_id(), "question": ["Which commodities have the highest volume disruption percentage?"]},
    {"id": hex_id(), "question": ["What mitigation strategies are recommended for Strait of Hormuz route disruptions?"]},
    {"id": hex_id(), "question": ["Show supply chain impacts with severity score above 0.7 including affected routes and facilities"]},
    {"id": hex_id(), "question": ["What is the average price change percentage by commodity for Critical impact level?"]},
])

# Pre-generate example SQL questions and sort by id
example_sqls = sorted_by_id([
    {
        "id": hex_id(),
        "question": ["What are the critical severity supply chain impacts for Crude Oil?"],
        "sql": [
            "SELECT commodity, disruption_type, impact_level, severity_score, affected_route, "
            "affected_facility, price_change_pct, volume_disrupted_pct, mitigation_strategy, "
            "downstream_impact_description "
            "FROM commodities_research_catalog.news_research.supply_chain_impacts "
            "WHERE commodity = 'Crude Oil' AND severity_score > 0.8 "
            "ORDER BY severity_score DESC"
        ]
    },
    {
        "id": hex_id(),
        "question": ["Show the last 10 days of closing prices for Natural Gas"],
        "sql": [
            "SELECT commodity, trade_date, close_price "
            "FROM commodities_research_catalog.news_research.commodity_prices "
            "WHERE commodity = 'Natural Gas' "
            "ORDER BY trade_date DESC LIMIT 10"
        ]
    },
])

# Genie Space configuration
serialized_space = {
    "version": 2,
    "config": {
        "sample_questions": sample_questions
    },
    "data_sources": {
        "tables": [
            {
                "identifier": f"{CATALOG}.{SCHEMA}.commodity_prices",
                "description": [
                    "Daily OHLCV price data for tracked commodities from March to April 2026"
                ]
            },
            {
                "identifier": f"{CATALOG}.{SCHEMA}.supply_chain_impacts",
                "description": [
                    "Supply chain disruption records linked to commodities news articles"
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": hex_id(),
                "content": [
                    "This space provides commodities supply chain disruption data and pricing data. "
                    "supply_chain_impacts has columns: record_id, article_id, commodity, disruption_type, "
                    "impact_level (Critical/High/Medium/Low), severity_score (0-1), affected_route, "
                    "affected_facility, price_change_pct, volume_disrupted_pct, mitigation_strategy, "
                    "downstream_impact_description, region, sector. "
                    "commodity_prices has columns: commodity, trade_date, open_price, high_price, low_price, "
                    "close_price, volume, price_unit. "
                    "When asked about supply chain impacts, include commodity, disruption_type, impact_level, "
                    "severity_score, affected_route, affected_facility, price_change_pct, volume_disrupted_pct, "
                    "mitigation_strategy, downstream_impact_description. Order by severity_score DESC. "
                    "When asked about prices, return trade_date and close_price ordered by trade_date DESC, "
                    "limit to 10 days unless specified. "
                    "Severity thresholds: Critical > 0.8, High > 0.6, Medium > 0.3, Low <= 0.3."
                ]
            }
        ],
        "example_question_sqls": example_sqls
    }
}

# Create the Genie Space
create_payload = {
    "title": GENIE_SPACE_NAME,
    "description": "Natural language interface for querying commodities supply chain disruption data and price history. Used by the Data Analyst Agent in the multi-agent research pipeline.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space)
}

response = requests.post(
    f"{base_url}/genie/spaces",
    headers=headers,
    json=create_payload
)

if response.status_code == 200:
    space_data = response.json()
    genie_space_id = space_data.get("space_id") or space_data.get("id")
    print(f"Genie Space created successfully!")
    print(f"  Name: {GENIE_SPACE_NAME}")
    print(f"  Space ID: {genie_space_id}")
    print(f"\n  >>> Save this ID for app.yaml: GENIE_SPACE_ID = \"{genie_space_id}\"")
else:
    print(f"Error creating Genie Space: {response.status_code}")
    print(response.text)
    # Try to find existing space
    print("\nSearching for existing Genie Space...")
    genie_space_id = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Genie Space with Sample Question

# COMMAND ----------

if genie_space_id:
    # Start a test conversation
    test_question = "What are the top 5 commodities by average severity score in supply chain impacts?"

    conv_resp = requests.post(
        f"{base_url}/genie/spaces/{genie_space_id}/start-conversation",
        headers=headers,
        json={"content": test_question}
    )

    if conv_resp.status_code == 200:
        conv_data = conv_resp.json()
        conversation_id = conv_data.get("conversation_id")
        message_id = conv_data.get("message_id")
        print(f"Test conversation started: {conversation_id}")
        print(f"Message ID: {message_id}")
        print(f"Question: {test_question}")
        print(f"\nStatus: {conv_data.get('status', 'SUBMITTED')}")
        print("(Check the Genie UI to see the full response)")
    else:
        print(f"Test conversation failed: {conv_resp.status_code}")
        print(conv_resp.text)
else:
    print("No Genie Space ID available — skipping test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC
# MAGIC The Genie Space has been created with:
# MAGIC - **Tables**: `supply_chain_impacts`, `commodity_prices`
# MAGIC - **Instructions**: Detailed schema descriptions and query guidelines
# MAGIC - **Sample Questions**: 6 example questions for supply chain and price analysis
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Copy the `GENIE_SPACE_ID` printed above
# MAGIC 2. Update `app/app.yaml` with the Genie Space ID
# MAGIC 3. Test the Genie Space in the Databricks UI with sample questions
# MAGIC 4. The Data Analyst Agent in `app/agents.py` will use this space via MCP

# COMMAND ----------

# Print configuration summary for the app
if genie_space_id:
    print("=" * 60)
    print("CONFIGURATION FOR app.yaml")
    print("=" * 60)
    print(f"""
Add to app.yaml env section:
  - name: GENIE_SPACE_ID
    value: "{genie_space_id}"

Add to app.yaml resources section:
  - name: genie-space
    genie_space:
      id: "{genie_space_id}"
      permission: CAN_RUN

MCP URL for the Data Analyst Agent:
  https://{workspace_host}/api/2.0/mcp/genie/{genie_space_id}
""")
