# Databricks notebook source
# MAGIC %md
# MAGIC # Supervisor Agent Setup (Agent Bricks)
# MAGIC
# MAGIC This notebook documents the Supervisor Agent configuration and tests the endpoint.
# MAGIC The Supervisor Agent is created via the **Databricks UI**, not programmatically.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Notebook 05 completed (Genie Space created)
# MAGIC - Vector Search index `news_articles_index` exists (from notebook 03)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Supervisor Agent in Databricks UI
# MAGIC
# MAGIC 1. Navigate to **Agents** in the left sidebar
# MAGIC 2. Click the **Supervisor Agent** tile → **Build**
# MAGIC 3. Configure:
# MAGIC    - **Name**: `Commodities Research Supervisor`
# MAGIC    - **Description**: `Analyzes breaking commodities news by searching related articles via Vector Search, querying supply chain and price data via Genie Space, and generating institutional-grade research papers with quantitative analysis.`
# MAGIC 4. **Add subagents**:
# MAGIC    - **Genie Space**: Select "Commodities Research Data Analyst" (created in notebook 05)
# MAGIC    - **Vector Search**: Add the `commodities_research_catalog.news_research.news_articles_index`
# MAGIC 5. **Instructions** (paste the text from the cell below)
# MAGIC 6. Click **Create Agent** and wait for deployment
# MAGIC 7. Click **"See Agent Status"** to get the serving endpoint name

# COMMAND ----------

# Instructions to paste into the Supervisor Agent configuration
SUPERVISOR_INSTRUCTIONS = """You are a senior commodities research analyst at a major investment bank.

When given breaking news about commodities, you must:

1. Search for related news articles using the Vector Search tool
2. Query supply chain impacts and commodity price data using the Genie Space
3. Synthesize all findings into a comprehensive research paper

Your research paper MUST follow this structure:

# Commodities Research Report: [Title based on the breaking news]
**Classification: For Institutional Clients Only**

## Executive Summary
(2-3 paragraphs summarizing key findings and implications)

## Market Impact Analysis
### Price Impact
(How the breaking news affects commodity prices, using retrieved price data)
### Supply Chain Disruption Assessment
(Detail supply chain impacts, affected routes, facilities, and downstream industries)
### Volume and Trade Flow Impact
(Quantify the impact on trade volumes and shipping routes)

## Commodity-Specific Analysis
(For each major commodity affected: price outlook, supply-demand dynamics, risk factors)

## Regional Impact Assessment
(How different regions are affected, including producing and consuming nations)

## Risk Factors and Scenarios
### Bull Case
### Base Case
### Bear Case

## Mitigation Strategies and Recommendations
(Actionable recommendations for market participants)

## Conclusion

Use specific data points from the retrieved articles and supply chain data.
Be quantitative where possible. Write in a professional investment research tone.
The paper should be 1500-2500 words."""

print("Copy the instructions above into the Supervisor Agent configuration.")
print(f"\nCharacter count: {len(SUPERVISOR_INSTRUCTIONS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Record Supervisor Configuration

# COMMAND ----------

# Update these values after creating the Supervisor Agent in the UI
SUPERVISOR_ENDPOINT = "<REPLACE_WITH_ENDPOINT_NAME>"  # From "See Agent Status"
GENIE_SPACE_ID = "<REPLACE_WITH_GENIE_SPACE_ID>"      # From notebook 05

print(f"Supervisor Endpoint: {SUPERVISOR_ENDPOINT}")
print(f"Genie Space ID: {GENIE_SPACE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test Supervisor Agent

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

# Test query
test_query = "Iran announces closure of Strait of Hormuz after failed nuclear negotiations"

print(f"Testing Supervisor Agent: {SUPERVISOR_ENDPOINT}")
print(f"Query: {test_query}")
print("=" * 60)

try:
    response = w.serving_endpoints.query(
        name=SUPERVISOR_ENDPOINT,
        messages=[
            ChatMessage(
                role=ChatMessageRole.USER,
                content=f"Analyze this breaking news and generate a comprehensive research report: {test_query}"
            )
        ],
        max_tokens=8192,
        temperature=0.3,
    )

    result_text = response.choices[0].message.content
    print(f"\nResponse length: {len(result_text)} characters")
    print(f"\nFirst 500 characters:")
    print(result_text[:500])
    print("\n...")
    print(f"\nLast 200 characters:")
    print(result_text[-200:])

except Exception as e:
    print(f"Error querying Supervisor: {e}")
    print("\nMake sure:")
    print("1. The Supervisor Agent has finished building")
    print("2. The endpoint name is correct")
    print("3. The service principal has CAN_QUERY permission")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Configuration for app.yaml
# MAGIC
# MAGIC Once the Supervisor Agent is working, update `app/app.yaml` with these values.

# COMMAND ----------

if SUPERVISOR_ENDPOINT != "<REPLACE_WITH_ENDPOINT_NAME>":
    print("=" * 60)
    print("CONFIGURATION FOR app.yaml")
    print("=" * 60)
    print(f"""
Update app.yaml env section:
  - name: SUPERVISOR_ENDPOINT
    value: "{SUPERVISOR_ENDPOINT}"

Update app.yaml resources section:
  - name: supervisor-endpoint
    serving_endpoint:
      name: "{SUPERVISOR_ENDPOINT}"
      permission: CAN_QUERY
""")
else:
    print("Please update SUPERVISOR_ENDPOINT variable in the cell above first.")
