# Commodities Research Intelligence

AI-powered commodities research platform using a **Multi-Agent Supervisor** architecture. Takes breaking news input, orchestrates specialized agents to retrieve market intelligence via Vector Search and Genie Space, and generates institutional-grade research papers.

![Architecture](architecture.png)

## Overview

This application uses the **OpenAI Agents SDK** with **Databricks MCP servers** to orchestrate a multi-agent research pipeline:

1. A user enters a breaking news event (e.g., "Iran announces closure of Strait of Hormuz")
2. The **Supervisor Agent** delegates to specialized sub-agents
3. The **News Search Agent** semantically searches 142+ commodities news articles via Vector Search MCP
4. The **Data Analyst Agent** queries supply chain impacts and commodity prices via Genie Space MCP
5. The Supervisor synthesizes all data into a comprehensive research paper
6. Results are presented across 4 interactive tabs with PDF export capability

## Architecture

### Multi-Agent Supervisor

The core of the application is a 3-agent system built with the OpenAI Agents SDK:

| Agent | Tool | Purpose |
|-------|------|---------|
| **Supervisor** | Handoffs + LLM | Orchestrates the pipeline, delegates to sub-agents, generates the research paper |
| **News Search Agent** | Vector Search MCP | Semantically searches the news corpus for related articles |
| **Data Analyst Agent** | Genie Space MCP | Queries supply chain impacts and commodity prices using natural language |

The Supervisor follows a sequential pipeline:
1. Hand off to News Search Agent → get related articles
2. Hand off to Data Analyst Agent → get supply chain + price data
3. Synthesize everything into a structured research paper with quantitative analysis

### Data Pipeline (One-Time Setup)

Four Databricks notebooks run sequentially on serverless compute:

| Notebook | Purpose | Output |
|----------|---------|--------|
| `01_generate_news_data.py` | Generates ~142 synthetic news articles from Bloomberg, Reuters, S&P Global, CNBC, and Financial Times across 7 themes | `news_articles` |
| `02_generate_supply_chain_data.py` | Creates supply chain impact records and daily commodity price history (Mar 1 - Apr 8, 2026) | `supply_chain_impacts`, `commodity_prices` |
| `03_setup_vector_search.py` | Prepares embedding text, provisions Vector Search endpoint, creates Delta Sync index | `news_articles_vs` + VS Index |
| `05_setup_genie_space.py` | Creates a Genie Space with supply chain + price tables for natural language querying | Genie Space |

**News themes covered:**
- Strait of Hormuz / Iran crisis
- Oil price volatility (Brent/WTI)
- Industrial metals (copper at record LME highs)
- European natural gas surge
- Fertilizer supply disruption
- Helium shortage impacting chipmaking
- Agriculture and wheat markets

**Commodities tracked (25):** Crude Oil, Brent, WTI, Natural Gas, LNG, Gold, Silver, Copper, Aluminum, Platinum, Wheat, Corn, Soybeans, Rice, Sugar, Iron Ore, Lithium, Nickel, Zinc, Palladium, Urea, Methanol, Sulfur, Helium, Cobalt

### Infrastructure

| Component | Resource |
|-----------|----------|
| **Workspace** | FEVM Serverless (`fevm-commodities-research`) |
| **Catalog** | `commodities_research_catalog.news_research` |
| **Vector Search Endpoint** | `commodities_vs_endpoint` (STANDARD) |
| **Vector Search Index** | `news_articles_index` (Delta Sync, `databricks-gte-large-en`) |
| **Genie Space** | Configured with `supply_chain_impacts` + `commodity_prices` |
| **LLM** | `databricks-claude-sonnet-4` (Foundation Model API) |
| **Embeddings** | `databricks-gte-large-en` |
| **App Compute** | Databricks Apps (Medium) |
| **Serving Endpoint** | `commodities-research-api` (MLflow pyfunc, scale-to-zero) — optional |

## Project Structure

```
ResearchNews/
├── README.md
├── architecture.png                      # Architecture diagram
├── notebooks/
│   ├── 01_generate_news_data.py          # News article generation
│   ├── 02_generate_supply_chain_data.py  # Supply chain + price data
│   ├── 03_setup_vector_search.py         # Vector search setup
│   ├── 04_deploy_serving_endpoint.py     # MLflow model + serving endpoint
│   └── 05_setup_genie_space.py           # Genie Space setup
└── app/
    ├── app.yaml                  # Databricks App configuration
    ├── requirements.txt          # Python dependencies
    ├── main.py                   # FastAPI backend (API + routing)
    ├── research_agents.py        # Multi-agent supervisor definitions
    ├── agent_parser.py           # Agent output → structured response parser
    └── static/
        └── index.html            # React frontend (single-file)
```

## Data Model

### `news_articles`
| Column | Type | Description |
|--------|------|-------------|
| `article_id` | STRING | Unique article identifier |
| `source` | STRING | News source (Bloomberg, Reuters, etc.) |
| `headline` | STRING | Article headline |
| `body` | STRING | Full article text |
| `published_at` | TIMESTAMP | Publication timestamp |
| `commodities` | ARRAY\<STRING\> | Mentioned commodities |
| `primary_commodity` | STRING | Primary commodity focus |
| `region` | STRING | Geographic region |
| `sector` | STRING | Market sector |
| `sentiment_score` | DOUBLE | Sentiment (-1 to 1) |
| `relevance_score` | DOUBLE | Relevance score (0 to 1) |

### `supply_chain_impacts`
| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | Unique record identifier |
| `article_id` | STRING | FK to news_articles |
| `commodity` | STRING | Affected commodity |
| `disruption_type` | STRING | Type of disruption |
| `impact_level` | STRING | Critical / High / Medium / Low |
| `severity_score` | DOUBLE | Severity (0 to 1) |
| `affected_route` | STRING | Trade route affected |
| `affected_facility` | STRING | Key facility impacted |
| `price_change_pct` | DOUBLE | Price impact percentage |
| `volume_disrupted_pct` | DOUBLE | Volume disruption percentage |
| `mitigation_strategy` | STRING | Recommended mitigation |

### `commodity_prices`
| Column | Type | Description |
|--------|------|-------------|
| `commodity` | STRING | Commodity name |
| `trade_date` | TIMESTAMP | Trading date |
| `open_price` | DOUBLE | Opening price |
| `high_price` | DOUBLE | Daily high |
| `low_price` | DOUBLE | Daily low |
| `close_price` | DOUBLE | Closing price |
| `volume` | LONG | Trading volume |

## App Features

- **Multi-agent orchestration** — Supervisor agent coordinates specialized agents for search, data analysis, and research generation
- **Semantic search** — News Search Agent uses Vector Search MCP to find relevant articles regardless of keyword overlap
- **Natural language data queries** — Data Analyst Agent uses Genie Space for dynamic SQL generation instead of hardcoded queries
- **Multi-source analysis** — Cross-references news from 5 major outlets
- **Supply chain mapping** — Links disruptions to specific trade routes, facilities, and downstream industries
- **AI research generation** — Claude Sonnet produces structured research papers with executive summary, price analysis, risk scenarios, and recommendations
- **Interactive UI** — 4 tabs for research paper, source articles, supply chain data, and price cards
- **PDF export** — Download the full report as a professionally formatted PDF with appendices

## Deployment

### Prerequisites
- Databricks FEVM workspace with serverless compute
- Unity Catalog enabled
- Access to Foundation Model API endpoints

### Steps

1. **Deploy workspace** — Create a new FEVM serverless workspace or use an existing one

2. **Run data notebooks** — Execute in order on serverless compute:
   ```
   01_generate_news_data.py
   02_generate_supply_chain_data.py
   03_setup_vector_search.py
   05_setup_genie_space.py
   ```

3. **Update app config** — Copy the Genie Space ID from notebook 05 output into `app/app.yaml` (replace `<REPLACE_WITH_GENIE_SPACE_ID>` in both the env and resources sections)

4. **Create the app**:
   ```bash
   databricks apps create commodities-research --profile=<profile>
   ```

5. **Grant permissions** to the app's service principal:
   ```sql
   GRANT USE_CATALOG, USE_SCHEMA, SELECT
   ON CATALOG commodities_research_catalog
   TO `<service-principal-client-id>`
   ```

6. **Deploy the app**:
   ```bash
   databricks apps deploy commodities-research \
     --source-code-path /Workspace/Users/<user>/commodities-research-app \
     --profile=<profile>
   ```

7. **(Optional) Deploy serving endpoint** — Run notebook `04_deploy_serving_endpoint.py` to register the MLflow model and create the `commodities-research-api` endpoint for programmatic access.

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Babel (in-browser), html2pdf.js |
| Backend | FastAPI, Uvicorn |
| Agent Framework | OpenAI Agents SDK, Databricks MCP Servers |
| AI/ML | Claude Sonnet 4 (LLM), GTE-Large-EN (Embeddings) |
| Search | Databricks Vector Search (Delta Sync) |
| Data Queries | Databricks Genie Space (natural language SQL) |
| Data | Delta Lake, Unity Catalog |
| Model Serving | MLflow pyfunc, Databricks Model Serving (scale-to-zero) |
| Compute | Databricks Apps, Serverless SQL, Serverless Jobs |
| Infrastructure | Databricks FEVM (AWS) |
