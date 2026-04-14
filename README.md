# Commodities Research Intelligence

AI-powered commodities research platform using a **Databricks Agent Bricks Supervisor** architecture. Takes breaking news input, orchestrates specialized agents to retrieve market intelligence via Vector Search and Genie Space, and generates institutional-grade research papers.

![Architecture](architecture.png)

## Overview

This application uses **Databricks Agent Bricks** to orchestrate a multi-agent research pipeline:

1. A user enters a breaking news event (e.g., "Iran announces closure of Strait of Hormuz")
2. The **Agent Bricks Supervisor** delegates to specialized sub-agents
3. The **News Search Agent** semantically searches 142+ commodities news articles via Vector Search
4. The **Data Analyst Agent** queries supply chain impacts and commodity prices via Genie Space
5. The Supervisor synthesizes all data into a comprehensive research paper
6. The research paper is displayed with real-time thinking steps and PDF export

## Architecture

### Agent Bricks Supervisor

The Supervisor Agent is created and managed via the **Databricks UI** (Build tab) and deployed as a managed serving endpoint. The FastAPI app calls this endpoint and streams the agent's thinking process to the frontend.

| Component | Description |
|-----------|-------------|
| **Supervisor Agent** | Managed by Agent Bricks. Orchestrates sub-agents and generates the research paper. |
| **News Search Agent** | Vector Search sub-agent. Semantically searches the news corpus for related articles. |
| **Data Analyst Agent** | Genie Space sub-agent. Queries supply chain impacts and commodity prices using natural language. |

The app uses an async submit/poll pattern to avoid proxy timeouts, with file-based job storage for cross-instance compatibility.

### Data Pipeline (One-Time Setup)

Databricks notebooks run sequentially on serverless compute:

| Notebook | Purpose | Output |
|----------|---------|--------|
| `01_generate_news_data.py` | Generates ~142 synthetic news articles from Bloomberg, Reuters, S&P Global, CNBC, and Financial Times across 7 themes | `news_articles` |
| `02_generate_supply_chain_data.py` | Creates supply chain impact records and daily commodity price history (Mar 1 - Apr 8, 2026) | `supply_chain_impacts`, `commodity_prices` |
| `03_setup_vector_search.py` | Prepares embedding text, provisions Vector Search endpoint, creates Delta Sync index | `news_articles_vs` + VS Index |
| `05_setup_genie_space.py` | Creates a Genie Space with supply chain + price tables for natural language querying | Genie Space |
| `06_setup_supervisor_agent.py` | Documents Supervisor Agent UI setup and tests the serving endpoint | Supervisor endpoint |

**Commodities tracked (25):** Crude Oil, Brent, WTI, Natural Gas, LNG, Gold, Silver, Copper, Aluminum, Platinum, Wheat, Corn, Soybeans, Rice, Sugar, Iron Ore, Lithium, Nickel, Zinc, Palladium, Urea, Methanol, Sulfur, Helium, Cobalt

### Infrastructure

| Component | Resource |
|-----------|----------|
| **Workspace** | FEVM Serverless (`fevm-commodities-research`) |
| **Catalog** | `commodities_research_catalog.news_research` |
| **Vector Search Endpoint** | `commodities_vs_endpoint` (STANDARD) |
| **Vector Search Index** | `news_articles_index` (Delta Sync, `databricks-gte-large-en`) |
| **Genie Space** | Configured with `supply_chain_impacts` + `commodity_prices` |
| **Supervisor Agent** | Agent Bricks managed serving endpoint |
| **Embeddings** | `databricks-gte-large-en` |
| **App Compute** | Databricks Apps (Medium) |

## Project Structure

```
ResearchNews/
├── README.md
├── architecture.png                      # Architecture diagram
├── notebooks/
│   ├── 01_generate_news_data.py          # News article generation
│   ├── 02_generate_supply_chain_data.py  # Supply chain + price data
│   ├── 03_setup_vector_search.py         # Vector search setup
│   ├── 04_deploy_serving_endpoint.py     # MLflow model + serving endpoint (optional)
│   ├── 05_setup_genie_space.py           # Genie Space setup
│   └── 06_setup_supervisor_agent.py      # Supervisor Agent setup docs + test
└── app/
    ├── app.yaml                  # Databricks App configuration
    ├── requirements.txt          # Python dependencies
    ├── main.py                   # FastAPI backend (submit/poll + Supervisor call)
    ├── agent_parser.py           # Supervisor text output → structured response parser
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

- **Agent Bricks orchestration** — Managed Supervisor Agent coordinates Vector Search and Genie Space sub-agents
- **Real-time thinking steps** — Frontend streams the agent's reasoning process as it works
- **Semantic search** — Vector Search finds relevant articles regardless of keyword overlap
- **Natural language data queries** — Genie Space generates SQL dynamically instead of hardcoded queries
- **Multi-source analysis** — Cross-references news from 5 major outlets
- **AI research generation** — Produces structured research papers with executive summary, price analysis, risk scenarios, and recommendations
- **Markdown rendering** — Tables, blockquotes, lists, and formatting rendered cleanly
- **PDF export** — Download the research paper as a formatted PDF

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

3. **Create the Supervisor Agent** — Follow the steps in notebook `06_setup_supervisor_agent.py`:
   - Navigate to Agents → Supervisor Agent → Build
   - Add the Genie Space and Vector Search as sub-agents
   - Note the serving endpoint name

4. **Update app config** — Set the Supervisor endpoint name in `app/app.yaml`

5. **Create the app**:
   ```bash
   databricks apps create commodities-research --profile=<profile>
   ```

6. **Grant permissions** to the app's service principal:
   - `CAN_QUERY` on the Supervisor serving endpoint
   - `CAN_QUERY` on any Knowledge Assistant endpoints used by the Supervisor
   - `CAN_RUN` on the Genie Space (via Share in the UI)
   - Unity Catalog: `USE_CATALOG`, `USE_SCHEMA`, `SELECT` on the catalog

7. **Deploy the app**:
   ```bash
   databricks apps deploy commodities-research \
     --source-code-path /Workspace/Users/<user>/commodities-research-app \
     --profile=<profile>
   ```

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Frontend | React 18, Babel (in-browser), html2pdf.js |
| Backend | FastAPI, Uvicorn |
| Agent Orchestration | Databricks Agent Bricks (Supervisor Agent) |
| AI/ML | Claude Sonnet (LLM), GTE-Large-EN (Embeddings) |
| Search | Databricks Vector Search (Delta Sync) |
| Data Queries | Databricks Genie Space (natural language SQL) |
| Data | Delta Lake, Unity Catalog |
| Compute | Databricks Apps, Serverless SQL, Serverless Jobs |
| Infrastructure | Databricks FEVM (AWS) |
