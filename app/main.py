"""
Commodities Research News App — FastAPI Backend

Takes breaking news input from users, performs vector search against stored
commodities news articles, retrieves supply chain impact data, and uses AI
to generate a comprehensive research paper analyzing commodity impact.
"""

import os
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from databricks.sdk.service.vectorsearch import QueryVectorIndexResponse
from pydantic import BaseModel

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Commodities Research Intelligence")

# Configuration from environment
VS_ENDPOINT = os.getenv("VS_ENDPOINT_NAME", "commodities_vs_endpoint")
VS_INDEX = os.getenv("VS_INDEX_NAME", "commodities_research_catalog.news_research.news_articles_index")
CATALOG = os.getenv("CATALOG", "commodities_research_catalog")
SCHEMA = os.getenv("SCHEMA", "news_research")
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-claude-sonnet-4")
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

# Initialize clients
w = WorkspaceClient()



# ── Request / Response Models ──────────────────────────────────────────────

class ResearchRequest(BaseModel):
    breaking_news: str
    num_articles: int = 10
    focus_commodities: Optional[list[str]] = None

class ArticleResult(BaseModel):
    article_id: str
    headline: str
    source: str
    primary_commodity: str
    region: str
    sector: str
    published_at: str
    body: str
    similarity_score: float

class SupplyChainImpact(BaseModel):
    commodity: str
    disruption_type: str
    impact_level: str
    severity_score: float
    affected_route: str
    affected_facility: str
    price_change_pct: float
    volume_disrupted_pct: float
    mitigation_strategy: str
    downstream_impact_description: str

class PriceData(BaseModel):
    commodity: str
    trade_date: str
    close_price: float
    price_change_pct: Optional[float] = None

class ResearchResponse(BaseModel):
    research_paper: str
    related_articles: list[ArticleResult]
    supply_chain_impacts: list[SupplyChainImpact]
    price_data: list[PriceData]
    metadata: dict

# ── Helper Functions ───────────────────────────────────────────────────────

def search_related_articles(query: str, num_results: int = 10) -> list[dict]:
    """Search for related news articles using Databricks SDK vector search."""
    try:
        response = w.vector_search_indexes.query_index(
            index_name=VS_INDEX,
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
        logger.info(f"Found {len(articles)} articles via vector search")
        return articles
    except Exception as e:
        logger.error(f"Vector search failed: {e}", exc_info=True)
        return []


def get_supply_chain_data(article_ids: list[str]) -> list[dict]:
    """Retrieve supply chain impact data for the matched articles."""
    if not article_ids:
        return []
    try:
        ids_str = ",".join([f"'{aid}'" for aid in article_ids])
        query = f"""
            SELECT commodity, disruption_type, impact_level, severity_score,
                   affected_route, affected_facility, price_change_pct,
                   volume_disrupted_pct, mitigation_strategy,
                   downstream_impact_description
            FROM {CATALOG}.{SCHEMA}.supply_chain_impacts
            WHERE article_id IN ({ids_str})
            ORDER BY severity_score DESC
        """
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s"
        )
        rows = []
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                rows.append({
                    "commodity": row[0],
                    "disruption_type": row[1],
                    "impact_level": row[2],
                    "severity_score": float(row[3]) if row[3] else 0,
                    "affected_route": row[4],
                    "affected_facility": row[5],
                    "price_change_pct": float(row[6]) if row[6] else 0,
                    "volume_disrupted_pct": float(row[7]) if row[7] else 0,
                    "mitigation_strategy": row[8],
                    "downstream_impact_description": row[9]
                })
        return rows
    except Exception as e:
        logger.error(f"Supply chain query failed: {e}")
        return []


def get_price_data(commodities: list[str], days: int = 10) -> list[dict]:
    """Get recent price history for relevant commodities."""
    if not commodities:
        return []
    try:
        commodities_str = ",".join([f"'{c}'" for c in commodities])
        query = f"""
            SELECT commodity, trade_date, close_price
            FROM {CATALOG}.{SCHEMA}.commodity_prices
            WHERE commodity IN ({commodities_str})
            ORDER BY commodity, trade_date DESC
            LIMIT {days * len(commodities)}
        """
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="30s"
        )
        rows = []
        if result.result and result.result.data_array:
            for row in result.result.data_array:
                rows.append({
                    "commodity": row[0],
                    "trade_date": str(row[1]) if row[1] else "",
                    "close_price": float(row[2]) if row[2] else 0
                })
        return rows
    except Exception as e:
        logger.error(f"Price data query failed: {e}")
        return []


def generate_research_paper(
    breaking_news: str,
    articles: list[dict],
    supply_chain: list[dict],
    prices: list[dict]
) -> str:
    """Use LLM to generate a comprehensive research paper."""

    # Build context from retrieved data
    articles_context = "\n\n".join([
        f"**{a['source']}** — {a['headline']}\n"
        f"Commodity: {a['primary_commodity']} | Region: {a['region']} | Sector: {a['sector']}\n"
        f"{a['body'][:800]}..."
        for a in articles[:8]
    ])

    sc_context = "\n".join([
        f"- {s['commodity']}: {s['impact_level']} impact ({s['disruption_type']}). "
        f"Price change: {s['price_change_pct']:+.1f}%, Volume disrupted: {s['volume_disrupted_pct']:.1f}%. "
        f"Route: {s['affected_route']}. Mitigation: {s['mitigation_strategy']}"
        for s in supply_chain[:15]
    ])

    price_context = ""
    if prices:
        by_commodity = {}
        for p in prices:
            by_commodity.setdefault(p["commodity"], []).append(p)
        for commodity, plist in by_commodity.items():
            sorted_prices = sorted(plist, key=lambda x: x["trade_date"])
            if len(sorted_prices) >= 2:
                first, last = sorted_prices[0]["close_price"], sorted_prices[-1]["close_price"]
                change = ((last - first) / first) * 100 if first else 0
                price_context += f"- {commodity}: ${last:.2f} (10-day change: {change:+.1f}%)\n"

    prompt = f"""You are a senior commodities research analyst at a major investment bank.
Generate a comprehensive research paper analyzing the impact of the following breaking news
on commodity markets, supply chains, and prices. Use the provided data from news articles,
supply chain analysis, and price history to support your analysis.

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
(Analyze how the breaking news affects commodity prices, using the price data provided)

### Supply Chain Disruption Assessment
(Detail the supply chain impacts, affected routes, facilities, and downstream industries)

### Volume and Trade Flow Impact
(Quantify the impact on trade volumes and shipping routes)

## Commodity-Specific Analysis
(For each major commodity affected, provide detailed analysis including price outlook,
supply-demand dynamics, and key risk factors)

## Regional Impact Assessment
(Analyze how different regions are affected, including producing and consuming nations)

## Risk Factors and Scenarios
### Bull Case
### Base Case
### Bear Case

## Mitigation Strategies and Recommendations
(Actionable recommendations for market participants)

## Conclusion

Use specific data points from the articles and supply chain data. Be quantitative where possible.
Write in a professional investment research tone. The paper should be 1500-2500 words.
"""

    try:
        response = w.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=[
                ChatMessage(
                    role=ChatMessageRole.SYSTEM,
                    content="You are a senior commodities research analyst producing institutional-grade research reports. Use precise language, specific data points, and quantitative analysis."
                ),
                ChatMessage(
                    role=ChatMessageRole.USER,
                    content=prompt
                )
            ],
            max_tokens=4096,
            temperature=0.3
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Research generation failed: {str(e)}")


# ── API Endpoints ──────────────────────────────────────────────────────────

@app.post("/api/research", response_model=ResearchResponse)
async def generate_research(request: ResearchRequest):
    """Main endpoint: takes breaking news, returns research paper with supporting data."""
    logger.info(f"Research request: {request.breaking_news[:100]}...")

    # Step 1: Vector search for related articles
    articles = search_related_articles(request.breaking_news, request.num_articles)
    if not articles:
        raise HTTPException(status_code=404, detail="No related articles found")

    # Step 2: Get supply chain impact data
    article_ids = [a["article_id"] for a in articles]
    supply_chain = get_supply_chain_data(article_ids)

    # Step 3: Extract unique commodities and get price data
    commodities = list(set(
        [a["primary_commodity"] for a in articles] +
        [s["commodity"] for s in supply_chain]
    ))
    if request.focus_commodities:
        commodities = list(set(commodities + request.focus_commodities))
    prices = get_price_data(commodities)

    # Step 4: Generate research paper
    research_paper = generate_research_paper(
        request.breaking_news, articles, supply_chain, prices
    )

    return ResearchResponse(
        research_paper=research_paper,
        related_articles=[ArticleResult(**a) for a in articles],
        supply_chain_impacts=[SupplyChainImpact(**s) for s in supply_chain],
        price_data=[PriceData(**p) for p in prices],
        metadata={
            "query": request.breaking_news,
            "num_articles_found": len(articles),
            "num_supply_chain_records": len(supply_chain),
            "commodities_analyzed": commodities,
            "generated_at": datetime.now().isoformat()
        }
    )


@app.get("/api/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# ── Serve Frontend ─────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return FileResponse("static/index.html")
