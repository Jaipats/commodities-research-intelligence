"""
Response Parser for Agent Bricks Supervisor Output

Converts the Supervisor Agent's text response into the structured
ResearchResponse format expected by the frontend. Uses a 3-tier strategy:
  1. Extract JSON block from the agent's structured output
  2. Regex-based extraction from conversational text
  3. Raw text fallback with empty structured data
"""

import json
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_agent_output(raw_text: str, breaking_news: str) -> dict:
    """
    Parse the raw text response from the Agent Bricks Supervisor
    into a dict matching the ResearchResponse schema.

    Args:
        raw_text: The text content from the Supervisor's serving endpoint response
        breaking_news: The original query for metadata

    Returns:
        dict with keys: research_paper, related_articles, supply_chain_impacts,
                        price_data, metadata
    """
    if not raw_text:
        logger.warning("No output text from Supervisor Agent")
        return _empty_response(breaking_news)

    # Tier 1: Try to extract a complete JSON block
    parsed = _extract_json_block(raw_text)
    if parsed and "research_paper" in parsed:
        logger.info("Tier 1: Successfully parsed structured JSON output")
        return _normalize_response(parsed, breaking_news)

    # Tier 2: Try to extract individual JSON sections
    logger.info("Tier 1 failed, trying Tier 2: section extraction")
    parsed = _extract_sections(raw_text)
    if parsed.get("research_paper"):
        logger.info("Tier 2: Successfully extracted sections from text")
        return _normalize_response(parsed, breaking_news)

    # Tier 3: Use raw text as the research paper
    logger.warning("Tier 2 failed, using Tier 3: raw text fallback")
    return {
        "research_paper": raw_text,
        "related_articles": [],
        "supply_chain_impacts": [],
        "price_data": [],
        "metadata": {
            "query": breaking_news,
            "num_articles_found": 0,
            "num_supply_chain_records": 0,
            "commodities_analyzed": [],
            "generated_at": datetime.now().isoformat(),
        },
    }



def _extract_json_block(text: str) -> dict | None:
    """
    Extract a JSON object from markdown code fences in the text.
    Handles ```json ... ``` blocks.
    """
    # Try to find ```json ... ``` block
    pattern = r"```(?:json)?\s*\n?(.*?)\n?```"
    matches = re.findall(pattern, text, re.DOTALL)

    for match in matches:
        try:
            parsed = json.loads(match.strip())
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    # Try to find a raw JSON object (starts with { and ends with })
    # Find the outermost { ... } block
    brace_start = text.find("{")
    if brace_start != -1:
        depth = 0
        for i in range(brace_start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[brace_start : i + 1])
                    except json.JSONDecodeError:
                        break

    return None


def _extract_sections(text: str) -> dict:
    """
    Extract structured data from conversational text by looking for
    recognizable patterns and sections.
    """
    result = {
        "research_paper": "",
        "related_articles": [],
        "supply_chain_impacts": [],
        "price_data": [],
    }

    # Try to find embedded JSON arrays for articles
    articles_match = re.search(
        r'"related_articles"\s*:\s*(\[.*?\])', text, re.DOTALL
    )
    if articles_match:
        try:
            result["related_articles"] = json.loads(articles_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find supply chain data
    sc_match = re.search(
        r'"supply_chain_impacts"\s*:\s*(\[.*?\])', text, re.DOTALL
    )
    if sc_match:
        try:
            result["supply_chain_impacts"] = json.loads(sc_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find price data
    price_match = re.search(r'"price_data"\s*:\s*(\[.*?\])', text, re.DOTALL)
    if price_match:
        try:
            result["price_data"] = json.loads(price_match.group(1))
        except json.JSONDecodeError:
            pass

    # Try to find the research paper section
    paper_match = re.search(
        r'"research_paper"\s*:\s*"(.*?)"(?=\s*,\s*")', text, re.DOTALL
    )
    if paper_match:
        result["research_paper"] = paper_match.group(1).replace("\\n", "\n")
    else:
        # Look for markdown headers that indicate the research paper content
        paper_start = re.search(r"^#\s+Commodities Research Report", text, re.MULTILINE)
        if paper_start:
            result["research_paper"] = text[paper_start.start():]
        else:
            # Use the whole text as the paper if it looks like prose
            if len(text) > 500:
                result["research_paper"] = text

    return result


def _normalize_response(parsed: dict, breaking_news: str) -> dict:
    """
    Normalize the parsed dict to ensure all required fields are present
    and have the correct types.
    """
    articles = parsed.get("related_articles", [])
    supply_chain = parsed.get("supply_chain_impacts", [])
    price_data = parsed.get("price_data", [])

    # Normalize articles
    normalized_articles = []
    for a in articles:
        normalized_articles.append({
            "article_id": str(a.get("article_id", "")),
            "headline": str(a.get("headline", "")),
            "source": str(a.get("source", "")),
            "primary_commodity": str(a.get("primary_commodity", "")),
            "region": str(a.get("region", "")),
            "sector": str(a.get("sector", "")),
            "published_at": str(a.get("published_at", "")),
            "body": str(a.get("body", "")),
            "similarity_score": float(a.get("similarity_score", a.get("score", 0))),
        })

    # Normalize supply chain impacts
    normalized_sc = []
    for s in supply_chain:
        normalized_sc.append({
            "commodity": str(s.get("commodity", "")),
            "disruption_type": str(s.get("disruption_type", "")),
            "impact_level": str(s.get("impact_level", "")),
            "severity_score": float(s.get("severity_score", 0)),
            "affected_route": str(s.get("affected_route", "")),
            "affected_facility": str(s.get("affected_facility", "")),
            "price_change_pct": float(s.get("price_change_pct", 0)),
            "volume_disrupted_pct": float(s.get("volume_disrupted_pct", 0)),
            "mitigation_strategy": str(s.get("mitigation_strategy", "")),
            "downstream_impact_description": str(
                s.get("downstream_impact_description", "")
            ),
        })

    # Normalize price data
    normalized_prices = []
    for p in price_data:
        normalized_prices.append({
            "commodity": str(p.get("commodity", "")),
            "trade_date": str(p.get("trade_date", "")),
            "close_price": float(p.get("close_price", 0)),
        })

    # Extract commodities analyzed
    commodities = list(set(
        [a["primary_commodity"] for a in normalized_articles if a["primary_commodity"]]
        + [s["commodity"] for s in normalized_sc if s["commodity"]]
    ))

    # Build metadata — prefer parsed metadata if available, otherwise construct
    metadata = parsed.get("metadata", {})
    if not metadata or not isinstance(metadata, dict):
        metadata = {}

    return {
        "research_paper": parsed.get("research_paper", ""),
        "related_articles": normalized_articles,
        "supply_chain_impacts": normalized_sc,
        "price_data": normalized_prices,
        "metadata": {
            "query": metadata.get("query", breaking_news),
            "num_articles_found": metadata.get(
                "num_articles_found", len(normalized_articles)
            ),
            "num_supply_chain_records": metadata.get(
                "num_supply_chain_records", len(normalized_sc)
            ),
            "commodities_analyzed": metadata.get(
                "commodities_analyzed", commodities
            ),
            "generated_at": metadata.get(
                "generated_at", datetime.now().isoformat()
            ),
        },
    }


def _empty_response(breaking_news: str) -> dict:
    """Return an empty response structure."""
    return {
        "research_paper": "Research generation failed — no output from the agent pipeline.",
        "related_articles": [],
        "supply_chain_impacts": [],
        "price_data": [],
        "metadata": {
            "query": breaking_news,
            "num_articles_found": 0,
            "num_supply_chain_records": 0,
            "commodities_analyzed": [],
            "generated_at": datetime.now().isoformat(),
        },
    }
