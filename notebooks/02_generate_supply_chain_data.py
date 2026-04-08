# Databricks notebook source
# MAGIC %md
# MAGIC # Supply Chain Impact Data Generator
# MAGIC Generates synthetic supply chain data linked to commodities news articles.
# MAGIC Run this AFTER notebook 01_generate_news_data.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType, DoubleType, IntegerType, LongType, BooleanType
)
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

CATALOG = "commodities_research_catalog"
SCHEMA = "news_research"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load News Articles for Context

# COMMAND ----------

articles_df = spark.table(f"{CATALOG}.{SCHEMA}.news_articles")
articles = articles_df.select("article_id", "primary_commodity", "region", "sector", "published_at", "commodities").collect()
print(f"Loaded {len(articles)} articles for context")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supply Chain Impact Definitions

# COMMAND ----------

SUPPLY_CHAIN_NODES = {
    "Crude Oil": {
        "producers": ["Saudi Arabia", "Russia", "USA", "Iraq", "UAE", "Kuwait", "Iran", "Brazil", "Canada", "Norway"],
        "trade_routes": ["Strait of Hormuz", "Suez Canal", "Cape of Good Hope", "Strait of Malacca", "Panama Canal"],
        "key_facilities": ["Ras Tanura Terminal", "Kharg Island", "Basrah Oil Terminal", "Fujairah Port", "Rotterdam Hub"],
        "downstream_industries": ["Refining", "Petrochemicals", "Transportation", "Aviation", "Plastics Manufacturing"],
        "unit": "barrels/day",
        "base_price": 95.0,
        "price_range": (75, 140),
        "typical_volume": 100000000
    },
    "Natural Gas": {
        "producers": ["USA", "Russia", "Qatar", "Iran", "Australia", "Norway", "Algeria", "Canada"],
        "trade_routes": ["Strait of Hormuz", "Nord Stream", "Trans-Mediterranean", "LNG Shipping Lanes"],
        "key_facilities": ["Ras Laffan LNG", "Sabine Pass LNG", "Yamal LNG", "North West Shelf", "Gorgon LNG"],
        "downstream_industries": ["Power Generation", "Heating", "Fertilizer Production", "Petrochemicals", "Industrial"],
        "unit": "MMBtu",
        "base_price": 3.01,
        "price_range": (2.5, 8.0),
        "typical_volume": 400000000000
    },
    "LNG": {
        "producers": ["Qatar", "Australia", "USA", "Russia", "Malaysia", "Indonesia", "Nigeria"],
        "trade_routes": ["Strait of Hormuz", "Suez Canal", "Strait of Malacca", "Cape of Good Hope"],
        "key_facilities": ["Ras Laffan", "Sabine Pass", "Cameron LNG", "Ichthys LNG", "Prelude FLNG"],
        "downstream_industries": ["Power Generation", "Industrial", "Residential Heating", "Transportation"],
        "unit": "MMBtu",
        "base_price": 22.5,
        "price_range": (15, 35),
        "typical_volume": 55000000000
    },
    "Copper": {
        "producers": ["Chile", "Peru", "DRC", "China", "USA", "Indonesia", "Australia", "Zambia"],
        "trade_routes": ["Pacific Shipping Routes", "Atlantic Routes", "Suez Canal"],
        "key_facilities": ["Escondida Mine", "Grasberg Mine", "Kamoa-Kakula Mine", "Chuquicamata", "Shanghai Exchange"],
        "downstream_industries": ["Electronics", "Construction", "EV Manufacturing", "Power Grid", "Renewable Energy"],
        "unit": "metric tons",
        "base_price": 13238.0,
        "price_range": (10000, 15000),
        "typical_volume": 25000000
    },
    "Gold": {
        "producers": ["China", "Australia", "Russia", "Canada", "USA", "Ghana", "South Africa", "Peru"],
        "trade_routes": ["London-Zurich-Hong Kong", "Perth-Shanghai", "New York-London"],
        "key_facilities": ["London Bullion Market", "COMEX Vaults", "Perth Mint", "Shanghai Gold Exchange", "Swiss Refineries"],
        "downstream_industries": ["Jewelry", "Investment", "Central Banks", "Electronics", "Dentistry"],
        "unit": "troy ounces",
        "base_price": 2250.0,
        "price_range": (1900, 2600),
        "typical_volume": 4500
    },
    "Aluminum": {
        "producers": ["China", "India", "Russia", "Canada", "UAE", "Bahrain", "Australia", "Norway"],
        "trade_routes": ["Strait of Hormuz", "Suez Canal", "Pacific Routes", "Atlantic Routes"],
        "key_facilities": ["Alba Smelter (Bahrain)", "EGA (UAE)", "Rusal Facilities", "Alcoa Refineries", "LME Warehouses"],
        "downstream_industries": ["Automotive", "Packaging", "Construction", "Aerospace", "Electronics"],
        "unit": "metric tons",
        "base_price": 2650.0,
        "price_range": (2200, 3200),
        "typical_volume": 70000000
    },
    "Wheat": {
        "producers": ["Russia", "EU", "China", "India", "USA", "Canada", "Australia", "Ukraine", "Argentina"],
        "trade_routes": ["Black Sea Routes", "Panama Canal", "Suez Canal", "Pacific Routes"],
        "key_facilities": ["Novorossiysk Port", "US Gulf Terminals", "Rouen Port", "Santos Port", "Geelong Port"],
        "downstream_industries": ["Flour Milling", "Baking", "Animal Feed", "Food Processing", "Biofuel"],
        "unit": "bushels",
        "base_price": 5.80,
        "price_range": (4.5, 8.0),
        "typical_volume": 800000000
    },
    "Urea": {
        "producers": ["China", "India", "Russia", "Qatar", "Saudi Arabia", "Indonesia", "Iran", "Egypt"],
        "trade_routes": ["Strait of Hormuz", "Suez Canal", "Indian Ocean Routes", "Pacific Routes"],
        "key_facilities": ["QAFCO (Qatar)", "Ma'aden (Saudi)", "IFFCO (India)", "Yara Plants", "CF Industries"],
        "downstream_industries": ["Agriculture", "Chemical Manufacturing", "AdBlue/DEF Production", "Resin Manufacturing"],
        "unit": "metric tons",
        "base_price": 380.0,
        "price_range": (250, 600),
        "typical_volume": 180000000
    },
    "Lithium": {
        "producers": ["Australia", "Chile", "China", "Argentina", "Brazil", "Zimbabwe"],
        "trade_routes": ["Pacific Routes", "Atlantic Routes", "Asia-Oceania Routes"],
        "key_facilities": ["Greenbushes Mine", "Salar de Atacama", "Pilbara Minerals", "Ganfeng Lithium", "Albemarle Plants"],
        "downstream_industries": ["EV Batteries", "Consumer Electronics", "Energy Storage", "Ceramics", "Pharmaceuticals"],
        "unit": "metric tons LCE",
        "base_price": 18500.0,
        "price_range": (12000, 30000),
        "typical_volume": 1000000
    },
    "Helium": {
        "producers": ["USA", "Qatar", "Algeria", "Russia", "Australia", "Tanzania"],
        "trade_routes": ["Strait of Hormuz", "Atlantic Routes", "Pacific Routes"],
        "key_facilities": ["Ras Laffan (Qatar)", "BLM Crude Helium Enrichment", "Darwin LNG", "Algeria Skikda"],
        "downstream_industries": ["Semiconductor Manufacturing", "MRI Systems", "Fiber Optics", "Welding", "Scientific Research"],
        "unit": "million cubic feet",
        "base_price": 450.0,
        "price_range": (300, 800),
        "typical_volume": 6000
    }
}

DISRUPTION_TYPES = [
    "Route Blockage", "Port Congestion", "Facility Shutdown", "Labor Strike",
    "Weather Event", "Sanctions Impact", "Export Restriction", "Military Conflict",
    "Infrastructure Failure", "Regulatory Change", "Demand Surge", "Inventory Depletion"
]

IMPACT_LEVELS = ["Critical", "High", "Medium", "Low"]

MITIGATION_STRATEGIES = [
    "Alternative routing via Cape of Good Hope",
    "Strategic reserve drawdown",
    "Emergency procurement from alternative suppliers",
    "Demand rationing and conservation protocols",
    "Supplier diversification program",
    "Inventory buffer increase",
    "Long-term contract renegotiation",
    "Domestic production acceleration",
    "Fuel switching to alternative energy sources",
    "Diplomatic intervention and negotiations",
    "Force majeure clause activation",
    "Spot market procurement at premium"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Supply Chain Records

# COMMAND ----------

def generate_supply_chain_record(article, record_date):
    """Generate a supply chain impact record linked to a news article."""
    commodity = article.primary_commodity

    # Get commodity-specific data or use defaults
    commodity_data = SUPPLY_CHAIN_NODES.get(commodity, {
        "producers": ["USA", "China", "EU"],
        "trade_routes": ["Global Shipping Lanes"],
        "key_facilities": ["Various"],
        "downstream_industries": ["Manufacturing", "Industrial"],
        "unit": "metric tons",
        "base_price": 100.0,
        "price_range": (50, 200),
        "typical_volume": 1000000
    })

    # Determine disruption severity based on region
    is_hormuz_related = article.region in ["Middle East", "Arabian Gulf"]
    base_severity = random.uniform(0.6, 1.0) if is_hormuz_related else random.uniform(0.1, 0.7)

    impact_level = (
        "Critical" if base_severity > 0.8 else
        "High" if base_severity > 0.6 else
        "Medium" if base_severity > 0.3 else
        "Low"
    )

    # Price impact
    price_min, price_max = commodity_data["price_range"]
    current_price = round(random.uniform(
        commodity_data["base_price"] * 0.9,
        commodity_data["base_price"] * (1.3 if is_hormuz_related else 1.1)
    ), 2)
    price_change_pct = round(random.uniform(-5, 25) if is_hormuz_related else random.uniform(-8, 12), 2)

    # Volume impact
    volume_disrupted_pct = round(random.uniform(10, 50) if is_hormuz_related else random.uniform(0, 20), 1)

    affected_route = random.choice(commodity_data["trade_routes"])
    affected_facility = random.choice(commodity_data["key_facilities"])
    affected_producer = random.choice(commodity_data["producers"])
    affected_industry = random.choice(commodity_data["downstream_industries"])

    return {
        "record_id": str(uuid.uuid4()),
        "article_id": article.article_id,
        "commodity": commodity,
        "recorded_at": record_date,
        "disruption_type": random.choice(DISRUPTION_TYPES),
        "impact_level": impact_level,
        "severity_score": round(base_severity, 3),
        "affected_route": affected_route,
        "affected_facility": affected_facility,
        "affected_producer_country": affected_producer,
        "affected_downstream_industry": affected_industry,
        "current_price": current_price,
        "price_change_pct": price_change_pct,
        "price_unit": commodity_data["unit"],
        "volume_disrupted_pct": volume_disrupted_pct,
        "estimated_duration_days": random.randint(3, 90),
        "alternative_supply_available": random.choice([True, False]),
        "mitigation_strategy": random.choice(MITIGATION_STRATEGIES),
        "downstream_impact_description": f"Disruption in {commodity} supply affecting {affected_industry} sector. "
            f"Estimated {volume_disrupted_pct}% volume reduction through {affected_route}. "
            f"Price impact of {price_change_pct}% observed. {affected_facility} operations "
            f"{'severely impacted' if impact_level in ['Critical', 'High'] else 'partially affected'}. "
            f"Key importing nations experiencing {'acute shortages' if impact_level == 'Critical' else 'supply tightness' if impact_level == 'High' else 'manageable constraints'}.",
        "region": article.region,
        "sector": article.sector,
        "confidence_score": round(random.uniform(0.6, 0.95), 3)
    }

# COMMAND ----------

supply_chain_records = []

for article in articles:
    record_date = article.published_at + timedelta(hours=random.randint(1, 12))

    # Generate 1-3 supply chain records per article
    num_records = random.randint(1, 3)
    for _ in range(num_records):
        record = generate_supply_chain_record(article, record_date)
        supply_chain_records.append(record)

print(f"Generated {len(supply_chain_records)} supply chain records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Supply Chain Data

# COMMAND ----------

sc_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("article_id", StringType(), False),
    StructField("commodity", StringType(), False),
    StructField("recorded_at", TimestampType(), False),
    StructField("disruption_type", StringType(), True),
    StructField("impact_level", StringType(), True),
    StructField("severity_score", DoubleType(), True),
    StructField("affected_route", StringType(), True),
    StructField("affected_facility", StringType(), True),
    StructField("affected_producer_country", StringType(), True),
    StructField("affected_downstream_industry", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("price_change_pct", DoubleType(), True),
    StructField("price_unit", StringType(), True),
    StructField("volume_disrupted_pct", DoubleType(), True),
    StructField("estimated_duration_days", IntegerType(), True),
    StructField("alternative_supply_available", BooleanType(), True),
    StructField("mitigation_strategy", StringType(), True),
    StructField("downstream_impact_description", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("confidence_score", DoubleType(), True)
])

sc_df = spark.createDataFrame(supply_chain_records, schema=sc_schema)
sc_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.supply_chain_impacts")

print(f"Wrote {sc_df.count()} records to {CATALOG}.{SCHEMA}.supply_chain_impacts")
sc_df.groupBy("impact_level").count().orderBy("count", ascending=False).show()
sc_df.groupBy("commodity").count().orderBy("count", ascending=False).show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Commodity Price History

# COMMAND ----------

def generate_price_history(commodity, commodity_data, start_date, end_date):
    """Generate daily price history for a commodity."""
    records = []
    current_price = commodity_data["base_price"]
    price_min, price_max = commodity_data["price_range"]

    current_date = start_date
    while current_date <= end_date:
        # Add realistic daily volatility with upward bias during crisis
        days_into_crisis = (current_date - datetime(2026, 2, 28)).days
        crisis_factor = min(0.02, days_into_crisis * 0.0005) if days_into_crisis > 0 else 0

        daily_return = random.gauss(crisis_factor, 0.025)
        current_price *= (1 + daily_return)
        current_price = max(price_min, min(price_max, current_price))

        high = current_price * (1 + random.uniform(0.005, 0.03))
        low = current_price * (1 - random.uniform(0.005, 0.03))
        open_price = current_price * (1 + random.uniform(-0.015, 0.015))
        volume = int(commodity_data["typical_volume"] * random.uniform(0.7, 1.5))

        records.append({
            "commodity": commodity,
            "trade_date": current_date,
            "open_price": round(open_price, 2),
            "high_price": round(high, 2),
            "low_price": round(low, 2),
            "close_price": round(current_price, 2),
            "volume": volume,
            "price_unit": commodity_data["unit"]
        })

        current_date += timedelta(days=1)

    return records

# Generate price history for all tracked commodities
price_records = []
start_date = datetime(2026, 3, 1)
end_date = datetime(2026, 4, 8)

for commodity, data in SUPPLY_CHAIN_NODES.items():
    records = generate_price_history(commodity, data, start_date, end_date)
    price_records.extend(records)

print(f"Generated {len(price_records)} price history records")

# COMMAND ----------

price_schema = StructType([
    StructField("commodity", StringType(), False),
    StructField("trade_date", TimestampType(), False),
    StructField("open_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("price_unit", StringType(), True)
])

price_df = spark.createDataFrame(price_records, schema=price_schema)
price_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.commodity_prices")

print(f"Wrote {price_df.count()} price records to {CATALOG}.{SCHEMA}.commodity_prices")
display(price_df.filter("commodity = 'Crude Oil'").orderBy("trade_date").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\nTables created in {CATALOG}.{SCHEMA}:")
print(f"  1. news_articles:         {spark.table(f'{CATALOG}.{SCHEMA}.news_articles').count()} rows")
print(f"  2. supply_chain_impacts:  {spark.table(f'{CATALOG}.{SCHEMA}.supply_chain_impacts').count()} rows")
print(f"  3. commodity_prices:      {spark.table(f'{CATALOG}.{SCHEMA}.commodity_prices').count()} rows")
