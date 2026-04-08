# Databricks notebook source
# MAGIC %md
# MAGIC # Commodities News Data Generator
# MAGIC Generates realistic synthetic commodities news articles from 5 major sources
# MAGIC based on real market events from March-April 2026.

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    ArrayType, DoubleType, IntegerType
)

spark = SparkSession.builder.getOrCreate()
fake = Faker()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "commodities_research_catalog"
SCHEMA = "news_research"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## News Article Templates Based on Real Events (March 29 - April 8, 2026)

# COMMAND ----------

SOURCES = ["Bloomberg", "Reuters", "S&P Global", "CNBC", "Financial Times"]

COMMODITIES = [
    "Crude Oil", "Brent Crude", "WTI Crude", "Natural Gas", "LNG",
    "Gold", "Silver", "Copper", "Aluminum", "Platinum",
    "Wheat", "Corn", "Soybeans", "Rice", "Sugar",
    "Iron Ore", "Lithium", "Nickel", "Zinc", "Palladium",
    "Urea", "Methanol", "Sulfur", "Helium", "Cobalt"
]

REGIONS = [
    "Middle East", "North America", "Europe", "Asia Pacific",
    "Latin America", "Africa", "Black Sea Region", "Arabian Gulf",
    "South Asia", "Southeast Asia"
]

SECTORS = [
    "Energy", "Precious Metals", "Industrial Metals",
    "Agriculture", "Petrochemicals", "Fertilizers", "Rare Earth"
]

# Real event-based article templates for realistic generation
ARTICLE_TEMPLATES = [
    # Strait of Hormuz / Iran crisis articles
    {
        "theme": "hormuz_crisis",
        "headlines": [
            "Strait of Hormuz Restrictions Tighten as Iran Expands Naval Patrols",
            "Oil Tanker Traffic Through Hormuz Drops {pct}% Amid Military Tensions",
            "Insurance Premiums for Gulf Shipping Surge to Record Highs",
            "Hormuz Crisis Enters Third Week: Global Energy Markets Brace for Extended Disruption",
            "Pakistan Proposes Two-Week Extension of Hormuz Deadline as Diplomatic Window Narrows",
            "Arabian Gulf LNG Exports Face Unprecedented Bottleneck as Strait Restrictions Hold",
            "Beyond Oil: {num} Commodities Facing Disruption from Hormuz Closure",
            "Iran-Backed Houthis Renew Red Sea Threats Amid Hormuz Standoff",
            "Strait of Hormuz: From Pricing Risk to Deliverability Crisis",
            "Global Commodity Markets Enter Uncharted Territory as Hormuz Disruption Persists"
        ],
        "commodities": ["Crude Oil", "Brent Crude", "WTI Crude", "LNG", "Natural Gas", "Methanol", "Sulfur", "Urea"],
        "regions": ["Middle East", "Arabian Gulf", "South Asia", "Asia Pacific"],
        "sector": "Energy",
        "body_templates": [
            "The effective closure of the Strait of Hormuz continues to reverberate through global commodity markets as {source} reports {detail}. Approximately 20% of the world's oil supply transits through the narrow waterway, and the ongoing restrictions have sent {commodity} prices {direction} by {pct}% over the past week. Analysts at {bank} warn that prolonged disruption could push {commodity} prices to ${price} per {unit}, levels not seen since {year}. The crisis, stemming from the February 28 joint US-Israeli strikes on Iranian targets, has escalated beyond initial expectations. Iran's retaliatory measures, including expanded naval patrols and mine-laying operations, have effectively reduced tanker traffic to a fraction of normal levels. {additional_detail} Market participants are closely watching diplomatic efforts, with Pakistan's Prime Minister proposing a two-week extension of the deadline and a temporary reopening of the strait. However, {analyst} at {bank} cautioned that 'even a partial reopening would take weeks to normalize shipping patterns, and insurance premiums will remain elevated for months.' The impact extends well beyond crude oil. Approximately 46% of global urea trade originates from the Arabian Gulf region, and fertilizer prices have surged {fert_pct}% since the crisis began. Methanol, aluminum, and sulfur supply chains are similarly strained, with downstream manufacturers in Asia reporting difficulty securing spot cargoes. {source} data shows that shipping rates for Very Large Crude Carriers (VLCCs) on the Middle East to Asia route have jumped to over ${vlcc_rate} per day, compared to ${vlcc_normal} before the crisis. Some traders report that cargoes are being rerouted around the Cape of Good Hope, adding 15-20 days to delivery times and further tightening global supply.",
            "The two-week diplomatic window proposed by Pakistan's Prime Minister Shehbaz Sharif represents the most significant de-escalation effort since the Strait of Hormuz crisis began in late February, {source} reports. Under the proposal, Iran would temporarily ease naval restrictions in the strait while multilateral negotiations proceed, potentially allowing limited tanker traffic to resume. {commodity} futures {direction} {pct}% on the news, as traders weighed the probability of a breakthrough against the risk of further escalation. {analyst} at {bank} noted that 'markets are pricing in roughly a 35% probability of successful negotiations, but the tail risks remain enormous.' The crisis has exposed the vulnerability of global commodity supply chains to geographic chokepoints. Beyond the immediate energy market impact, industries from semiconductor manufacturing to agriculture are feeling the effects. Helium supplies, critical for chipmaking, have been severely disrupted as LNG processing facilities in Qatar operate at reduced capacity. Several major chip manufacturers in Taiwan and South Korea have reportedly begun rationing helium stocks. {additional_detail} Regional responses vary significantly. India has activated emergency fuel rationing protocols and is negotiating emergency crude imports from Russia with sanctions waivers. Bangladesh has implemented rolling blackouts in industrial zones to conserve fuel. Meanwhile, China has accelerated drawdowns from its strategic petroleum reserve, releasing an estimated {spr_release} million barrels since the crisis began."
        ]
    },
    # Oil price volatility
    {
        "theme": "oil_prices",
        "headlines": [
            "Brent Crude Tumbles {pct}% as Ceasefire Hopes Emerge",
            "WTI Retreats Below ${price} as Diplomatic Breakthrough Looms",
            "Oil Markets Whiplash: Brent Swings ${swing} in Single Session",
            "OPEC+ Emergency Meeting Called as Oil Price Volatility Hits Record",
            "Crude Oil Implied Volatility Surges to 2020 Pandemic Levels",
            "Oil Traders Brace for 'Binary Outcome' on Iran Negotiations",
            "Brent-WTI Spread Widens to ${spread} Amid Hormuz Premium",
            "Energy Stocks Diverge as Oil Price Swings Create Winners and Losers",
            "US Strategic Petroleum Reserve Drawdown Fails to Cap Oil Rally",
            "Oil at ${price}: What This Means for Global Inflation Forecasts"
        ],
        "commodities": ["Crude Oil", "Brent Crude", "WTI Crude"],
        "regions": ["North America", "Middle East", "Europe", "Asia Pacific"],
        "sector": "Energy",
        "body_templates": [
            "{commodity} futures experienced extreme volatility on {day}, {direction} {pct}% to settle at ${price} per barrel as markets reacted to {catalyst}. The move represents one of the largest single-session swings since the pandemic-era disruptions of 2020, and has prompted margin calls across the derivatives complex. {source} data shows that open interest in {commodity} options has surged {oi_pct}% over the past month, with a pronounced skew toward out-of-the-money call options at the ${strike} level, suggesting traders are hedging against further upside risk. The CBOE Crude Oil Volatility Index (OVX) hit {ovx} on {day}, its highest level since April 2020. {analyst} at {bank} wrote in a research note that 'the current environment is characterized by genuine uncertainty rather than directional conviction, which is reflected in the elevated implied volatility across the curve.' OPEC+ has called an emergency virtual meeting for {meeting_date} to discuss potential production responses. However, analysts note that many OPEC+ members are already producing near capacity, limiting the cartel's ability to meaningfully increase output. Saudi Arabia's spare capacity is estimated at only {spare} million barrels per day, down from {spare_before} million before the crisis. The price action has significant implications for global inflation. {bank2} estimates that every $10 increase in oil prices adds approximately 0.3 percentage points to headline CPI in developed economies. With Brent having risen over ${total_rise} since the crisis began, central banks may face difficult decisions on monetary policy in the coming months.",
            "Oil markets are pricing in what traders describe as a 'binary outcome' on Iran negotiations, with {commodity} options showing an unusual bimodal distribution of expected prices. {source} reports that the 30-day options market implies a roughly equal probability of {commodity} trading at either ${low_price} or ${high_price} by mid-May, with relatively low probability of prices remaining near current levels. This 'barbell' positioning reflects the market's view that either a diplomatic resolution will trigger a sharp selloff, or a breakdown in talks will send prices significantly higher. {analyst} at {bank} observed that 'we haven't seen this kind of bimodal pricing since the 2022 Russia-Ukraine conflict.' Trading volumes have been extraordinary, with {exchange} reporting that {commodity} futures traded {volume} million contracts in March, a monthly record. The surge in activity has been driven by both commercial hedgers — airlines, refiners, and petrochemical companies locking in prices — and speculative accounts positioning for the binary outcome. Physically, the market structure tells a concerning story. {commodity} backwardation has steepened to ${backwardation} per barrel between the front month and six months out, indicating acute near-term supply tightness. Floating storage has declined to {floating} million barrels globally, well below the five-year average of {floating_avg} million."
        ]
    },
    # Copper and industrial metals
    {
        "theme": "industrial_metals",
        "headlines": [
            "Copper Hits Record ${price}/Ton on LME as Supply Tightens",
            "Chinese Industrial Demand Drives Copper to 5-Month High",
            "LME Copper Inventories Fall to Critical Levels Amid Smelter Bottlenecks",
            "Aluminum Premiums Surge as Hormuz Disruption Cuts Arabian Gulf Exports",
            "Green Energy Transition Metals Face Supply Squeeze: Copper, Lithium, Nickel",
            "Copper-Gold Ratio Signals Industrial Resilience Despite Geopolitical Risks",
            "Mining Giants Report Record Q1 Earnings on Elevated Metal Prices",
            "EV Battery Metal Prices Diverge: Lithium Rebounds While Cobalt Lags",
            "Iron Ore Steadies Near ${price}/Ton as China Stimulus Expectations Build",
            "Zinc Deficit Widens as European Smelters Struggle with Energy Costs"
        ],
        "commodities": ["Copper", "Aluminum", "Lithium", "Nickel", "Iron Ore", "Zinc", "Cobalt"],
        "regions": ["Asia Pacific", "Europe", "Latin America", "Africa"],
        "sector": "Industrial Metals",
        "body_templates": [
            "LME {commodity} prices surged to ${price} per ton on {day}, marking a new {timeframe} high as {catalyst}. The rally extends a remarkable run that has seen {commodity} appreciate over {ytd_pct}% year-to-date on COMEX, driven by a confluence of supply constraints and robust demand from China's industrial sector. {source} reports that Chinese {commodity} imports rose {import_pct}% year-over-year in March, as manufacturers took advantage of a brief price dip to rebuild depleted inventories. Physical premiums in Shanghai have climbed to ${premium}/ton over LME, the highest since {premium_year}. On the supply side, {supply_detail}. LME warehouse inventories stand at just {inventory} metric tons, representing only {days_supply} days of global consumption — a level that market participants describe as critically low. {analyst} at {bank} raised their 12-month {commodity} price target to ${target}/ton, citing 'a structural deficit that is only deepening as the energy transition accelerates demand while permitting timelines for new mines continue to extend.' The green energy transition remains the dominant demand driver. Each megawatt of offshore wind capacity requires approximately {wind_copper} tons of copper, while a typical electric vehicle uses {ev_copper} kg compared to {ice_copper} kg for an internal combustion engine. With global EV sales projected to reach {ev_sales} million units in 2026, the incremental copper demand from electrification alone is estimated at {incremental} million tons.",
            "{commodity} markets are experiencing a historic supply squeeze as the Strait of Hormuz disruption compounds existing structural deficits. {source} reports that Arabian Gulf aluminum and nickel exports have been severely curtailed, with some Asian buyers unable to secure spot cargoes at any price. The situation is particularly acute for aluminum, where the Middle East accounts for approximately {me_share}% of global primary production. Smelters in Bahrain and the UAE, which together produce over {me_production} million tons annually, are operating normally but face mounting logistics challenges in shipping finished product. {analyst} at {bank} estimates that the disruption has effectively removed {removed} million tons of annualized aluminum supply from accessible markets. For {commodity} specifically, {detail}. Looking ahead, the fundamental picture remains supportive. Global {commodity} demand is projected to grow {demand_growth}% annually through 2030, driven primarily by electrification and infrastructure investment. However, the pipeline of new mining projects has thinned considerably, with average permitting timelines now exceeding {permit_years} years in major jurisdictions. 'We are entering an era of structural metal scarcity,' warned {analyst2} at {bank2}. 'The investment needed to meet demand growth simply hasn't materialized, and the Hormuz crisis is a stark reminder of how fragile supply chains remain.'"
        ]
    },
    # Natural gas / LNG
    {
        "theme": "natural_gas",
        "headlines": [
            "European Natural Gas Prices Spike {pct}% on Hormuz Supply Fears",
            "Henry Hub Gas Forecast Trimmed to ${price}/MMBtu for Q2",
            "LNG Spot Prices in Asia Hit ${price}/MMBtu as Qatari Exports Stall",
            "European Gas Storage Drawdowns Accelerate Despite Spring Temperatures",
            "US LNG Export Terminals Running at Full Capacity to Fill Global Gap",
            "TTF Natural Gas Futures Surge Past EUR{price}/MWh",
            "Natural Gas: The Hidden Casualty of the Hormuz Crisis",
            "Global LNG Market Tightens as New Export Capacity Delays Mount",
            "Gas-to-Coal Switching Resumes in Europe as Prices Cross Threshold",
            "EIA Trims Q2 Henry Hub Forecast Amid Shifting Supply-Demand Balance"
        ],
        "commodities": ["Natural Gas", "LNG"],
        "regions": ["Europe", "Middle East", "North America", "Asia Pacific"],
        "sector": "Energy",
        "body_templates": [
            "European natural gas prices surged {pct}% on {day} as the Strait of Hormuz crisis continues to disrupt LNG shipments from the Arabian Gulf, {source} reports. The Dutch TTF benchmark hit EUR{ttf_price}/MWh, its highest level since the energy crisis of 2022, as traders scramble to secure alternative supplies ahead of the 2026-27 heating season. Qatar, the world's largest LNG exporter, has seen approximately {qatar_pct}% of its export volumes disrupted by the Hormuz restrictions. While some cargoes are being rerouted through the Bab el-Mandeb strait and around the Cape of Good Hope, the additional transit time of 15-20 days has effectively reduced available supply to European and Asian markets. {analyst} at {bank} estimates that the disruption is equivalent to removing {removed} billion cubic meters of annualized LNG supply from the global market. 'This is a supply shock that rivals the 2022 disruption in magnitude, but is occurring at a time when global LNG spare capacity is even more limited,' the analyst noted. European gas storage levels stand at {storage}% of capacity, compared to {storage_avg}% at this point last year. While this is above the EU's mandated minimum of 90% by November 1, the current rate of drawdown — driven by both heating demand and industrial consumption — suggests that meeting the target will require significant additional LNG imports or demand destruction. US LNG exports have ramped to record levels, with facilities operating at {util}% utilization. However, the incremental US supply is insufficient to offset the full Qatari disruption, and domestic Henry Hub prices have risen to ${hh_price}/MMBtu as a result.",
            "The US Energy Information Administration trimmed its Henry Hub natural gas spot price forecast for Q2 and Q3 2026 in its April Short-Term Energy Outlook, projecting prices to average ${hh_price}/MMBtu, {source} reports. Despite the global LNG supply crunch driven by the Hormuz crisis, US domestic gas markets remain relatively insulated due to robust production from the Permian and Appalachian basins. US dry natural gas production averaged {production} billion cubic feet per day in March, near record levels, as producers respond to elevated prices and strong export demand. However, {analyst} at {bank} cautioned that 'the disconnect between US domestic prices and global LNG prices cannot persist indefinitely — as more export capacity comes online, Henry Hub will increasingly reflect global dynamics.' The LNG market disruption has had cascading effects across Asian economies. Japan and South Korea, which rely on LNG for {jk_share}% and {sk_share}% of their electricity generation respectively, are negotiating emergency procurement contracts at significant premiums. Spot LNG prices in Northeast Asia have risen to ${asia_lng}/MMBtu, nearly triple the level of six months ago. In response, several Asian utilities have increased coal-fired generation, raising concerns about emissions targets. {detail}"
        ]
    },
    # Gold and precious metals
    {
        "theme": "precious_metals",
        "headlines": [
            "Gold Retreats as Risk Appetite Returns on Ceasefire Hopes",
            "Silver Underperforms Gold as Industrial Demand Outlook Clouded",
            "Central Bank Gold Buying Hits Record in Q1 2026",
            "Gold-to-Oil Ratio Signals Unusual Market Stress",
            "Platinum Deficit Widens as South African Mining Disruptions Persist",
            "Gold Pulls Back {pct}% After Historic Rally, Analysts See Support at ${price}",
            "Palladium Surges on Renewed Supply Concerns from Russia",
            "Precious Metals Decline {pct}% Alongside Equities in Risk-Off Session",
            "Gold ETF Flows Reverse After Three Months of Inflows",
            "Silver-Gold Ratio Hits {ratio}: What It Signals for Markets"
        ],
        "commodities": ["Gold", "Silver", "Platinum", "Palladium"],
        "regions": ["North America", "Europe", "Africa", "Asia Pacific"],
        "sector": "Precious Metals",
        "body_templates": [
            "Gold prices {direction} {pct}% to ${price} per ounce on {day} as {catalyst}. The move comes after a turbulent March that saw precious metals decline alongside equities and bonds — an unusual correlation breakdown that reflects the complex interplay of inflation fears, geopolitical risk, and liquidity dynamics in current markets. {source} reports that central banks purchased a record {cb_tons} metric tons of gold in Q1 2026, led by China, India, Turkey, and Poland. The buying spree reflects a continued shift away from dollar-denominated reserves and toward hard assets, a trend that has accelerated since the freezing of Russian central bank assets in 2022. {analyst} at {bank} noted that 'central bank demand is providing a structural floor for gold prices, even as ETF investors have become more tactical in their positioning.' Gold ETF holdings {etf_direction} by {etf_tons} tons in March, bringing total holdings to {total_etf} tons, the {etf_level} level since {etf_year}. The precious metals complex presents a mixed picture. While gold has benefited from safe-haven demand, silver has underperformed due to its dual nature as both a precious and industrial metal. The gold-silver ratio stands at {gs_ratio}, above the 10-year average of {gs_avg}, suggesting silver is historically cheap relative to gold. Platinum continues to trade at a significant discount to gold, with the spread at ${pt_discount}/oz, as the transition away from internal combustion engines weighs on long-term demand expectations for autocatalysts. However, growing hydrogen economy applications and persistent supply challenges in South Africa are providing offsetting support.",
            "The precious metals market is navigating a complex landscape where traditional safe-haven dynamics are being distorted by the unprecedented nature of the current geopolitical crisis, {source} reports. Gold's retreat from its March highs has caught many investors off guard, as the metal has failed to sustain its rally despite escalating tensions in the Middle East. {analyst} at {bank} attributes this to 'a liquidity-driven selloff where investors are raising cash across all asset classes to meet margin calls in energy derivatives.' The correlation between gold and the S&P 500 has risen to {correlation} over the past 30 days, compared to the historical average of {corr_avg}, confirming the unusual market dynamics. However, the analyst expects this correlation to break down as the immediate margin-call pressures ease, and forecasts gold to reach ${target}/oz by year-end. Physical gold demand remains robust. {detail_physical}. Meanwhile, platinum group metals are telling their own story. {pgm_detail}."
        ]
    },
    # Agriculture and fertilizers
    {
        "theme": "agriculture",
        "headlines": [
            "Wheat Prices Steady as Black Sea Exports Hold Despite Global Tensions",
            "Fertilizer Prices Surge {pct}% as Arabian Gulf Urea Exports Disrupted",
            "Corn Futures Rally on US Planting Delays and Export Demand",
            "S&P Global Launches Black Sea Wheat Price Benchmark",
            "Global Food Security Alert: Fertilizer Disruption Threatens 2026 Harvests",
            "Soybean Markets Eye South American Harvest Amid Logistics Challenges",
            "Rice Prices Firm as Asian Nations Build Strategic Reserves",
            "Sugar Hits {timeframe} High on Brazilian Real Strength and Ethanol Demand",
            "Potash Producers Rally as Fertilizer Supply Crunch Deepens",
            "USDA Projects Record Global Wheat Production Despite Regional Risks"
        ],
        "commodities": ["Wheat", "Corn", "Soybeans", "Rice", "Sugar", "Urea"],
        "regions": ["Black Sea Region", "North America", "Latin America", "South Asia", "Southeast Asia"],
        "sector": "Agriculture",
        "body_templates": [
            "Agricultural commodity markets are facing a complex outlook as the Strait of Hormuz crisis disrupts fertilizer supply chains while grain fundamentals remain broadly supportive, {source} reports. {commodity} prices {direction} {pct}% on {day} as {catalyst}. The fertilizer market is bearing the brunt of the disruption. The Arabian Gulf accounts for approximately 20% of all seaborne fertilizer exports, with 46% of global urea trade originating from the region. {analyst} at {bank} estimates that the Hormuz restrictions have effectively removed {removed} million tons of annualized urea supply from accessible markets, sending benchmark prices up {fert_pct}% since early March. The implications for the 2026 growing season are significant. Farmers in South and Southeast Asia, who are preparing for the monsoon planting season, are facing sharply higher input costs. India, the world's largest urea importer, has been forced to tender emergency purchases at premiums of ${premium}/ton over pre-crisis levels. The Indian government is considering expanding subsidies to shield farmers from the price shock. In contrast, grain markets have shown relative resilience. Global wheat and rice supplies remain ample, with the USDA projecting record world wheat production of {wheat_prod} million metric tons for the 2026/27 marketing year. The Black Sea region continues to export at near-normal levels, and S&P Global's newly launched Platts Milling Wheat Marker provides enhanced price transparency for the region's trade. {additional}",
            "{commodity} futures {direction} {pct}% to ${price} per bushel on {day} as {catalyst}. The move reflects the growing tension between comfortable global grain supplies and the risk of input cost inflation from fertilizer disruptions. {source} reports that US planting progress for spring crops is running behind the five-year average, with {planted_pct}% of intended corn acres planted versus {avg_planted}% normally by this date. Wet conditions across the Midwest have delayed fieldwork, and some agronomists are beginning to discuss the possibility of acreage switching from corn to soybeans if planting windows close. The fertilizer situation adds another layer of uncertainty. With nitrogen fertilizer prices elevated, some US farmers are reporting plans to reduce application rates, which could impact yields. {analyst} at {bank} estimates that a {reduction}% reduction in nitrogen application rates across the US Corn Belt could reduce yields by {yield_impact} bushels per acre, equivalent to a {prod_impact} million metric ton reduction in production. Meanwhile, South American harvests are progressing well. Brazil's soybean harvest is {brazil_pct}% complete, running ahead of last year's pace, and Argentine corn production is projected at {arg_corn} million metric tons, above initial estimates. {detail}"
        ]
    },
    # Helium and specialty commodities
    {
        "theme": "specialty",
        "headlines": [
            "Helium Shortage Threatens Global Chipmaking as LNG Disruption Spreads",
            "Semiconductor Supply Chain Faces New Headwind from Helium Scarcity",
            "Methanol Prices Double Since Hormuz Crisis Began",
            "Graphite Supply Concerns Mount as Clean Energy Demand Surges",
            "Rare Earth Elements: China Tightens Export Controls Amid Global Tensions",
            "Sulfur Prices Surge as Refinery Output Shifts Disrupt Byproduct Supply",
            "Cobalt Market Rebalances as Congo Output Stabilizes",
            "Lithium Spot Prices Rebound {pct}% from 2025 Lows on EV Demand Recovery",
            "Critical Minerals Supply Chain Mapping Becomes National Security Priority",
            "Neon Gas Prices Spike on Renewed Ukraine Conflict Fears"
        ],
        "commodities": ["Helium", "Methanol", "Lithium", "Cobalt", "Sulfur"],
        "regions": ["Middle East", "Asia Pacific", "Africa", "North America"],
        "sector": "Petrochemicals",
        "body_templates": [
            "The Strait of Hormuz crisis is exposing critical vulnerabilities in specialty commodity supply chains that extend far beyond oil and gas, {source} reports. {commodity} prices have {direction} {pct}% since the disruption began, with downstream industries scrambling to secure alternative supplies. The helium market has been particularly hard hit. Qatar, which accounts for approximately {qatar_share}% of global helium supply through its LNG processing operations, has seen exports severely curtailed. Helium is a critical input in semiconductor manufacturing, where it is used for cooling and purging in chip fabrication processes. {analyst} at {bank} reports that major chipmakers in Taiwan and South Korea have begun rationing helium stocks, with some facilities operating at reduced capacity. 'This is a supply chain vulnerability that few had fully appreciated,' the analyst noted. 'The concentration of helium production in a handful of LNG-rich countries creates a single point of failure for the global semiconductor industry.' TSMC and Samsung have declined to comment on specific production impacts but acknowledged in recent filings that 'certain input materials' face supply constraints. Industry sources suggest that helium inventories at major fabs are sufficient for {weeks} weeks of normal operations, after which production cuts may become necessary. {detail}",
            "The market for {commodity} is undergoing a significant transformation as geopolitical disruptions and the energy transition reshape supply-demand dynamics, {source} reports. Prices have {direction} {pct}% {timeframe}, reflecting {catalyst}. {detail_main} The specialty commodities space is increasingly being viewed through a national security lens. The US, EU, and Japan have all published updated critical minerals lists in 2026 that expand the scope of strategic materials beyond traditional definitions. {analyst} at {bank} notes that 'the Hormuz crisis has accelerated a fundamental rethinking of commodity supply chain resilience — governments and companies are now willing to pay significant premiums for supply diversity and geographic redundancy.' Investment in alternative sources is ramping up. {investment_detail} However, the timeline for new capacity to come online remains measured in years, not months. In the interim, prices are likely to remain elevated and volatile, with {commodity} particularly exposed to {risk_factor}."
        ]
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Articles

# COMMAND ----------

BANKS = [
    "Goldman Sachs", "Morgan Stanley", "JPMorgan", "Citigroup", "Bank of America",
    "Barclays", "Deutsche Bank", "UBS", "Credit Suisse", "HSBC",
    "Macquarie", "RBC Capital Markets", "Wells Fargo", "BNP Paribas", "Societe Generale"
]

ANALYSTS = [
    "Sarah Chen", "Michael Torres", "Priya Sharma", "David Okonkwo", "Emma Lindqvist",
    "James McAllister", "Amir Hassan", "Rachel Kim", "Thomas Brandt", "Maria Garcia",
    "Alexander Petrov", "Yuki Tanaka", "Benjamin Cole", "Fatima Al-Rashid", "Henrik Johansson"
]

def generate_article(template, article_date, source):
    """Generate a single synthetic news article from a template."""
    theme = template["theme"]
    headline_template = random.choice(template["headlines"])
    body_template = random.choice(template["body_templates"])
    commodity = random.choice(template["commodities"])
    region = random.choice(template["regions"])
    sector = template["sector"]

    # Generate random but realistic values
    pct = round(random.uniform(1.5, 15.0), 1)
    price = round(random.uniform(50, 2500), 2)
    num = random.randint(5, 12)
    swing = round(random.uniform(3, 12), 2)
    spread = round(random.uniform(5, 20), 2)
    ratio = round(random.uniform(75, 95), 1)

    # Format headline
    headline = headline_template.format(
        pct=pct, price=price, num=num, swing=swing,
        spread=spread, ratio=ratio,
        timeframe=random.choice(["5-year", "3-year", "10-year", "multi-year"])
    )

    # Select analyst and bank
    analyst = random.choice(ANALYSTS)
    bank = random.choice(BANKS)
    bank2 = random.choice([b for b in BANKS if b != bank])
    analyst2 = random.choice([a for a in ANALYSTS if a != analyst])

    # Generate body with substitutions
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    body = body_template.format(
        source=source,
        commodity=commodity,
        direction=random.choice(["surged", "jumped", "rallied", "climbed", "rose", "declined", "fell", "retreated", "dropped", "tumbled"]),
        pct=pct,
        price=round(random.uniform(50, 2500), 2),
        unit=random.choice(["barrel", "ton", "ounce", "MMBtu", "bushel"]),
        year=random.choice(["2008", "2011", "2014", "2020", "2022"]),
        bank=bank, bank2=bank2,
        analyst=analyst, analyst2=analyst2,
        day=random.choice(days),
        catalyst=random.choice([
            "reports of diplomatic progress on the Hormuz crisis",
            "escalating tensions in the Strait of Hormuz",
            "stronger-than-expected Chinese industrial data",
            "concerns over supply chain disruptions",
            "unexpected inventory drawdowns",
            "OPEC+ production uncertainty",
            "rising insurance premiums for Gulf shipping",
            "central bank policy divergence"
        ]),
        detail=random.choice([
            "tanker bookings from the Arabian Gulf have fallen to their lowest level in over a decade",
            "refinery utilization rates in Asia have dropped below 80% due to feedstock shortages",
            "warehouse inventories are at critically low levels across major trading hubs",
            "physical premiums have surged to multi-year highs in key consuming regions"
        ]),
        additional_detail=random.choice([
            "Several major shipping companies have suspended transits pending further clarity on the security situation.",
            "The International Energy Agency has convened an emergency meeting to assess the supply impact.",
            "Neighboring Oman has offered to facilitate alternative shipping routes through its territorial waters.",
            "Insurance underwriters have classified the Strait as a war-risk zone, triggering force majeure clauses in multiple contracts."
        ]),
        fert_pct=round(random.uniform(20, 60), 1),
        vlcc_rate=random.randint(100000, 250000),
        vlcc_normal=random.randint(30000, 60000),
        spr_release=random.randint(15, 50),
        ttf_price=round(random.uniform(45, 85), 2),
        qatar_pct=round(random.uniform(40, 70), 0),
        removed=round(random.uniform(10, 50), 1),
        storage=round(random.uniform(35, 55), 1),
        storage_avg=round(random.uniform(45, 65), 1),
        util=round(random.uniform(92, 99), 1),
        hh_price=round(random.uniform(2.80, 3.50), 2),
        production=round(random.uniform(103, 108), 1),
        jk_share=random.randint(25, 40),
        sk_share=random.randint(20, 35),
        asia_lng=round(random.uniform(18, 30), 2),
        meeting_date=random.choice(["April 10", "April 12", "April 15"]),
        spare=round(random.uniform(1.0, 2.5), 1),
        spare_before=round(random.uniform(3.0, 4.5), 1),
        total_rise=random.randint(15, 40),
        low_price=random.randint(70, 85),
        high_price=random.randint(120, 160),
        exchange=random.choice(["CME", "ICE", "NYMEX"]),
        volume=round(random.uniform(15, 30), 1),
        backwardation=round(random.uniform(5, 15), 2),
        floating=round(random.uniform(40, 70), 0),
        floating_avg=round(random.uniform(80, 120), 0),
        oi_pct=round(random.uniform(15, 40), 0),
        strike=random.randint(120, 180),
        ovx=round(random.uniform(55, 85), 1),
        timeframe=random.choice(["year-to-date", "since the crisis began", "in Q1", "this quarter"]),
        ytd_pct=round(random.uniform(10, 40), 1),
        import_pct=round(random.uniform(5, 20), 1),
        premium=random.randint(80, 200),
        premium_year=random.choice(["2021", "2022", "2023"]),
        supply_detail=random.choice([
            "major mines in Chile and Peru are facing water scarcity issues that limit expansion",
            "Indonesian export restrictions continue to tighten available supply",
            "labor disputes at key African mining operations remain unresolved",
            "permitting delays have pushed back several large-scale projects by 2-3 years"
        ]),
        inventory=random.randint(50000, 200000),
        days_supply=round(random.uniform(2, 8), 1),
        target=random.randint(10000, 15000),
        wind_copper=round(random.uniform(8, 15), 1),
        ev_copper=random.randint(60, 85),
        ice_copper=random.randint(18, 25),
        ev_sales=round(random.uniform(18, 25), 1),
        incremental=round(random.uniform(1.5, 3.0), 1),
        me_share=random.randint(8, 15),
        me_production=round(random.uniform(5, 8), 1),
        cb_tons=random.randint(280, 400),
        etf_direction=random.choice(["increased", "decreased"]),
        etf_tons=random.randint(20, 80),
        total_etf=random.randint(2800, 3500),
        etf_level=random.choice(["highest", "lowest"]),
        etf_year=random.choice(["2023", "2024", "early 2025"]),
        gs_ratio=round(random.uniform(78, 92), 1),
        gs_avg=round(random.uniform(72, 82), 1),
        pt_discount=random.randint(800, 1200),
        correlation=round(random.uniform(0.5, 0.8), 2),
        corr_avg=round(random.uniform(-0.2, 0.1), 2),
        detail_physical=random.choice([
            "Indian gold imports surged 35% in March as jewelers restocked ahead of the wedding season",
            "The Perth Mint reported record retail demand for gold bars and coins in Q1",
            "Chinese gold premiums have risen to $25/oz over London spot, indicating strong domestic demand"
        ]),
        pgm_detail=random.choice([
            "Platinum deficit is projected to widen to 500,000 oz in 2026, driven by South African electricity constraints",
            "Palladium prices have found support from renewed supply concerns related to Russian exports",
            "Rhodium has rebounded 20% from its 2025 lows as automotive catalyst demand stabilizes"
        ]),
        wheat_prod=random.randint(790, 810),
        planted_pct=random.randint(5, 15),
        avg_planted=random.randint(15, 25),
        reduction=random.randint(5, 15),
        yield_impact=round(random.uniform(5, 15), 1),
        prod_impact=round(random.uniform(5, 15), 1),
        brazil_pct=random.randint(70, 95),
        arg_corn=round(random.uniform(48, 56), 1),
        qatar_share=random.randint(25, 35),
        weeks=random.randint(4, 10),
        demand_growth=round(random.uniform(2, 5), 1),
        permit_years=random.randint(7, 15),
        risk_factor=random.choice([
            "further Hormuz disruption",
            "Chinese policy shifts",
            "regulatory changes in key producing countries",
            "energy transition timeline uncertainty"
        ]),
        investment_detail=random.choice([
            "The US Department of Energy has allocated $2 billion for domestic helium production from non-LNG sources",
            "Major mining companies have announced $15 billion in combined capital expenditure for critical minerals projects",
            "Japan's JOGMEC has signed offtake agreements with three new lithium projects in Latin America"
        ]),
        detail_main=random.choice([
            "The transition from combustion engines to electric vehicles is fundamentally reshaping demand patterns for battery metals",
            "Supply concentration in a handful of countries creates geopolitical vulnerability that markets are only beginning to price",
            "The intersection of energy transition demand and geopolitical supply risk is creating a new paradigm for commodity pricing"
        ]),
        additional=random.choice([
            "The FAO Food Price Index declined 0.8% in March, suggesting that overall food inflation remains manageable despite fertilizer concerns.",
            "India's food ministry has assured that domestic wheat stocks remain above buffer norms, with 28 million metric tons in government warehouses.",
            "The Black Sea Grain Initiative continues to facilitate Ukrainian exports, providing a stabilizing influence on global grain markets."
        ])
    )

    # Extract mentioned commodities from headline and body
    mentioned_commodities = [c for c in COMMODITIES if c.lower() in (headline + body).lower()]
    if commodity not in mentioned_commodities:
        mentioned_commodities.append(commodity)

    # Generate a unique article
    article_id = str(uuid.uuid4())
    timestamp = article_date + timedelta(
        hours=random.randint(5, 22),
        minutes=random.randint(0, 59)
    )

    return {
        "article_id": article_id,
        "source": source,
        "headline": headline,
        "body": body,
        "published_at": timestamp,
        "commodities": mentioned_commodities[:5],
        "primary_commodity": commodity,
        "region": region,
        "sector": sector,
        "sentiment_score": round(random.uniform(-0.8, 0.8), 3),
        "relevance_score": round(random.uniform(0.6, 1.0), 3),
        "author": fake.name(),
        "url": f"https://www.{source.lower().replace(' ', '')}.com/commodities/{article_id[:8]}"
    }

# COMMAND ----------

# Generate articles for the past 10 days
articles = []
end_date = datetime(2026, 4, 8)
start_date = end_date - timedelta(days=10)

for day_offset in range(11):
    article_date = start_date + timedelta(days=day_offset)

    # Skip weekends for lower volume
    is_weekend = article_date.weekday() >= 5

    for source in SOURCES:
        # More articles on weekdays
        num_articles = random.randint(1, 2) if is_weekend else random.randint(2, 4)

        for _ in range(num_articles):
            template = random.choice(ARTICLE_TEMPLATES)
            article = generate_article(template, article_date, source)
            articles.append(article)

print(f"Generated {len(articles)} articles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

schema = StructType([
    StructField("article_id", StringType(), False),
    StructField("source", StringType(), False),
    StructField("headline", StringType(), False),
    StructField("body", StringType(), False),
    StructField("published_at", TimestampType(), False),
    StructField("commodities", ArrayType(StringType()), True),
    StructField("primary_commodity", StringType(), True),
    StructField("region", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("sentiment_score", DoubleType(), True),
    StructField("relevance_score", DoubleType(), True),
    StructField("author", StringType(), True),
    StructField("url", StringType(), True)
])

df = spark.createDataFrame(articles, schema=schema)
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.news_articles")

print(f"Wrote {df.count()} articles to {CATALOG}.{SCHEMA}.news_articles")
df.groupBy("source").count().orderBy("source").show()
df.groupBy("sector").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Sample Data

# COMMAND ----------

display(spark.sql(f"""
    SELECT source, headline, primary_commodity, region, published_at
    FROM {CATALOG}.{SCHEMA}.news_articles
    ORDER BY published_at DESC
    LIMIT 20
"""))
