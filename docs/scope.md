## Key Points

### 1. What the pipeline is built for

This project implements a **batch ETL pipeline** that extracts a subset of trade data from the **Eurostat Comext API** and prepares it for structured analysis.

The pipeline ingests raw trade data, applies cleaning and transformation steps, and loads the processed results into a **PostgreSQL database**, where the data can be queried to answer clearly defined business questions.

High-level flow:

FETCH (Eurostat API)  
→ TRANSFORM (cleaning, filtering, shaping in Python)  
→ STORE (processed data as Parquet)  
→ LOAD (PostgreSQL)  
→ QUERY (analytical insights via SQL)

The focus of the project is **data ingestion, modeling, and reliability**, not visualization.

---

### 2. Business questions it answers

- How much does Europe rely on **foreign countries** for passenger cars over time?
- Of those imported cars, **what share are electric vehicles**, by country and year?

---

### 3. Scope

**Dataset**  
- Eurostat Comext  
- DS-059341: *International trade of EU and non-EU countries by HS classification*

**Product**  
- HS/CN 8703  
  *Passenger cars and other motor vehicles principally designed for the transport of fewer than 10 persons*
  This code includes family cars, SUVs, taxis and racing cars.
  It dos not include buses, trucks, special vehicles or vehicle parts.
- “Electric passenger vehicles — not trucks, not buses.”

**Grain**  
One row represents the **total trade value** of passenger cars for:
- a given **month**
- a **reporting country** (EU country)
- a **partner country**
- a **trade flow** (import or export)

**Measures**  
- Trade value (EUR)  
- Optional: quantity (units), where available

**Dimensions / Keys**  
- Time (year, month)  
- Reporter country  (European Union in general, or specific countries)
- Partner country  (China, Japan, USA and Noth Korea)
- Trade flow  (Imports)
- Product classification (EV vs non-EV via CN subcodes)

**Time Range**  
- Monthly data (2018 → latest available)

**Pipeline Stages**  
- Extract data from Eurostat API (monthly batches)
- Clean and transform data in Python
- Persist intermediate results as Parquet
- Load modeled data into PostgreSQL
- Query data using SQL to answer business questions

**Data Modeling Approach**  
- One fact table at the defined grain
- Minimal supporting dimension tables
- Shaping performed primarily in Python, analytical logic in SQL

**Non-Goals**  
- No real-time or streaming processing  
- No Spark or distributed frameworks  
- No Airflow or external schedulers  
- No dashboards or BI tools  
- No support for multiple products or datasets