## Decisions

This document explains why certain choices were made when designing the project.
The goal is to make the reasoning clear, not to justify every single detail.

It focuses on decisions that meaningfully affect the scope, complexity, and usefulness of the pipeline.

---

### 1. Why HS 8703 was chosen

HS/CN 8703 covers passenger cars for fewer than 10 people, which aligns well with the questions this project is trying to answer.

The focus here is on cars as a consumer good that affects the general population, not on industrial vehicles or commercial transport.  
Limiting the project to a single, well-defined product keeps the scope manageable while still being economically meaningful.

---

### 2. Why a monthly grain

A monthly grain offers a good balance between detail and simplicity.

It allows trends to be observed over time and makes it possible to relate changes in car imports to broader economic, political, or regulatory events, without generating unnecessary data volume.  
Yearly data would be too coarse, and finer granularity is not available from the source.

---

### 3. Why trade data instead of production data

This project is centered on trade dependency, not on measuring domestic production.

Trade data directly shows how much Europe relies on external suppliers and how car flows move across borders.  
While production data could complement this analysis, it is less consistently available and would add complexity without directly serving the main questions.

---

### 4. Why EV share is measured within imports

Measuring the share of electric vehicles within imports highlights technological dependence.

This choice is motivated by the growing presence of Chinese electric vehicles in European markets, which has become an important economic and policy topic.  
Looking specifically at imports keeps the analysis aligned with the project’s trade-focused perspective.

---

### 5. Why batch ingestion

Eurostat Comext data is updated monthly, so a batch ingestion naturally fits the source.

Using batch processing keeps the pipeline simple, predictable, and easy to maintain, without introducing real-time complexity that would not add value in this context.

---

### 6. Why PostgreSQL

PostgreSQL was chosen because it is widely used on the industry. 

---

### 7. Why dashboards were excluded

Dashboards are intentionally out of scope.

The goal of this project is to demonstrate data engineering fundamentals—data ingestion, transformation, modeling, and querying—not visualization or BI delivery.  
Insights are surfaced through SQL queries rather than dashboards to keep the focus on the pipeline itself.