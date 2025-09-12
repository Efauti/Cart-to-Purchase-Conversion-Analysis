# Cart-to-Purchase Conversion Analysis

## Project Overview

This project demonstrates the design and implementation of a full ETL (Extract, Transform, Load) pipeline to analyze customer behavior in the cart-to-purchase conversion process. The dataset spans October 2019 to February 2020 and contains raw e-commerce event logs (product views, cart additions, purchases, and removals).

My goal was to build a robust workflow that prepares the data for reliable analysis and visualization. While Tableau integration is still pending, the ETL process itself is complete and structured to support any BI tool.

---

## ETL Process

### 1. Extract

* Collected raw monthly CSV files (October 2019 – February 2020).
* Imported them into **staging tables** (`temp_oct_2019`, `temp_nov_2019`, … `temp_feb_2020`) using `LOAD DATA INFILE`.
* Used flexible `VARCHAR(255)` fields during loading to avoid data truncation before cleaning.

### 2. Transform

I performed multiple transformations to ensure data quality, usability, and integrity:

* **Cleaning and Standardization**

  * Dropped sparse and uninformative columns (`category_code`).
  * Normalized event values: `remove_from_cart → removed`, `purchase → purchased`, etc.
  * Replaced nulls with meaningful placeholders (`No Brand`, `unknown`, `0.00`).

* **Data Integrity**

  * Designed a unique constraint in `backup_events_history` to prevent duplicates.
  * Split out **product details** from event logs into a dedicated `products` table.

* **Anomaly Detection**

  * `anomalies_price`: separated unrealistic price points.
  * `session_anomalies`: tracked duplicate session IDs.
  * `anomalies_session_length`: separated unusually long sessions.

* **Feature Engineering**

  * `session_history`: included session duration, weekday, month, year, and week number for period and season analysis.
  * `session_summary`: aggregated funnel activity (`viewed`, `carted`, `purchased`, `removed`).
  * `product_price_variations`: calculated the percentage difference between the highest and lowest observed prices for the same product.

### 3. Load

I structured the cleaned data into an **analysis-ready schema**:

* **Fact Tables**

  * `events_history`: all event-level interactions.
  * `session_summary`: aggregated session outcomes.

* **Dimension Tables**

  * `products`: product-level reference details.
  * `session_history`: enriched session metadata.

* **Reference/Backup**

  * `backup_events_history`: raw, deduplicated event storage.

This schema is intentionally star-like, optimized for downstream BI or statistical analysis.

---

## Current State

* The ETL pipeline is complete and well-documented.
* Power BI dashboards are not yet integrated, but cleaned outputs (`events_history`, `session_summary`, `product_price_variations`) are structured and exportable for visualization in any BI tool.
* As a next step, I plan to build dashboards to track conversion funnel drop-offs, session anomalies, and price variation effects on purchasing behavior.

---

## Key Learnings

* Hands-on practice with **data staging, cleaning, and normalization** in SQL.
* Application of **anomaly detection** logic to improve data reliability.
* Experience in **schema design** for analytics (fact vs. dimension tables).
* Understanding how to prepare a dataset for **cart-to-purchase funnel analysis** in a scalable way.
