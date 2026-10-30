# FuelStream-Analytics: Medallion Pipeline with PySpark

## 🎯 Project Goal
To build a professional-grade data pipeline under the Medallion architecture (Bronze -> Silver -> Gold) using **PySpark** and **Databricks**. The business objective is to transform raw consumer transactions from a gas station network (fuel, lubricants, and convenience store) into an optimized analytical model designed to evaluate customer loyalty, detect consumption trends, and segment demographic behavior.

---

## 🏗️ Phase 1: Smart Data Sampling and Preparation (BigQuery)

The original dataset hosted in BigQuery consists of **67 million transaction records (~8 GB)** and a reference table with **2 million customers**. To ensure technical feasibility within the hardware limitations of the Databricks Free Edition, a data reduction strategy was designed without compromising statistical rigor or business realism.

### 📈 Stratified Sampling Strategy
A simple random sampling approach (`RAND()`) would have jeopardized the representativeness of low-volume products (such as lubricants, which account for less than 0.2% of the total volume). 

To keep the original sales distribution and commercial identity intact, a **stratified sampling strategy based on product type (`Prod`)** was implemented. This approach extracts exactly **1.50%** of the data from each category to consolidate a controlled sample of approximately **1,000,000 transactions**.

#### Extraction SQL Code (BigQuery):
```sql
CREATE OR REPLACE TABLE `jtaguico-websites.datasets_test.sh_trx_sampled` AS
WITH stratified_transactions AS (
    SELECT *,
           -- Assigns a unique random number to each row within its specific product group
           ROW_NUMBER() OVER(PARTITION BY Prod ORDER BY RAND()) as random_row,
           -- Counts the total number of records available for that specific product
           COUNT(*) OVER(PARTITION BY Prod) as total_product
    FROM `jtaguico-websites.datasets_test.sh_trx_Unq`
    WHERE Prod IN ('LUBRI REGU','LUBRI PREM','SUPER','NAFTA VPN','DIESEL VPN','PRODUCTO SHOP') 
)
SELECT * EXCEPT(random_row, total_product)
FROM stratified_transactions
-- Filters exactly 1.50% of each product category to reach the global target
WHERE random_row <= (total_product * 0.015)
LIMIT 1010000;
```

### 👥 Customer Integrity Matching Strategy
To maintain strict referential integrity across our data model, downloading a random subset of customers was not an option, as it would introduce orphaned transaction records (missing customer metadata) during the Silver layer `JOIN` phase. 

Instead, a deterministic extraction was applied to the customer reference table (`clients_02`). The query filters and extracts **only the unique customers** who possess at least one corresponding record within our previously created stratified transaction sample (`sh_trx_sampled`). 

#### Customer Extraction SQL Code (BigQuery):
```sql
SELECT c.* 
FROM `jtaguico-websites.datasets_test.clients_02` c
WHERE c.Persona_id IN (
    SELECT DISTINCT Person_id 
    FROM `jtaguico-websites.datasets_test.sh_trx_sampled`
);
```

### 🛡️ Data Quality Audit & Referential Integrity Verification
To validate the initial state of the sampled datasets prior to migration, a structural audit was performed using a `LEFT JOIN` to detect any "orphaned" transactions (purchases tied to a non-existent `Person_id` in the customer master table).

```sql
SELECT COUNT(*) as orphaned_transactions
FROM `jtaguico-websites.datasets_test.sh_trx_sampled` t
LEFT JOIN `jtaguico-websites.datasets_test.clients_02` c 
  ON t.Person_id = c.Persona_id
WHERE c.Persona_id IS NULL;
```

**Audit Result:** The query identified exactly **92 orphaned records** (representing a negligible 0.009% of the sample). In a standard staging setup, these would either be dropped immediately or trigger alerts. 

### 🪵 The Bronze Layer Strategy (Enterprise-Grade ELT)
To accurately mirror a real-world enterprise production lifecycle, **the 92 orphaned records will purposely be left untouched within the Bronze layer**. 

Following best practices in ELT (Extract-Load-Transform) architectures, the Bronze layer must act as a historical append-only ledger that captures data exactly as it arrives from the source, imperfections included. All data cleansing rules, referential integrity enforcements, and structural filters will be explicitly handled programmatically using **PySpark DataFrames** in the subsequent **Silver Layer** notebook.

---

## 📊 Phase 2: Exploratory Data Analysis (EDA) & Profiling

A comprehensive profiling query was executed to extract temporal distributions and volumetric behaviors across the stratified sample:

```sql
SELECT 
    EXTRACT(YEAR FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', Date)) AS anio,
    EXTRACT(MONTH FROM PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3S', Date)) AS mes,
    COUNT(*) AS cantidad_transacciones,
    ROUND(SUM(Qty), 2) AS volumen_total_litros
FROM `jtaguico-websites.datasets_test.sh_trx_sampled`
GROUP BY anio, mes
ORDER BY anio DESC, mes DESC;
```

### 🔍 Key Data Insights & Outlier Identification
The operational footprint of the dataset continuously spans from **July 2016 to September 2019**, maintaining a steady baseline of 25,000 to 32,000 transactions per month. However, two critical anomalies were discovered:

1. **System Initialization (2016):** The year 2016 shows a very low transaction count (under 1,000 records monthly). This perfectly simulates a system rollout phase or the gradual onboarding of initial service stations.
2. **Severe Volumetric Outliers (2017-05 & 2018-04):** During May 2017 and April 2018, transaction counts remained completely normal, yet the total volume of liters (`Qty`) systematically exploded to **over 71 million liters** each month. 

*Business & Technical Diagnosis:* This is a classic upstream corruption issue (e.g., sensor calibration malfunction, decimal shifting during point-of-sale data entry, or bulk testing scripts leaking into production). 

*Resolution Plan:* These two anomalous months will be isolated and cleaned using statistical capping or filtering techniques within the **PySpark Silver Layer** to avoid heavily biasing our final business KPIs in the Gold Layer.


