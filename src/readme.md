# src/ – Source Code Overview

This folder contains the core PySpark notebooks/scripts used for data ingestion, cleaning, continent-wise processing, and exploratory data analysis (EDA) of the NOAA GSOD weather dataset.

The three main notebooks form a clear pipeline:

1. `0_continent_batch.ipynb`  
   Continent-specific batch extraction & initial cleaning  
2. `1_ingestion_cleaning.ipynb`  
   Union of continent tables + country enrichment + final global Delta save  
3. `2_eda_statistics.ipynb`  
   Statistical aggregations, feature engineering & visualization preparation

## 1. 0_continent_batch.ipynb – Continent-wise Ingestion & Cleaning

Purpose:  
Process one continent at a time (to stay within free Databricks memory limits).  
For each continent:  
- Define 5 countries × 3 cities = 15 stations  
- Generate S3 paths only for those stations (2000–2024)  
- Read gzipped .op files  
- Parse, clean, convert units, add season  
- Save as continent-specific Delta table

Key code snippets
Station definition (example – Africa):
```python
africa_stations = {
    "Africa": {
        "Sudafrica": [
            {"code": "688160", "city": "Cape Town (mediterraneo costiero)"},
            # ... 2 more
        ],
        # ... Nigeria, Egitto, Kenya, Marocco
    }
}
```
Flatten to USAF list + city mapping:
```python
usaf_list = [item["code"] for paese in africa_stations["Africa"].values() for item in paese]
usaf_to_city = {item["code"]: item["city"] for paese in africa_stations["Africa"].values() for item in paese}
```
S3 path generation & reading (simplified):
```python
paths = [f"s3://noaa-gsod-pds/{year}/{code}99999.op.gz" for year in range(2000, 2025) for code in usaf_list]
df_raw = spark.read.text(paths)  # or binaryFile for gz handling
```
Unit conversion check & season addition:
```python
temp_avg = df.agg(F.avg("TEMP")).collect()[0][0]
if temp_avg is not None and temp_avg > 40:
    df = df.withColumn("TEMP_c", (F.col("TEMP") - 32) * 5/9) \
           .withColumn("MAX_c", (F.col("MAX") - 32) * 5/9) \
           .withColumn("MIN_c", (F.col("MIN") - 32) * 5/9)
else:
    df = df.withColumnRenamed("TEMP", "TEMP_c")  # already in °C

df = df.withColumn("season",
    F.when(F.month("DATE").isin([12,1,2]), "Inverno")
     .when(F.month("DATE").isin([3,4,5]), "Primavera")
     .when(F.month("DATE").isin([6,7,8]), "Estate")
     .otherwise("Autunno"))
```
Save per continent:
```python
delta_path = f"/Volumes/workspace/weather/continents/{continent_code}"
df_clean.write.format("delta").mode("overwrite").save(delta_path)
```
## 2. 1_ingestion_cleaning.ipynb – Global Union & Country Enrichment

Purpose:  
Load all 6 continent Delta tables → union them → enrich with country name → save final global Delta table.

Key steps & code

Union loop:
```python
continents = ["EU", "AS", "AF", "OC", "NA", "SA"]
df_all = None
for cont in continents:
    path = f"/Volumes/workspace/weather/continents/{cont.lower()}"
    df_cont = spark.read.format("delta").load(path).withColumn("continent", F.lit(cont))
    df_all = df_cont if df_all is None else df_all.unionByName(df_cont)
```
Country extraction from station name (most robust part):
```python
country_map = {
    "IT": "Italia", "FR": "Francia", "DE": "Germania", "ES": "Spagna", "GB": "Regno Unito",
    "ZA": "Sudafrica", "NG": "Nigeria", "EG": "Egitto", "KE": "Kenya", "MA": "Marocco",
    # ... altri paesi
}

df_with_country = df_all.withColumn(
    "country",
    F.coalesce(
        F.col("NAME").substr(-3, 2).map(country_map),
        F.regexp_extract("NAME", r",\s*([A-Z]{2})$", 1).map(country_map),
        F.lit("Sconosciuto")
    )
)
```
Final save:
```python
global_delta_path = "/Volumes/workspace/weather/data/continents2_06_03/"
df_with_country.write.format("delta").mode("overwrite").save(global_delta_path)
```
## 3. 2_eda_statistics.ipynb – Statistics, Features & Visualization Prep

Purpose:  
Load the global Delta table → compute descriptive statistics, extreme counts, anomalies → prepare Pandas DataFrames for Seaborn plots.

Key computations

Annual stats per city/year:
```python
stats_city_year = df.groupBy("year", "continent", "country", "NAME") \
    .agg(
        F.avg("TEMP_c").alias("avg_temp_c"),
        F.max("MAX_c").alias("max_temp_c"),
        F.min("MIN_c").alias("min_temp_c"),
        F.count(F.when(F.col("PRCP") > 10, True)).alias("giorni_prcp_gt_10mm"),
        # ... altri conteggi estremi
    )
```
Anomalies vs baseline (2000–2009):
```python
baseline = df.filter(F.col("year").between(2000, 2009)) \
    .groupBy("NAME").agg(F.avg("TEMP_c").alias("baseline_avg_temp_c"))

anomalies = df.groupBy("NAME", "year").agg(F.avg("TEMP_c").alias("annual_avg_temp_c")) \
    .join(baseline, "NAME") \
    .withColumn("anomaly_c", F.col("annual_avg_temp_c") - F.col("baseline_avg_temp_c"))
```
Conversion to Pandas for plotting:
```python
pdf = stats_city_year.toPandas()
pdf_anom = anomalies.toPandas()
```
All subsequent visualizations (heatmaps, line plots, bar charts, boxplots) are built using Seaborn on these Pandas DataFrames.

---

Pipeline summary  
0_continent_batch → 1_ingestion_cleaning → 2_eda_statistics  
→ raw S3 → continent Deltas → global enriched Delta → statistical features → Seaborn visuals
