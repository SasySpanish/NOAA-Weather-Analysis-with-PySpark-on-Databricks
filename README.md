## Global Weather Analysis Project

This project explores global weather patterns using NOAA's Global Surface Summary of the Day (GSOD) dataset, processed entirely on **Databricks Community Edition** (free tier). It demonstrates a complete ETL and analytical workflow for handling historical weather data (2000–2024), focusing on temperature trends, precipitation extremes, and variability across continents, countries, and cities.

### Data Ingestion from NOAA on Amazon S3

The journey starts with ingesting raw daily weather summaries directly from NOAA's public S3 bucket (`s3://noaa-gsod-pds/`).  
To stay within the free cluster's memory and compute limits, we avoided downloading the full archive and instead targeted a curated selection of **77 weather stations** worldwide.  
Data was read using PySpark, parsed from gzipped fixed-width files, and immediately cleaned (missing values → null, Fahrenheit → Celsius conversion, basic quality filtering).

### Structured Division: 6 Continents → 5 Countries → 3 Cities

The dataset was intentionally divided into **six continents**:

- Europe
- Asia
- Africa
- North America
- South America
- Oceania

For each continent, **five representative countries** were selected, and within each country **three major cities** (stations) were chosen — resulting in the final set of 77 stations.  
This hierarchical structure enabled modular processing (one notebook per continent) and ensured balanced geographic coverage without overwhelming resources.

Examples:

- Europe → Italy (Bolzano, Rome Ciampino, Palermo), France (Paris CDG, Marseille, Brest), Germany, Spain, UK…
- Asia → China (Beijing, Shanghai, Guangzhou), India (Delhi, Mumbai, Chennai), Japan, Russia, Indonesia…
- Africa → South Africa (Cape Town, Johannesburg, Durban), Nigeria…

### Storage in Delta Lake

After initial cleaning and enrichment (adding continent, country, hemisphere, season), data was saved in **Delta Lake** format — first per continent, then unioned into a single global table.  
Delta provided schema enforcement, ACID transactions, and efficient querying even on limited hardware, making iterative analysis fast and reliable.

### Feature Engineering & Statistical Calculations

Using PySpark aggregations, we computed a rich set of features by year, continent, country, and city:

- Average / max / min temperature
- Days >35°C, days <0°C (frost)
- Precipitation days (>10 mm, >50 mm)
- Diurnal temperature range (Tmax – Tmin)
- Temperature variability (standard deviation)
- Humidity %, wind speed averages
- Extreme events (lightning, snow, hail from FRSHTT flags)
- Anomalies vs 2000–2009 baseline per station

These metrics form the basis for all downstream insights.

### Visualizations with Seaborn & Matplotlib

Final analysis and plots were created by converting Spark results to Pandas DataFrames and using **Seaborn** + **Matplotlib** for publication-quality charts.  
Every plot avaiable [Here](results/img)
Key visualizations include: 

- **Top 6 Continents by Average Temperature in 2024** 
  ![Top 6 Continenti per Temperatura Media 2024](C.%20temp%20media%20x%20continente%202024.png)
  
- **Heatmap of average days with >10 mm rain** per continent and year  
  ![Heatmap: Giorni medi >10mm per Continente e Anno](results/img/C.%20giorni%20di%20pioggia%2010mm%20continente%20x%20anno.png)-

- **Trend of annual mean temperature per continent** (2014–2024)  
  ![Trend Temperatura Media Annuale per Continente 2014–2024](results/img/C.%20trend%20temperatura%20annuale%20continente%2014-24.png)

- **Temperature Variability (Standard Deviation) per Continent and Year (2014–2024)**  
![Heatmap Variabilità Temperatura per Continente e Anno](C.%20heatmap%20variabilità%20temperatura%20continente%2014-24.png)

- **Average Diurnal Temperature Range (Tmax – Tmin) per Country – Last 5 Years**  
  ![Escursione Termica Media per Paese – Ultimi 5 Anni](P.%20escursione%20termica%20per%20paese%20last5y.png)

The visualizations reveal strong tropical influence on highest temperatures, elevated heavy-rain days in Europe and parts of South America/Asia, greater variability in Asia and North America, and a general pattern of stable-to-slightly-warming continental averages over the decade.

All code runs end-to-end on free Databricks — from S3 ingestion to interactive Seaborn charts.

## Author
Developed by **[Salvatore Spagnuolo](https://github.com/SasySpanish)**  

---
