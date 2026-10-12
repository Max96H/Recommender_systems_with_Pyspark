# Recommender_systems_with_Pyspark

## Production-Ready Lakehouse & Recommendation Engine

![Databricks](https://img.shields.io/badge/Databricks-Free%20Tier-red?logo=databricks)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-MLlib-orange?logo=apache-spark)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-blue?logo=delta)
![Streamlit](https://img.shields.io/badge/Streamlit-App-ff4b4b?logo=streamlit)

An end-to-end Data Engineering pipeline and Machine Learning recommendation system built on **Databricks**, utilizing the **Medallion Architecture (Bronze, Silver, Gold)**, **Apache Spark MLlib (ALS)**, and an interactive **Streamlit** web application.

---

## 🏗️ Architecture & Medallion Design
```
[Local Scraper / VS Code]
│ (JSON)
▼
[GitHub Repository] ──(Databricks Git Sync)──► [Databricks Workspace]
│
┌────────────────────────────────────────────┘
▼
[Bronze Layer] (Raw ingestion of CSVs & Scraped JSON)
│
▼
[Silver Layer] (Schema enforcement, casting, array parsing, left joins)
│
▼
[Gold Layer]   (Aggregated user metrics, partitioned fact tables, ML views)
│
▼
[Serving]      (Spark MLlib ALS Recommendation Engine + Streamlit App)
```
---

## 📦 Repository Structure

```text
streaming-recommendation-lakehouse/
├── README.md                          # Project documentation
├── requirements.txt                   # Python dependencies for local execution & Streamlit
├── app.py                             # Interactive Streamlit recommendation portal
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Raw landing zone into immutable Delta tables
│   ├── 02_silver_cleaning.py          # Data quality checks, casting, and metadata join
│   ├── 03_gold_features.py            # Feature engineering & partitioned fact tables
│   └── 04_als_model_training.py       # Spark MLlib ALS collaborative filtering model
├── src/
│   ├── scraper.py                     # BeautifulSoup web scraping utility for movie metadata
│   └── utils.py                       # Modular Spark session and path configurations
└── databricks/
    └── workflow_job.json              # Infrastructure-as-Code job configuration
```

## ⚠️ Data Sources

This project integrates internal structured data with external semi-structured web data:

- MovieLens Ratings (ratings.csv): High-volume user interaction data.

- MovieLens Movies (movies.csv): Static metadata requiring string parsing for genres.

- MovieLens Links (links.csv): Cross-referencing IDs (IMDb/TMDB) used for external enrichment.

- Scraped Metadata (scraped_metadata.json): Web-scraped director names, budgets, and poster URLs for top movies.

## 🔄 Detailed Phase Explanations
### Phase 1: Ingestion & Scraping (Bronze Layer)

- Web Scraping: Due to outbound network restrictions on the Databricks Free Tier, src/scraper.py is executed locally via VS Code using BeautifulSoup to fetch IMDb details, generating scraped_metadata.json.

- Landing: Raw CSV files and the JSON file are loaded into immutable Bronze Delta tables in Databricks with absolute fidelity to their original schemas.

### Phase 2: Relational Engineering (Silver Layer)

- Data Quality & Schema Enforcement: Dropped orphan records (ratings for non-existent movies/users) and enforced strict data types.

- Transformation: Split the pipe-delimited genres string into a Spark ArrayType for efficient filtering.

- Enrichment Join: Performed a Left Join between the MovieLens movie catalog and the scraped metadata to create silver_movies_enriched.

### Phase 3: Feature Engineering (Gold Layer)

- Power Users (dim_users): Aggregated user activity to categorize users into tiers (Power User, Regular User, Casual User).

- Optimized Fact Tables (fact_ratings): Created a production-ready fact table partitioned by rating to optimize downstream analytical queries.

### Phase 4: Machine Learning & Serving

- ALS Model: Trained a collaborative filtering model using pyspark.ml.recommendation.ALS with coldStartStrategy="drop" to handle user sparsity.

- Serving: Generated top-10 personalized movie recommendations for every user, stored in streaming_gold.gold_user_recommendations.

## 🚀 Installation & Setup
### 1. Local Development & Scraping

Clone the repository and install local dependencies:
```
git clone [https://github.com/your-username/streaming-recommendation-lakehouse.git](https://github.com/your-username/streaming-recommendation-lakehouse.git)
cd streaming-recommendation-lakehouse
pip install -r requirements.txt
```
Run the scraper locally to generate the enrichment data:
```
python src/scraper.py
```

### 2. Databricks Workspace Setup

1. Sync your GitHub repository to Databricks using Databricks Repos.

2. Upload your raw data files (movies.csv, ratings.csv, links.csv, and scraped_metadata.json) to your Databricks Volume or DBFS path (/FileStore/tables/streaming/).

## ⚙️ Workflow Automation

You can automate the pipeline execution using either of the two methods below:

### Method A: Using the Databricks UI (Recommended for Beginners)

1. Navigate to Workflows in your Databricks sidebar ➔ Create Job.

2. Add 4 sequential notebook tasks pointing to your synced repo paths:
    - Task 1: 01_bronze_ingestion

    - Task 2: 02_silver_cleaning (Depends on Task 1)

    - Task 3: 03_gold_features (Depends on Task 2)

    - Task 4: 04_als_model_training (Depends on Task 3)

3. Click Run Now to execute the pipeline end-to-end.

### Method B: Using Infrastructure-as-Code (workflow_job.json)

If importing the provided databricks/workflow_job.json configuration file, make sure to replace the value of **"existing_cluster_id"** with your active Databricks cluster ID before importing.

## 🌐 Interactive Web App (Streamlit)

To launch the interactive recommendation portal:

1. Configure your local .env file with your Databricks credentials:
```
Extrait de code

DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
DATABRICKS_TOKEN=dapi_your_personal_access_token
```
2. Run the Streamlit app:
```
Bash

streamlit run app.py
```
3. Enter a User ID in the sidebar to view live personalized movie recommendations!

## 👥 Contributors & Timeline

- Author: Max96H

- Course/Context: Data Engineering Learning Path & Databricks Exploration (2026) (BeCode)

- Timeline: 5/10/2026 -> 9/10/2026


