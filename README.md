# Recommender_systems_with_Pyspark


### Structure  

```
streaming-recommendation-lakehouse/
├── README.md                          # Comprehensive project documentation
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Landing zone & raw Delta tables
│   ├── 02_silver_cleaning.py          # Type casting, parsing & enrichment join
│   ├── 03_gold_features.py            # Aggregations & ML feature store views
│   └── 04_als_model_training.py       # Spark MLlib ALS recommendation engine
├── src/
│   ├── scraper.py                     # BeautifulSoup web scraping utility
│   └── utils.py                       # Helper functions (logging, paths)
└── databricks/
    └── workflow_job.json              # Optional: Databricks workflow orchestration config
```