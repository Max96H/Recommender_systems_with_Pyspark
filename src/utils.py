from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """Returns a standardized Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

# Centralized path configurations for your Databricks environment
BASE_DATA_PATH = "/FileStore/tables/streaming/"

PATHS = {
    "movies": f"{BASE_DATA_PATH}movies.csv",
    "ratings": f"{BASE_DATA_PATH}ratings.csv",
    "links": f"{BASE_DATA_PATH}links.csv",
    "scraped": f"{BASE_DATA_PATH}scraped_metadata.json"
}