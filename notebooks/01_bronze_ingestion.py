# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1: Bronze Ingestion Layer
# MAGIC Landing zone for raw CSVs and scraped JSON into immutable Delta tables.

# COMMAND: Define paths and database
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

# Create Bronze schema/database
spark.sql("CREATE DATABASE IF NOT EXISTS streaming_bronze")

# Define file paths (adjust based on your Databricks Volume / DBFS path)
DATA_PATH = "/FileStore/tables/streaming/"
movies_path = f"{DATA_PATH}movies.csv"
ratings_path = f"{DATA_PATH}ratings.csv"
links_path = f"{DATA_PATH}links.csv"
scraped_json_path = f"{DATA_PATH}scraped_metadata.json"

# COMMAND: Ingest Movies CSV -> Bronze Delta Table
movies_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(movies_path)

movies_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("streaming_bronze.bronze_movies")

print("Bronze movies table created successfully.")

# COMMAND: Ingest Ratings CSV -> Bronze Delta Table
ratings_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(ratings_path)

ratings_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("streaming_bronze.bronze_ratings")

print("Bronze ratings table created successfully.")

# COMMAND: Ingest Links CSV -> Bronze Delta Table
links_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(links_path)

links_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("streaming_bronze.bronze_links")

print("Bronze links table created successfully.")

# COMMAND: Ingest Scraped JSON -> Bronze Delta Table
scraped_df = spark.read \
    .option("multiline", "true") \
    .json(scraped_json_path)

scraped_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("streaming_bronze.bronze_scraped_metadata")

print("Bronze scraped metadata table created successfully.")