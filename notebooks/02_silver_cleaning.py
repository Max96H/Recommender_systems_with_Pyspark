# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 2: Silver Cleaning & Relational Engineering Layer
# MAGIC - Type casting and schema enforcement
# MAGIC - Cleaning orphan records (dirty data)
# MAGIC - Parsing genres into ArrayType
# MAGIC - Left joining MovieLens data with Scraped Metadata

# COMMAND: Setup and Database Initialization
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, lit

spark = SparkSession.builder.appName("SilverCleaning").getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS streaming_silver")

# COMMAND: 1. Clean and Transform Movies
# Read from Bronze
bronze_movies = spark.table("streaming_bronze.bronze_movies")

# Transformations:
# - Ensure proper data types
# - Parse genres string "Action|Adventure" into a Spark ArrayType
silver_movies = bronze_movies \
    .select(
        col("movieId").cast("integer"),
        trim(col("title")).alias("title"),
        split(col("genres"), "\|").alias("genres_array")
    ) \
    .dropna(subset=["movieId", "title"])

# Write to Silver Delta table (SCD Type 1 overwrite/update)
silver_movies.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_silver.silver_movies")

print(f"Silver movies processed. Count: {silver_movies.count()}")

# COMMAND: 2. Clean and Validate Ratings
bronze_ratings = spark.table("streaming_bronze.bronze_ratings")
bronze_links = spark.table("streaming_bronze.bronze_links")

# Type casting ratings and timestamps
typed_ratings = bronze_ratings \
    .select(
        col("userId").cast("integer"),
        col("movieId").cast("integer"),
        col("rating").cast("float"),
        col("timestamp").cast("long")
    ) \
    .dropna()

# Data Quality Check: Handle "dirty" data by ensuring ratings only exist for valid movies in our catalog
valid_movies = silver_movies.select("movieId")
silver_ratings = typed_ratings.join(valid_movies, on="movieId", how="inner")

silver_ratings.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_silver.silver_ratings")

print(f"Silver ratings processed and cleaned. Count: {silver_ratings.count()}")

# COMMAND: 3. Clean Scraped Metadata & Join with Links
bronze_scraped = spark.table("streaming_bronze.bronze_scraped_metadata")
bronze_links = spark.table("streaming_bronze.bronze_links")

# Clean scraped metadata types
silver_scraped = bronze_scraped \
    .select(
        col("movieId").cast("integer"),
        trim(col("director")).alias("director"),
        trim(col("budget")).alias("budget"),
        trim(col("poster_url")).alias("poster_url")
    )

# Left join Movie Master with Scraped Metadata to create an enriched movie table
movie_master = silver_movies.join(silver_scraped, on="movieId", how="left")

# Fill missing scraped values gracefully
movie_master_filled = movie_master \
    .fillna({"director": "Unknown", "budget": "Unknown", "poster_url": "Unknown"})

movie_master_filled.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_silver.silver_movies_enriched")

print("Silver enriched movie master table created successfully.")