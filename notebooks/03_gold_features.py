# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 3: Feature Engineering & Gold Layer
# MAGIC - Aggregate user metrics to identify "Power Users" (`dim_users`)
# MAGIC - Create optimized fact tables for ML training (`fact_ratings`)
# MAGIC - Performance optimization via table partitioning

# COMMAND: Setup and Database Initialization
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, when

spark = SparkSession.builder.appName("GoldFeatureEngineering").getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS streaming_gold")

# COMMAND: 1. Build dim_users (Power User Profiles)
silver_ratings = spark.table("streaming_silver.silver_ratings")

dim_users = silver_ratings.groupBy("userId").agg(
    count("movieId").alias("total_ratings_given"),
    avg("rating").alias("avg_rating_given"),
    max("timestamp").alias("last_activity_timestamp")
).withColumn(
    "user_tier",
    when(col("total_ratings_given") >= 50, "Power User")
    .when(col("total_ratings_given") >= 10, "Regular User")
    .otherwise("Casual User")
)

dim_users.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_gold.dim_users")

print(f"Gold dim_users created successfully. Total users: {dim_users.count()}")

# COMMAND: 2. Build fact_ratings with Performance Optimization (Partitioning)
# Partitioning by rating helps isolate slices of data efficiently for specific analytical queries or model feedback loops.
fact_ratings = silver_ratings \
    .select("userId", "movieId", "rating", "timestamp")

fact_ratings.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("rating") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_gold.fact_ratings")

print("Gold fact_ratings created and partitioned by rating successfully.")

# COMMAND: 3. Optimize / Copy dim_movies_enriched for Gold Layer consumption
silver_movies_enriched = spark.table("streaming_silver.silver_movies_enriched")

dim_movies_enriched = silver_movies_enriched \
    .select("movieId", "title", "genres_array", "director", "budget", "poster_url")

dim_movies_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_gold.dim_movies_enriched")

print("Gold dim_movies_enriched created successfully.")