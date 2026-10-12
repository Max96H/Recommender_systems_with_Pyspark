# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 4: Machine Learning & Recommendation Engine (ALS)
# MAGIC - Train a Spark MLlib ALS collaborative filtering model
# MAGIC - Handle model sparsity and performance optimization via caching
# MAGIC - Generate Top-10 recommendations per user

# COMMAND: Setup and Data Loading
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("ALSModelTraining").getOrCreate()

# Read from Gold Fact Table
fact_ratings = spark.table("streaming_gold.fact_ratings")

# Split data into training (80%) and testing (20%) sets
(training_data, test_data) = fact_ratings.randomSplit([0.8, 0.2], seed=42)

# Optimization: Cache the training set to speed up iterative ALS matrix factorization calculations
training_data.cache()

print(f"Training records: {training_data.count()}, Test records: {test_data.count()}")

# COMMAND: Configure and Train the ALS Model
als = ALS(
    maxIter=10,
    regParam=0.1,
    userCol="userId",
    itemCol="movieId",
    ratingCol="rating",
    coldStartStrategy="drop",  # Handles users/items with no previous history by dropping NaNs during evaluation
    nonnegative=True
)

print("Training ALS model...")
model = als.fit(training_data)
print("Model training complete.")

# COMMAND: Evaluate Model Performance (RMSE)
predictions = model.transform(test_data)

evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

# COMMAND: Generate Top-10 Recommendations for All Users
# Spark MLlib can calculate recommendations for all users natively in parallel
user_recs = model.recommendForAllUsers(10)

# Save recommendations to a Gold Serving Delta table
user_recs.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("streaming_gold.gold_user_recommendations")

print("Top-10 user recommendations generated and saved to streaming_gold.gold_user_recommendations.")

# Unpersist cache to free memory
training_data.unpersist()