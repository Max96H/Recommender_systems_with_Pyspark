# Databricks notebook source
# MAGIC %md
# MAGIC # Optional Setup: Direct Download of MovieLens Data in Databricks

import os
import zipfile
import requests

# Define target paths in Databricks DBFS or Unity Catalog Volumes
# (Using FileStore for standard free tier access)
TARGET_DIR = "/FileStore/tables/streaming/"
dbutils.fs.mkdirs(TARGET_DIR)

# Note: Using MovieLens Latest Small dataset as an example direct download URL,
# or you can use your specific GroupLens link URL.
MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
zip_path = "/tmp/movielens.zip"
extracted_dir = "/tmp/movielens_extracted"

print(f"Downloading MovieLens dataset from {MOVIELENS_URL}...")
response = requests.get(MOVIELENS_URL)
with open(zip_path, "wb") as f:
    f.write(response.content)
print("Download complete.")

print("Extracting files...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall("/tmp/movielens_extracted")
print("Extraction complete.")

# Move extracted CSVs from local driver temp storage to DBFS/FileStore table path
# (MovieLens small extracts into a folder named 'ml-latest-small/')
source_folder = os.path.join(extracted_dir, "ml-latest-small")
for file_name in os.listdir(source_folder):
    if file_name.endswith(".csv"):
        local_file = os.path.join(source_folder, file_name)
        dbutils.fs.cp(f"file:{local_file}", f"{TARGET_DIR}{file_name}")
        print(f"Copied {file_name} to {TARGET_DIR}")

# --- CLEANUP STEP --- Not necessary since temp but cleaner
print("Cleaning up temporary files...")
if os.path.exists(zip_path):
    os.remove(zip_path)  # Delete the zip file

if os.path.exists(extracted_dir):
    shutil.rmtree(extracted_dir)  # Delete the extracted folder tree

print("All raw CSV files are now successfully landed in Databricks storage!")