import json
import os
import time
from bs4 import BeautifulSoup
import pandas as pd
import requests

def scrape_movie_metadata(links_csv_path: str, output_json_path: str, limit: int = 500):
    """
    Reads MovieLens links.csv and scrapes director, budget, and poster_url 
    for the specified limit of movies using BeautifulSoup.
    """
    print(f"Loading links from {links_csv_path}...")
    links_df = pd.read_csv(links_csv_path).head(limit)
    
    scraped_data = []
    
    # Headers to mimic a real browser and avoid basic bot blocks
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print(f"Starting scraping process for {limit} movies...")
    for index, row in links_df.iterrows():
        movie_id = row['movieId']
        imdb_id = str(row['imdbId']).zfill(7)
        imdb_url = f"https://www.imdb.com/title/tt{imdb_id}/"
        
        metadata = {
            "movieId": int(movie_id),
            "imdbId": imdb_id,
            "director": "Unknown",
            "budget": "Unknown",
            "poster_url": "Unknown"
        }
        
        try:
            response = requests.get(imdb_url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Example scraping logic for IMDb structure
                # Note: IMDb structure updates frequently; adjust selectors as needed
                director_tag = soup.find("a", {"class": "ipc-metadata-list-item__list-content-item--link"})
                if director_tag:
                    metadata["director"] = director_tag.text.strip()
                
                # Sleep briefly to be respectful to the server
                time.sleep(0.5)
        except Exception as e:
            print(f"Error scraping movieId {movie_id}: {e}")
            
        scraped_data.append(metadata)
        
        if (index + 1) % 50 == 0:
            print(f"Progress: {index + 1}/{limit} movies scraped.")

    # Save to JSON
    os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
        
    print(f"Scraping complete! Saved to {output_json_path}")

if __name__ == "__main__":
    # Example local execution paths
    scrape_movie_metadata(
        links_csv_path="data/raw/links.csv",
        output_json_path="data/raw/scraped_metadata.json",
        limit=500
    )