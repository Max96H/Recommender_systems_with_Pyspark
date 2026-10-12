import streamlit as st
import pandas as pd
from databricks import sql
import os

# Page Configuration
st.set_page_config(
    page_title="StreamFlix Recommendation Engine",
    page_icon="🎬",
    layout="wide"
)

# Title and Description
st.title("🎬 StreamFlix Recommendation Portal")
st.markdown("Explore personalized movie recommendations powered by **Databricks**, **Spark MLlib (ALS)**, and **Delta Lake**.")

# Helper function to query Databricks SQL Warehouse
@st.cache_resource
def get_databricks_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST", "your-databricks-workspace.cloud.databricks.com"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/your_warehouse_id"),
        access_token=os.getenv("DATABRICKS_TOKEN", "your_personal_access_token")
    )

# Sidebar for User Input
st.sidebar.header("User Control Panel")
user_id_input = st.sidebar.number_input("Enter User ID:", min_value=1, max_value=610, value=1, step=1)

if st.sidebar.button("Get Recommendations"):
    with st.spinner("Fetching predictions from Databricks Gold Layer..."):
        try:
            # Connect and query the Gold layer recommendation and movie metadata tables
            connection = get_databricks_connection()
            
            # Query to get recommendations for the specific user and join with enriched movie details
            query = f"""
                SELECT 
                    r.userId,
                    explode(r.recommendations) as rec,
                    m.title,
                    m.genres_array,
                    m.director,
                    m.budget,
                    m.poster_url
                FROM streaming_gold.gold_user_recommendations r
                JOIN streaming_gold.dim_movies_enriched m ON explode(r.recommendations).movieId = m.movieId
                WHERE r.userId = {user_id_input}
                LIMIT 10
            """
            
            # Note: Depending on Spark version, explode in SQL might require LATERAL VIEW. 
            # Alternatively, we can query pandas dataframes directly:
            rec_df = pd.read_sql(query, connection)
            connection.close()

            if rec_df.empty:
                st.warning(f"No recommendations found for User ID {user_id_input}. Try another ID!")
            else:
                st.subheader(f"Top 10 Recommendations for User #{user_id_input}")
                
                # Display results in a clean grid layout
                for index, row in rec_df.iterrows():
                    with st.container():
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            st.image(row.get("poster_url", "https://via.placeholder.com/150"), width=100)
                        with col2:
                            st.markdown(f"### {row['title']}")
                            genres = ", ".join(row['genres_array']) if isinstance(row['genres_array'], list) else row['genres_array']
                            st.write(f"**Genres:** {genres}")
                            st.write(f"**Director:** {row['director']} | **Budget:** {row['budget']}")
                            st.write(f"**Predicted Rating Score:** {row['rec'].get('rating', 'N/A'):.2f}")
                        st.divider()

        except Exception as e:
            st.error(f"Connection Error (Check your environment tokens/credentials): {e}")
            st.info("Tip: If running locally without Databricks live connection, you can mock this data frame for your demo presentation!")