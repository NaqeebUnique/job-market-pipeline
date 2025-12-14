import requests
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os

# --- CONFIG ---
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")
NEON_CONNECTION_STRING = os.environ.get("NEON_DB_URL")

if not NEON_CONNECTION_STRING:
    raise ValueError("NEON_DB_URL is missing! Check your environment variables.")

# --- 1. FETCH JOBS ---
def fetch_jobs():
    url = "https://api.adzuna.com/v1/api/jobs/in/search/1"
    params = {
        "app_id": ADZUNA_APP_ID, 
        "app_key": ADZUNA_APP_KEY, 
        "what": "data analyst", 
        "results_per_page": 50,
        "content-type": "application/json"
    }
    print("Fetching jobs...")
    response = requests.get(url, params=params)
    data = response.json()
    return data.get('results', [])

# --- 2. CLEAN DATA ---
def process_data(jobs_list):
    if not jobs_list: return pd.DataFrame()
    jobs = []
    for item in jobs_list:
        jobs.append({
            "id": str(item.get("id")),
            "title": item.get("title"),
            "company": item.get("company", {}).get("display_name"),
            "location": item.get("location", {}).get("display_name"),
            "description": item.get("description"),
            "salary_min": item.get("salary_min"),
            "salary_max": item.get("salary_max"),
            "created_at": item.get("created"),
            "source_url": item.get("redirect_url")
        })
    df = pd.DataFrame(jobs)
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
    df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df

# --- 3. SAVE TO NEON ---
def save_to_neon(df):
    if df.empty: return

    print("Connecting to Neon...")
    # Add sslmode=require if not in string
    engine = create_engine(NEON_CONNECTION_STRING)
    
    with engine.connect() as conn:
        print("Creating table if not exists...")
        # Create table logic
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS job_postings (
                id TEXT PRIMARY KEY,
                title TEXT,
                company TEXT,
                location TEXT,
                description TEXT,
                salary_min NUMERIC,
                salary_max NUMERIC,
                created_at TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                source_url TEXT
            );
        """))
        
        # Temp table upload
        df.to_sql('job_postings_temp', engine, if_exists='replace', index=False)
        
        # Upsert logic
        print("Merging data...")
        conn.execute(text("""
            INSERT INTO job_postings (id, title, company, location, description, salary_min, salary_max, created_at, source_url)
            SELECT id, title, company, location, description, salary_min, salary_max, created_at, source_url
            FROM job_postings_temp
            ON CONFLICT (id) DO NOTHING;
        """))
        conn.commit()
        print("Success! Jobs saved to Neon.")

if __name__ == "__main__":
    raw = fetch_jobs()
    clean = process_data(raw)
    save_to_neon(clean)
