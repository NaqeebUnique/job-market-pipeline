import requests
import pandas as pd
from sqlalchemy import create_engine, text
import time
import os

# --- CONFIG ---
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY")
NEON_CONNECTION_STRING = os.environ.get("NEON_DB_URL")

if not NEON_CONNECTION_STRING:
    raise ValueError("NEON_DB_URL is missing! Check your environment variables.")

JOB_TITLES = ["data analyst", "business data analyst", "data engineer", "business intelligence analyst", "power bi developer", 
              "data visualization engineer", "business intelligence developer", "bi Engineer", "bi solutions analyst", "data visualization analyst",
              "data consultant", "analytics consultant"]

# --- 1. FETCH JOBS ---
def fetch_jobs():
    all_jobs = []
    
    for role in JOB_TITLES:
        for page in range(1, 4): 
            print(f"Fetching {role} (Page {page})...")
            url = f"https://api.adzuna.com/v1/api/jobs/in/search/{page}"
        params = {
            "app_id": ADZUNA_APP_ID, 
            "app_key": ADZUNA_APP_KEY, 
            "what": role,
            "results_per_page": 50,
            "content-type": "application/json"
        }
        
        time.sleep(1) 
        try:
            response = requests.get(url, params=params)
            data = response.json()
            results = data.get('results', [])
            
            for job in results:
                job['search_role'] = role 
            
            all_jobs.extend(results)
            print(f"   Found {len(results)} jobs.")
            
        except Exception as e:
            print(f"   Error fetching {role}: {e}")

    print(f"Total jobs fetched: {len(all_jobs)}")
    return all_jobs

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
            "source_url": item.get("redirect_url"),
            "search_term": item.get("search_role") # <--- New Field
        })
    df = pd.DataFrame(jobs)
    # Ensure types
    df['salary_min'] = pd.to_numeric(df['salary_min'], errors='coerce')
    df['salary_max'] = pd.to_numeric(df['salary_max'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'])
    return df

# --- 3. SAVE TO NEON ---
def save_to_neon(df):
    if df.empty: return

    print("Connecting to Neon...")
    engine = create_engine(NEON_CONNECTION_STRING)
    
    with engine.connect() as conn:
        # Update Create Table to include 'search_term'
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
                source_url TEXT,
                search_term TEXT  -- <--- New Column
            );
        """))
        
        # ... (Rest of the Upsert logic needs to include search_term) ...
        df.to_sql('job_postings_temp', engine, if_exists='replace', index=False)
        
        print("Merging data...")
        # Note: If ID exists, we usually do NOTHING. 
        # But if you want to update the 'search_term' for existing jobs, that's complex.
        # For now, let's just insert new ones.
        
        conn.execute(text("""
            INSERT INTO job_postings (id, title, company, location, description, salary_min, salary_max, created_at, source_url, search_term)
            SELECT id, title, company, location, description, salary_min, salary_max, created_at, source_url, search_term
            FROM job_postings_temp
            ON CONFLICT (id) DO NOTHING;
        """))
        conn.commit()
        print("Success! Jobs saved.")

if __name__ == "__main__":
    raw = fetch_jobs()
    clean = process_data(raw)
    save_to_neon(clean)
