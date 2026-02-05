import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Kobo credentials
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aHD4pamiAJvmTcRXsHuuT9/export-settings/esaKr7vPneA4yLfFtLqBw3R/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

schema_name = "migration"
table_name = "migration_on_job_market"

# --------------------------------------------------
# Step 1: Fetch data
# --------------------------------------------------
print("Fetching data from KoboToolbox...")
response = requests.get(
    KOBO_CSV_URL,
    auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD)
)

if response.status_code != 200:
    raise Exception(f"Failed to fetch data: {response.status_code}")

csv_data = io.StringIO(response.text)
df = pd.read_csv(csv_data, sep=";", on_bad_lines="skip")

# --------------------------------------------------
# Step 2: Clean & normalize columns (CRITICAL FIX)
# --------------------------------------------------
print("Processing data...")

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace("?", "", regex=False)
    .str.replace(" ", "_")
    .str.replace("&", "and")
    .str.replace("-", "_")
)

# Convert NaN → None (PostgreSQL-safe)
df = df.where(pd.notnull(df), None)

# Convert timestamps
df["start"] = pd.to_datetime(df["start"], errors="coerce")
df["end"] = pd.to_datetime(df["end"], errors="coerce")

# Optional sanity check (remove later)
print(df.head())

# --------------------------------------------------
# Step 3: Upload to PostgreSQL
# --------------------------------------------------
print("Uploading data to PostgreSQL...")

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)

cur = conn.cursor()

cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

cur.execute(f"""
CREATE TABLE {schema_name}.{table_name} (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    gender TEXT,
    education_level TEXT,
    province_of_origin TEXT,
    age_range TEXT,
    rural_to_urban BOOLEAN,
    employment_status TEXT,
    months_to_first_job INT,
    monthly_income_rwf INT,
    job_applications_12m INT,
    job_changes INT
);
""")

insert_query = f"""
INSERT INTO {schema_name}.{table_name} (
    start_time,
    end_time,
    gender,
    education_level,
    province_of_origin,
    age_range,
    rural_to_urban,
    employment_status,
    months_to_first_job,
    monthly_income_rwf,
    job_applications_12m,
    job_changes
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for _, row in df.iterrows():
    cur.execute(insert_query, (
        row["start"],
        row["end"],
        row["gender"],
        row["current_highest_level_of_education"],
        row["province_of_origin"],
        row["what_was_your_age_range_at_the_time_of_migration"],
        str(row.get("did_you_migrate_from_a_rural_area_to_this_urban_area", "")).strip().lower() == "yes",
        row["what_is_your_current_employment_status_in_this_urban_area"],
        row["how_many_months_did_it_take_you_to_get_your_first_job_after_arriving_in_the_city"] or 0,
        row["what_is_your_current_average_monthly_income_(rwf)"] or 0,
        row["in_the_last_12_months,_how_many_job_applications_have_you_submitted_in_this_urban_area"] or 0,
        row["how_many_times_have_you_changed_jobs_since_moving_to_the_city"] or 0
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Data successfully loaded into PostgreSQL (no NULL issues, no KeyErrors)")
