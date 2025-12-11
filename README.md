# Traffic Accident Hotspot Detection and Severity Prediction Using NYC Crash and Weather Data

## Overview

This project builds an end-to-end data pipeline and modeling workflow for
New York City traffic crashes. It:

1. Loads raw NYC crash data and NYC daily weather data into a PostgreSQL database.
2. Cleans the data and defines a binary **severity** label for each crash.
3. Links each crash to the weather on the day it occurred.
4. Runs SQL queries to identify spatial and temporal **hotspots** of severe crashes.
5. Trains machine learning models to predict whether a crash will be **serious** or **minor**.

The project was developed by **Vishal Nagamalla** and **Pranav Patil** for a course project in database management and data mining.

## Setup

### 1. Python environment

```bash
conda create -n traffic_env python=3.11
conda activate traffic_env
pip install -r requirements.txt
```

### 2. PostgreSQL

Install PostgreSQL (example for macOS with Homebrew):

```bash
brew install postgresql@14
brew services start postgresql@14
```

Create the database:

```bash
psql postgres
CREATE DATABASE traffic_db;
\q
```

### 3. Configure the connection

Edit `config.py` and set `DB_URL` using your local username:

```python
DB_URL = "postgresql+psycopg2://<your-username>@localhost:5432/traffic_db"

ACCIDENTS_CSV_PATH = "data/raw/nyc_crashes.csv"
WEATHER_CSV_PATH   = "data/raw/nyc_weather_daily.csv"

RANDOM_STATE = 42
MIN_ROWS_FOR_TRAINING = 5000
```

Test the connection:

```bash
python - << 'EOF'
from sqlalchemy import create_engine, text
from config import DB_URL
engine = create_engine(DB_URL)
with engine.connect() as conn:
    print(conn.execute(text("SELECT 1")).scalar_one())
EOF
```

You should see `1`.

---

## Loading the Data

### 1. Create the tables and indexes

```bash
psql traffic_db -f schema.sql
```

This creates:

- `weather` table (daily weather)
- `accidents` table (one row per crash) with a foreign key `weather_id`
- Indexes on crash datetime, severity, location, and borough/date

### 2. Run the ETL pipeline

```bash
python etl.py
```

This will:

- Load the daily weather CSV into the `weather` table.
- Load the crash CSV into the `accidents` table.
- Compute a binary **severity** label for each crash  
  (serious if at least one death **or** at least three injuries).
- Join each crash to the weather for its crash date and set `weather_id`.

You can verify counts in `psql`:

```sql
SELECT COUNT(*) FROM weather;
SELECT COUNT(*) FROM accidents;
SELECT COUNT(*) FROM accidents WHERE weather_id IS NOT NULL;
```

---

## Hotspot Analysis Queries

To run the analysis queries:

```bash
psql traffic_db -f sql_queries.sql
```

The queries include examples such as:

- Top borough–hour combinations by number of serious crashes.
- Severity distribution by borough.
- Zip-code-level hotspots ranked by number and percentage of severe crashes.

These outputs feed the discussion of spatial and temporal crash patterns in the report.

---

## Machine Learning Pipeline

To train and evaluate the models:

```bash
python ml_pipeline.py
```

`ml_pipeline.py` performs the following steps:

1. Pulls the joined accident–weather data from PostgreSQL.
2. Derives time features (hour of day, day of week, month, weekend flag).
3. Builds feature matrices using:
   - Location and time features (latitude, longitude, hour, weekday, etc.)
   - Weather features (precipitation, max and min temperature, description)
4. Splits the data chronologically so that earlier crashes are used for training and later crashes for testing.
5. Trains two binary classifiers:
   - **Logistic Regression**
   - **Random Forest**
6. Prints scikit-learn classification reports and confusion matrices for each model.

## How to Reproduce

1. Create the `traffic_db` PostgreSQL database and update `config.py` with your username.
2. Run:

   ```bash
   psql traffic_db -f schema.sql
   python etl.py
   ```

3. Run hotspot queries:

   ```bash
   psql traffic_db -f sql_queries.sql
   ```

4. Run the ML pipeline:

   ```bash
   python ml_pipeline.py
   ```