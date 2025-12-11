# SQLAlchemy DB URL. For PostgreSQL:
# postgresql+psycopg2://USER:PASSWORD@HOST:PORT/DBNAME
DB_URL = "postgresql+psycopg2://vishal@localhost:5432/traffic_db"

# Paths to your CSVs (downloaded manually from the portals)
ACCIDENTS_CSV_PATH = "data/raw/nyc_crashes.csv"
WEATHER_CSV_PATH   = "data/raw/nyc_weather_daily.csv"

# ML settings
RANDOM_STATE = 42
MIN_ROWS_FOR_TRAINING = 5000 