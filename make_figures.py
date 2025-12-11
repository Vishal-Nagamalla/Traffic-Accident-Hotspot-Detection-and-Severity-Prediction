import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL

import matplotlib.pyplot as plt

def get_engine():
    return create_engine(DB_URL)

def load_accidents_for_plots(engine):
    query = """
        SELECT crash_datetime, borough, severity
        FROM accidents
        WHERE crash_datetime IS NOT NULL
          AND borough IS NOT NULL
    """
    df = pd.read_sql(query, engine, parse_dates=["crash_datetime"])
    return df

def plot_hourly_accidents(df):
    df["hour"] = df["crash_datetime"].dt.hour
    hourly = df.groupby("hour").size().reset_index(name="count")

    plt.figure()
    plt.bar(hourly["hour"], hourly["count"])
    plt.xlabel("Hour of day")
    plt.ylabel("Number of accidents")
    plt.title("Accidents by hour of day")
    plt.xticks(range(0, 24))
    plt.tight_layout()
    plt.savefig("figures/hourly_accidents.png", dpi=300)
    plt.close()

def plot_borough_severity(df):
    borough_stats = (
        df.groupby("borough")["severity"]
        .agg(total="count", serious="sum")
        .reset_index()
    )
    borough_stats["serious_rate"] = borough_stats["serious"] / borough_stats["total"]

    plt.figure()
    plt.bar(borough_stats["borough"], borough_stats["serious_rate"])
    plt.xlabel("Borough")
    plt.ylabel("Serious crash rate")
    plt.title("Serious crash rate by borough")
    plt.tight_layout()
    plt.savefig("figures/borough_severity.png", dpi=300)
    plt.close()

def plot_model_performance():
    models = ["Logistic Regression", "Random Forest"]
    precision = [0.035, 0.000]
    recall = [0.654, 0.000]

    x = range(len(models))

    plt.figure()
    width = 0.35
    plt.bar([i - width/2 for i in x], precision, width, label="Precision")
    plt.bar([i + width/2 for i in x], recall, width, label="Recall")

    plt.xticks(list(x), models, rotation=15)
    plt.ylabel("Score")
    plt.ylim(0, 1.0)
    plt.title("Class 1 (Serious) Precision/Recall by Model")
    plt.legend()
    plt.tight_layout()
    plt.savefig("figures/model_performance.png", dpi=300)
    plt.close()

def main():
    engine = get_engine()
    df = load_accidents_for_plots(engine)
    plot_hourly_accidents(df)
    plot_borough_severity(df)
    plot_model_performance()

if __name__ == "__main__":
    main()