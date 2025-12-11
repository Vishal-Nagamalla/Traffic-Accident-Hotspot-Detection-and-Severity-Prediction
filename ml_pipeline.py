import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier

from config import DB_URL, RANDOM_STATE, MIN_ROWS_FOR_TRAINING


def get_engine():
    return create_engine(DB_URL)


def fetch_ml_dataframe(engine):
    sql = """
    SELECT
        accident_id,
        crash_datetime,
        borough,
        zip_code,
        latitude,
        longitude,
        num_injuries,
        num_deaths,
        severity
    FROM accidents
    WHERE severity IS NOT NULL;
    """
    df = pd.read_sql(sql, engine, parse_dates=["crash_datetime"])
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour"] = df["crash_datetime"].dt.hour
    df["day_of_week"] = df["crash_datetime"].dt.dayofweek  
    df["month"] = df["crash_datetime"].dt.month
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    return df


def build_feature_matrix(df: pd.DataFrame):
    df = add_time_features(df)

    for col in ["borough", "zip_code"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown").astype(str)

    numeric_features = [
        "latitude",
        "longitude",
        "hour",
        "day_of_week",
        "month",
        "is_weekend",
    ]
    for col in numeric_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["crash_datetime", "severity", "latitude", "longitude"])

    X = df[numeric_features + ["borough", "zip_code"]]
    y = df["severity"].astype(int)

    return X, y, numeric_features, ["borough", "zip_code"]


def train_and_evaluate_models(X, y, numeric_features, categorical_features):
    n = len(X)
    if n < MIN_ROWS_FOR_TRAINING:
        print(
            f"Not enough rows ({n}) to train robust models. "
            f"Lower MIN_ROWS_FOR_TRAINING in config.py if needed."
        )
        return

    split_idx = int(0.8 * n)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    numeric_transformer = Pipeline(
        steps=[("scaler", StandardScaler())]
    )
    categorical_transformer = Pipeline(
        steps=[("onehot", OneHotEncoder(handle_unknown="ignore"))]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    log_reg = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=RANDOM_STATE,
                ),
            ),
        ]
    )

    rf = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                RandomForestClassifier(
                    n_estimators=200,
                    random_state=RANDOM_STATE,
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                ),
            ),
        ]
    )

    print("Training Logistic Regression...")
    log_reg.fit(X_train, y_train)
    y_pred_lr = log_reg.predict(X_test)
    print("\n=== Logistic Regression ===")
    print(classification_report(y_test, y_pred_lr, digits=3))
    print("Confusion matrix:\n", confusion_matrix(y_test, y_pred_lr))

    print("\nTraining Random Forest...")
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    print("\n=== Random Forest ===")
    print(classification_report(y_test, y_pred_rf, digits=3))
    print("Confusion matrix:\n", confusion_matrix(y_test, y_pred_rf))


def main():
    engine = get_engine()

    df = fetch_ml_dataframe(engine)
    df = df.sort_values("crash_datetime")

    X, y, numeric_features, categorical_features = build_feature_matrix(df)
    train_and_evaluate_models(X, y, numeric_features, categorical_features)


if __name__ == "__main__":
    main()