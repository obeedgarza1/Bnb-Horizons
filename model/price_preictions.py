import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_absolute_percentage_error,
)
import numpy as np
from sklearn.model_selection import GridSearchCV
from scipy.stats import zscore
from sklearn.model_selection import GridSearchCV


DATABASE_URL = "postgresql://postgres:Jugarfutbol1!@localhost:5432/airbnb_data"

engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)
session = Session()


def all_data():
    query = """
        SELECT * FROM gold.earnings_summary"""

    data = pd.read_sql(query, engine)
    columns = [
        "id",
        "season",
        "city_name",
        "neighbourhood_id",
        "room_type",
        "accommodates",
        "bedrooms",
        "bathrooms",
        "latitude",
        "longitude",
        "host_is_superhost",
        "host_identity_verified",
        "unavailable_days",
        "available_days",
        "price_float",
    ]
    df = data[columns]
    return df


df_predict_price = all_data()


def city_center(df):
    city_centers = {
        "Barcelona": {"latitude": 41.3851, "longitude": 2.1734},
        "Euskadi": {"latitude": 42.9896, "longitude": -2.6189},  # Bilbao as the center
        "Girona": {"latitude": 41.9818, "longitude": 2.8237},
        "Madrid": {"latitude": 40.4168, "longitude": -3.7038},
        "Malaga": {"latitude": 36.7213, "longitude": -4.4213},
        "Mallorca": {"latitude": 39.6953, "longitude": 3.0176},  # Palma as the center
        "Menorca": {"latitude": 39.8895, "longitude": 4.2642},  # Mahón as the center
        "Sevilla": {"latitude": 37.3891, "longitude": -5.9845},
        "Valencia": {"latitude": 39.4699, "longitude": -0.3763},
    }

    df["city_center_lat"] = df["city_name"].map(lambda x: city_centers[x]["latitude"])
    df["city_center_lon"] = df["city_name"].map(lambda x: city_centers[x]["longitude"])

    df["distance_to_center"] = np.sqrt(
        (df["latitude"] - df["city_center_lat"]) ** 2
        + (df["longitude"] - df["city_center_lon"]) ** 2
    )

    return df


df_predict_price = city_center(df_predict_price)

df_predict_price["price_zscore"] = zscore(df_predict_price["price_float"])
df_predict_price["bedrooms_zscore"] = zscore(df_predict_price["bedrooms"])

outliers = df_predict_price[
    (df_predict_price["price_zscore"].abs() > 2.5)
    | (df_predict_price["bedrooms_zscore"].abs() > 2.5)
]
df_predict_price = df_predict_price[~df_predict_price["id"].isin(outliers["id"])]

df_predict_price = df_predict_price.drop(
    columns=[
        "city_center_lat",
        "city_center_lon",
        "id",
        "bedrooms_zscore",
        "price_zscore",
        "available_days",
        "unavailable_days",
    ]
)

mean_price = df_predict_price["price_float"].mean()
print(f"the mean price for all the listings is: {mean_price}")

df_encoded = pd.get_dummies(
    df_predict_price, columns=["city_name", "room_type", "season"], drop_first=True
)

X = df_encoded.drop("price_float", axis=1)
y = df_encoded["price_float"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=17
)

model = xgb.XGBRegressor(random_state=17)
param_grid = {
    "max_depth": [3, 6, 9, 12],
    "learning_rate": [0.1, 0.01, 0.001],
    "n_estimators": [100, 200, 300],
    "subsample": [0.6, 0.8, 1.0],
}

# Grid search to find the best parameters
grid_search = GridSearchCV(
    estimator=model, param_grid=param_grid, cv=3, scoring="neg_mean_squared_error"
)
grid_search.fit(X_train, y_train)
print("Best parameters found: ", grid_search.best_params_)

best_params = {
    "learning_rate": 0.1,
    "max_depth": 12,
    "n_estimators": 300,
    "subsample": 0.8,
}
model = xgb.XGBRegressor(**best_params, random_state=17)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"MAE: {mae:.2f}")
print(f"RMSE: {rmse:.2f}")

import joblib

joblib.dump(model, "price_model_xgb.pkl")


df_predict_rate = all_data()
df_predict_rate = city_center(df_predict_rate)

df_predict_rate["price_zscore"] = zscore(df_predict_rate["price_float"])
df_predict_rate["bedrooms_zscore"] = zscore(df_predict_rate["bedrooms"])
df_predict_rate["occupancy_rate"] = (
    df_predict_rate["unavailable_days"]
    / (df_predict_rate["available_days"] + df_predict_rate["unavailable_days"])
).round(2)


outliers = df_predict_rate[
    (df_predict_rate["price_zscore"].abs() > 2.5)
    | (df_predict_rate["bedrooms_zscore"].abs() > 2.5)
]
df_predict_rate = df_predict_rate[~df_predict_rate["id"].isin(outliers["id"])]
df_predict_rate = df_predict_rate.dropna(subset=["occupancy_rate"])

df_predict_rate = df_predict_rate.drop(
    columns=[
        "city_center_lat",
        "city_center_lon",
        "id",
        "bedrooms_zscore",
        "price_zscore",
        "unavailable_days",
        "available_days",
    ]
)

high_occupancy = df_predict_rate[df_predict_rate["occupancy_rate"] > 0.05]
low_occupancy = df_predict_rate[df_predict_rate["occupancy_rate"] <= 0.05]
scale_pos_weight = len(low_occupancy) / len(high_occupancy)

df_encoded = pd.get_dummies(
    df_predict_rate, columns=["city_name", "room_type", "season"], drop_first=True
)

X = df_encoded.drop("occupancy_rate", axis=1)
y = df_encoded["occupancy_rate"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=17
)

model = xgb.XGBRegressor(random_state=17)
param_grid = {
    "max_depth": [3, 6, 9],
    "learning_rate": [0.1, 0.01],
    "n_estimators": [100, 200],
    "subsample": [0.8, 1.0],
}

grid_search = GridSearchCV(
    estimator=model, param_grid=param_grid, cv=3, scoring="neg_mean_squared_error"
)
grid_search.fit(X_train, y_train)
print("Best parameters found: ", grid_search.best_params_)

best_params = {
    "learning_rate": 0.1,
    "max_depth": 12,
    "n_estimators": 300,
    "subsample": 0.8,
}

model = xgb.XGBRegressor(**best_params, random_state=17)
model.fit(X_train, y_train)

if hasattr(model, "feature_names_in_"):
    print("Columns used by the model:", model.feature_names_in_)
else:
    print("Model does not have feature names information.")

y_pred = model.predict(X_test)

mape = mean_absolute_percentage_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print(f"MAE: {mae:.2f}")
print(f"RMSE: {rmse:.2f}")
print(f"MAPE: {mape:.2f}")

joblib.dump(model, "occupancy_model_xgb.pkl")
