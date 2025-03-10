import sys
import os

# Add backend folder to the path
sys.path.append(os.path.abspath("backend"))
from db_connection import get_duckdb_connection

import streamlit as st
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
import json
import scipy.stats as stats


@st.cache_resource(show_spinner=False)
def get_db_connection():
    con = get_duckdb_connection()
    return con


con = get_duckdb_connection()


@st.cache_resource(show_spinner=False)
def get_filters():
    """
    Retrieve unique accommodates and room types from the database.
    """
    query = """
        SELECT
            array_agg(DISTINCT l.accommodates) AS unique_accommodates,
            array_agg(DISTINCT r.room_type) AS unique_room_types
        FROM pgdb.silver.listings l
        LEFT JOIN pgdb.silver.room_types r ON l.room_type_id = r.room_type_id
    """

    result = con.execute(query).fetchone()

    unique_accommodates = result[0] if result[0] else []
    unique_room_types = result[1] if result[1] else []

    return unique_accommodates, unique_room_types


@st.cache_resource(show_spinner=False)
def get_cities():
    """
    Retrieve city names from the database.
    """
    return sorted(
        row[0]
        for row in con.execute(
            "SELECT DISTINCT city_name FROM pgdb.silver.city"
        ).fetchall()
    )


@st.cache_resource(show_spinner=False)
def get_room_types():
    """
    Retrieve room types from the database.
    """
    return sorted(
        row[0]
        for row in con.execute(
            "SELECT DISTINCT room_type FROM pgdb.silver.room_types"
        ).fetchall()
    )


@st.cache_resource(show_spinner=False)
def get_neighbourhoods(city):
    """
    Retrieve neighbourhood names and IDs for the given city.
    """
    if city:
        return con.execute(
            """
            SELECT DISTINCT n.neighbourhood, n.neighbourhood_id
            FROM pgdb.silver.neighbourhoods n
            JOIN pgdb.silver.city c ON n.city_id = c.city_id
            WHERE c.city_name = ?
            """,
            [city],
        ).fetchall()
    return []


def get_loc(city, neighbourhood, city_centers):
    """
    Get the average latitude and longitude of the neighbourhood and the distance to the city center.
    """
    query = """
        SELECT AVG(latitude) AS latitude, AVG(longitude) AS longitude
        FROM pgdb.gold.earnings_summary
        WHERE city_name = ? AND neighbourhood = ?"""

    params = [city, neighbourhood]
    result = con.execute(query, params).fetchone()

    avg_lat, avg_lon = result[0], result[1]
    city_center = city_centers.get(city, {"latitude": 0, "longitude": 0})
    distance_to_center = np.sqrt(
        (avg_lat - city_center["latitude"]) ** 2
        + (avg_lon - city_center["longitude"]) ** 2
    )
    return avg_lat, avg_lon, distance_to_center


@st.cache_data(show_spinner=False)
def geometry_query():
    """
    Query the database and return neighbourhoods with geometry as proper JSON.
    """
    query = """
    SELECT 
        n.neighbourhood_id,
        n.neighbourhood, 
        n.geometry
    FROM pgdb.silver.neighbourhoods n;
    """
    try:
        data = con.execute(query).fetchdf()
        data["geometry"] = data["geometry"].apply(
            lambda x: shape(json.loads(x.strip('"').replace('\\"', '"')))
        )
        return data
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame()


df = geometry_query()


def data_query(where_clause):
    """
    Query the DuckDB database and return filtered data based on user input.
    """
    query = f"""
    SELECT *
    FROM pgdb.gold.listings_aggregated l
    WHERE {where_clause}
    ORDER BY id DESC;
    """
    try:
        data = con.execute(query).fetchdf()
        data["seasonal_prices"] = data["seasonal_prices"].apply(
            lambda x: x.strip('"').replace('\\"', '"') if pd.notna(x) else x
        )
        return data
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame()


def neigh_price_query(selected_neighbourhood):
    """
    Query the data of neighbourhoods and return the average price, accommodates, bedrooms, bathrooms, and the data of the selected neighbourhood.
    """
    query = f"""
    SELECT 
        l.neighbourhood,
        l.price_float,
        l.accommodates,
        l.bedrooms,
        l.bathrooms
    FROM pgdb.gold.earnings_summary l
    WHERE l.neighbourhood = '{selected_neighbourhood}'
    """
    try:
        data = con.execute(query).fetchdf()
        avg_accommodates = data["accommodates"].mean()
        avg_bedrooms = data["bedrooms"].mean()
        avg_bathrooms = data["bathrooms"].mean()
        mean_price = data["price_float"].mean()
        std_dev = data["price_float"].std()
        n = len(data)
        if n > 1 and std_dev > 0:
            if n > 30:
                critical_value = stats.norm.ppf(0.975)
            else:
                critical_value = stats.t.ppf(0.975, n - 1)
            margin_of_error = critical_value * std_dev / np.sqrt(n)
            lower_bound = mean_price - margin_of_error
            upper_bound = mean_price + margin_of_error
        else:
            lower_bound, upper_bound = mean_price

        return (
            avg_accommodates,
            avg_bedrooms,
            avg_bathrooms,
            lower_bound,
            upper_bound,
            data,
        )
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame()


st.cache_data(show_spinner=False)


@st.cache_resource(show_spinner=False)
def get_seasons():
    """
    Get the seasons from the database and sort them in a specific order.
    """
    place_order = {
        "Early Spring": 1,
        "Early Summer": 2,
        "Early Autumn": 3,
        "Early Winter": 4,
    }

    seasons = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT season FROM pgdb.silver.listings"
        ).fetchall()
    ]

    return sorted(seasons, key=lambda season: place_order.get(season, float("inf")))


@st.cache_resource(show_spinner=False)
def price_ranges():
    """
    Get the price ranges from the database and sort them in a specific order.
    """
    price_order = {
        "Extremely Cheap (<$20)": 1,
        "Very Cheap ($20-$50)": 2,
        "Cheap ($50-$100)": 3,
        "Moderate ($100-$200)": 4,
        "Expensive ($200-$300)": 5,
        "Very Expensive (>$300)": 6,
    }
    price_range = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT price_range FROM pgdb.gold.reccomendations_summary"
        ).fetchall()
    ]

    return sorted(price_range, key=lambda price: price_order.get(price, float("inf")))


def reccomendation_query(where_clause):
    """
    Query the DuckDB database and return filtered data based on user input.
    """
    query = f"""
    SELECT *
    FROM pgdb.gold.reccomendations_summary
    WHERE {where_clause}
    ORDER BY id DESC;
    """
    try:
        data = con.execute(query).fetchdf()
        data["categorized_amenities"] = data["categorized_amenities"].apply(
            lambda x: json.loads(x) if isinstance(x, str) else x
        )

        return data
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame()
