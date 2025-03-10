from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, VARCHAR, INTEGER, BIGINT, FLOAT, text, JSON
from sqlalchemy.dialects.postgresql import JSONB
import pandas as pd
import numpy as np
import json
import sys

sys.path.append("../..")
from backend.db_connection import get_sqlalchemy_session

engine, session = get_sqlalchemy_session()


def listings_aggregated():
    query = """
        SELECT 
            l.id, 
            l.date_id,
            l.season,
            n.neighbourhood_id,
            n.neighbourhood, 
            c.city_name,
            p.property_type,
            r.room_type, 
            l.accommodates,
            l.bedrooms,
            l.bathrooms,
            l.minimum_nights, 
            l.maximum_nights, 
            l.listing_url,
            l.picture_url,
            l.latitude,
            l.longitude,
            l.review_missing,
            l.review_scores_rating, 
            l.categorized_amenities,
            l.price_float
        FROM silver.listings l
        LEFT JOIN silver.property_types p ON l.property_id = p.property_id
        LEFT JOIN silver.room_types r ON l.room_type_id = r.room_type_id
        LEFT JOIN silver.city c ON l.city_id = c.city_id
        LEFT JOIN silver.neighbourhoods n ON l.neighbourhood_id = n.neighbourhood_id
        """
    listings_df = pd.read_sql(query, engine)

    listings_df["categorized_amenities"] = listings_df["categorized_amenities"].replace(
        "null", np.nan
    )

    listings_df = listings_df.sort_values(by="date_id", ascending=False)

    picture_url = listings_df.groupby("id")["picture_url"].last().reset_index()
    categories = listings_df.groupby("id")["categorized_amenities"].last().reset_index()
    neighbourhoods = (
        listings_df.groupby("id")[["neighbourhood_id", "neighbourhood"]]
        .last()
        .reset_index()
    )
    property_types = listings_df.groupby("id")["property_type"].last().reset_index()
    room_types = listings_df.groupby("id")["room_type"].last().reset_index()

    seasonal_prices = (
        listings_df.groupby("id")
        .apply(lambda x: json.dumps(dict(zip(x["season"], x["price_float"]))))
        .reset_index(name="seasonal_prices")
    )

    final_df = (
        picture_url.merge(categories, on="id", how="left")
        .merge(neighbourhoods, on="id", how="left")
        .merge(property_types, on="id", how="left")
        .merge(room_types, on="id", how="left")
        .merge(seasonal_prices, on="id", how="left")
    )

    grouped_listings = (
        listings_df.groupby("id")
        .agg(
            {
                "city_name": "last",
                "listing_url": "last",
                "accommodates": "mean",
                "bedrooms": "mean",
                "bathrooms": "mean",
                "minimum_nights": "mean",
                "maximum_nights": "mean",
                "latitude": "mean",
                "longitude": "mean",
                "review_missing": "mean",
                "review_scores_rating": "mean",
                "price_float": "mean",
            }
        )
        .reset_index()
    )

    columns_to_round = [
        "review_missing",
        "accommodates",
        "bedrooms",
        "bathrooms",
        "minimum_nights",
        "maximum_nights",
    ]
    grouped_listings[columns_to_round] = (
        grouped_listings[columns_to_round].round(0).astype(int)
    )

    final_df = final_df.merge(grouped_listings, on="id", how="left")

    final_df = final_df[
        [
            "id",
            "neighbourhood_id",
            "neighbourhood",
            "city_name",
            "property_type",
            "room_type",
            "accommodates",
            "bedrooms",
            "bathrooms",
            "minimum_nights",
            "maximum_nights",
            "listing_url",
            "picture_url",
            "latitude",
            "longitude",
            "review_missing",
            "review_scores_rating",
            "categorized_amenities",
            "price_float",
            "seasonal_prices",
        ]
    ]

    dtype_dict = {
        "id": BIGINT(),
        "neighbourhood_id": INTEGER(),
        "neighbourhood": VARCHAR(),
        "city_name": VARCHAR(),
        "property_type": VARCHAR(),
        "room_type": VARCHAR(),
        "accommodates": INTEGER(),
        "bedrooms": INTEGER(),
        "bathrooms": INTEGER(),
        "minimum_nights": INTEGER(),
        "maximum_nights": INTEGER(),
        "listing_url": VARCHAR(),
        "picture_url": VARCHAR(),
        "latitude": FLOAT(),
        "longitude": FLOAT(),
        "review_missing": INTEGER(),
        "review_scores_rating": FLOAT(),
        "categorized_amenities": JSONB(),
        "price_float": FLOAT(),
        "seasonal_prices": JSONB(),
    }
    final_df.to_sql(
        "listings_aggregated",
        engine,
        schema="gold",
        index=False,
        method="multi",
        chunksize=20000,
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE gold.listings_aggregated
        ADD CONSTRAINT pk_listings PRIMARY KEY (id)
     """
        )
    )


print("lisitings_aggregated inserted into the database successfully!")

listings_aggregated()


def earnings_summary():
    query = """
    SELECT 
        l.id, 
        l.date_id,
        l.season,
        n.neighbourhood_id,
        n.neighbourhood, 
        c.city_name,
        p.property_type,
        r.room_type, 
        l.accommodates,
        l.bedrooms,
        l.bathrooms,
        l.latitude,
        l.longitude,
        ha.host_is_superhost,
        ha.host_identity_verified,
        ha.host_response_time,
        l.review_missing,
        l.review_scores_rating, 
        l.categorized_amenities,
        l.price_float
    FROM silver.listings AS l
    LEFT JOIN silver.property_types AS p ON l.property_id = p.property_id
    LEFT JOIN silver.room_types AS r ON l.room_type_id = r.room_type_id
    LEFT JOIN silver.city AS c ON l.city_id = c.city_id
    LEFT JOIN silver.neighbourhoods AS n ON l.neighbourhood_id = n.neighbourhood_id
    LEFT JOIN silver.host_activity AS ha 
        ON l.host_id = ha.host_id 
        AND l.date_id = ha.date_id 
        AND l.id = ha.listing_id;
        """
    earnings_df = pd.read_sql(query, engine)

    query = """
    SELECT * 
    FROM silver.calendar
    """
    calendar_df = pd.read_sql(query, engine)
    calendar_df = calendar_df.pivot_table(
        index=["id", "season"], columns="available", values="count"
    ).reset_index()
    calendar_df = calendar_df.rename(
        columns={0: "unavailable_days", 1: "available_days"}
    )
    df = pd.merge(earnings_df, calendar_df, on=["id", "season"], how="left")
    df[["unavailable_days", "available_days"]] = (
        df[["unavailable_days", "available_days"]].fillna(0).astype("int")
    )
    df["host_is_superhost"] = df["host_is_superhost"].map(
        {"unknown": 2, "t": 1, "f": 0}
    )
    df = df[~df["host_identity_verified"].isnull()]
    df["host_identity_verified"] = df["host_identity_verified"].map({"t": 1, "f": 0})
    df = df[
        [
            "id",
            "neighbourhood_id",
            "neighbourhood",
            "city_name",
            "season",
            "property_type",
            "room_type",
            "accommodates",
            "bedrooms",
            "bathrooms",
            "latitude",
            "longitude",
            "host_is_superhost",
            "host_identity_verified",
            "host_response_time",
            "review_missing",
            "review_scores_rating",
            "categorized_amenities",
            "unavailable_days",
            "available_days",
            "price_float",
        ]
    ]

    dtype_dict = {
        "id": BIGINT(),
        "neighbourhood_id": INTEGER(),
        "neighbourhood": VARCHAR(),
        "city_name": VARCHAR(),
        "season": VARCHAR(20),
        "property_type": VARCHAR(),
        "room_type": VARCHAR(),
        "accommodates": INTEGER(),
        "bedrooms": INTEGER(),
        "bathrooms": INTEGER(),
        "latitude": FLOAT(),
        "longitude": FLOAT(),
        "host_is_superhost": INTEGER(),
        "host_identity_verified": INTEGER(),
        "host_response_time": VARCHAR(30),
        "review_missing": INTEGER(),
        "review_scores_rating": FLOAT(),
        "categorized_amenities": JSONB(),
        "unavailable_days": INTEGER(),
        "available_days": INTEGER(),
        "price_float": FLOAT(),
    }
    df.to_sql(
        "earnings_summary",
        engine,
        schema="gold",
        index=False,
        method="multi",
        chunksize=20000,
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE gold.earnings_summary
        ADD CONSTRAINT pk_listings_summary PRIMARY KEY (id, season)
    """
        )
    )
    session.commit()

    print("earnings_summary inserted into the database successfully!")


earnings_summary()


def reccomendation_summary():
    query = """
    SELECT 
        l.id,
        l.name,
        l.description,
        c.city_name,
        n.neighbourhood,
        r.room_type,
        l.listing_url,
        l.picture_url,
        h.host_name,
        ha.host_about,
        ha.host_response_time,
        ha.host_picture_url,
        l.latitude,
        l.longitude,
        l.accommodates,
        l.bedrooms,
        l.bathrooms,
        l.minimum_nights,
        l.maximum_nights,
        l.date_id,
        l.season,
        l.review_scores_rating,
        l.categorized_amenities,
        l.price_float,
        CASE 
            WHEN l.price_float < 20 THEN 'Extremely Cheap (<$20)'
            WHEN l.price_float >= 20 AND l.price_float < 50 THEN 'Very Cheap ($20-$50)'
            WHEN l.price_float >= 50 AND l.price_float < 100 THEN 'Cheap ($50-$100)'
            WHEN l.price_float >= 100 AND l.price_float < 200 THEN 'Moderate ($100-$200)'
            WHEN l.price_float >= 200 AND l.price_float < 300 THEN 'Expensive ($200-$300)'
            ELSE 'Very Expensive (>$300)'
        END AS price_range
    FROM silver.listings l
    LEFT JOIN silver.city c ON l.city_id = c.city_id
    LEFT JOIN silver.neighbourhoods n ON l.neighbourhood_id = n.neighbourhood_id
    LEFT JOIN silver.room_types r ON l.room_type_id = r.room_type_id
    LEFT JOIN silver.host_details h ON l.host_id = h.host_id
    LEFT JOIN silver.host_activity AS ha 
        ON l.host_id = ha.host_id 
        AND l.date_id = ha.date_id 
        AND l.id = ha.listing_id;
    """
    reccomendation_df = pd.read_sql(query, engine)
    reccomendation_df["categorized_amenities"] = reccomendation_df[
        "categorized_amenities"
    ].replace(["None", "null"], np.nan)

    reccomendation_df = reccomendation_df.sort_values(by="date_id", ascending=False)

    picture_url = reccomendation_df.groupby("id")["picture_url"].last().reset_index()
    reccomendation_df = reccomendation_df.merge(
        picture_url, on="id", suffixes=("", "_latest")
    )
    reccomendation_df["picture_url"] = reccomendation_df["picture_url_latest"]

    categories = (
        reccomendation_df.groupby("id")["categorized_amenities"].last().reset_index()
    )
    reccomendation_df = reccomendation_df.merge(
        categories, on="id", suffixes=("", "_latest")
    )

    reccomendation_df["categorized_amenities"] = reccomendation_df[
        "categorized_amenities"
    ].fillna(reccomendation_df["categorized_amenities_latest"])

    reccomendation_df.drop(
        columns=["categorized_amenities_latest", "picture_url_latest", "date_id"],
        inplace=True,
    )

    reccomendation_df["categorized_amenities"] = reccomendation_df[
        "categorized_amenities"
    ].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

    reccomendation_df["categorized_amenities"] = reccomendation_df[
        "categorized_amenities"
    ].fillna({})
    reccomendation_df = reccomendation_df[
        [
            "id",
            "name",
            "description",
            "listing_url",
            "picture_url",
            "season",
            "city_name",
            "neighbourhood",
            "room_type",
            "accommodates",
            "bedrooms",
            "bathrooms",
            "latitude",
            "longitude",
            "host_name",
            "host_about",
            "host_response_time",
            "host_picture_url",
            "minimum_nights",
            "maximum_nights",
            "review_scores_rating",
            "categorized_amenities",
            "price_float",
            "price_range",
        ]
    ]

    dtype_dict = {
        "id": BIGINT(),
        "name": VARCHAR(),
        "description": VARCHAR(),
        "listing_url": VARCHAR(),
        "picture_url": VARCHAR(),
        "season": VARCHAR(),
        "city_name": VARCHAR(),
        "neighbourhood": VARCHAR(),
        "room_type": VARCHAR(),
        "accommodates": INTEGER(),
        "bedrooms": INTEGER(),
        "bathrooms": INTEGER(),
        "latitude": FLOAT(),
        "longitude": FLOAT(),
        "host_name": VARCHAR(),
        "host_about": VARCHAR(),
        "host_response_time": VARCHAR(),
        "host_picture_url": VARCHAR(),
        "minimum_nights": INTEGER(),
        "maximum_nights": INTEGER(),
        "review_scores_rating": FLOAT(),
        "categorized_amenities": JSON(),
        "price_float": FLOAT(),
        "price_range": VARCHAR(),
    }
    reccomendation_df.to_sql(
        "reccomendations_summary",
        engine,
        schema="gold",
        index=False,
        method="multi",
        chunksize=20000,
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE gold.reccomendations_summary
        ADD CONSTRAINT pk_reccomendation_summary PRIMARY KEY (id, season)
    """
        )
    )
    session.commit()

    print("reccomendations inserted into the database successfully!")


reccomendation_summary()
