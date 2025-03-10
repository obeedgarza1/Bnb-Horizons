import sys
from sqlalchemy.orm import sessionmaker
from sqlalchemy import (
    SMALLINT,
    VARCHAR,
    INTEGER,
    BIGINT,
    FLOAT,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
import json
import pandas as pd
import duckdb

sys.path.append("..")
sys.path.append("../..")
from backend.db_connection import get_sqlalchemy_session
from silver.data_cleaning import get_and_clean_data
from silver.data_cleaning import clean_json

engine, session = get_sqlalchemy_session()

df = get_and_clean_data()
geojson_df = clean_json()
calendar_path = r"../../data/calendar_with_season.parquet"


def insert_calendar_table(path):
    try:
        calendar_df = duckdb.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
        calendar_df = calendar_df.rename(columns={"listing_id": "id", "date": "count"})
        calendar_df["available"] = (
            calendar_df["available"].map({"t": 1, "f": 0}).fillna("unknown")
        )
        calendar_df = calendar_df.drop(columns="city")
        calendar_df.to_sql(
            "calendar",
            engine,
            schema="silver",
            if_exists="fail",
            index=False,
            method="multi",
        )
        print("calendar inserted into the Bronze table successfully!")
    except Exception as e:
        print(f"Error inserting data into Bronze table: {e}")


insert_calendar_table(calendar_path)


def city_table():
    city_df = pd.DataFrame(df["city"].unique(), columns=["city_name"])

    dtype_dict = {
        "city_id": SMALLINT(),
        "city_name": VARCHAR(50),
    }

    city_df.to_sql(
        "city",
        engine,
        schema="silver",
        if_exists="fail",
        index=True,
        index_label="city_id",
        method="multi",
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE silver.city
        ADD CONSTRAINT pk_city PRIMARY KEY (city_id);
    """
        )
    )
    session.commit()
    print("Unique cities inserted into the database successfully!")


city_table()


def property_table():
    property_df = pd.DataFrame(df["property_type"].unique(), columns=["property_type"])

    dtype_dict = {
        "property_id": SMALLINT(),
        "property_type": VARCHAR(50),
    }

    property_df.to_sql(
        "property_types",
        engine,
        schema="silver",
        if_exists="fail",
        index=True,
        index_label="property_id",
        method="multi",
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE silver.property_types
        ADD CONSTRAINT pk_property_type PRIMARY KEY (property_id);
    """
        )
    )
    session.commit()
    print("Unique properties inserted into the database successfully!")


property_table()


def room_type_table():
    room_type_df = pd.DataFrame(df["room_type"].unique(), columns=["room_type"])

    dtype_dict = {
        "room_type_id": SMALLINT(),
        "room_type": VARCHAR(50),
    }

    room_type_df.to_sql(
        "room_types",
        engine,
        schema="silver",
        if_exists="fail",
        index=True,
        index_label="room_type_id",
        method="multi",
        dtype=dtype_dict,
    )
    session.execute(
        text(
            """
        ALTER TABLE silver.room_types
        ADD CONSTRAINT pk_room_type PRIMARY KEY (room_type_id);
    """
        )
    )
    session.commit()
    print("Unique room types inserted into the database successfully!")


room_type_table()


def convert_to_geojson(geometry):
    return geometry.__geo_interface__


def neighbourhoods_table():
    neighbourhood_df = clean_json()
    city_df = pd.read_sql("SELECT * FROM silver.city", engine)
    neighbourhood_df = pd.merge(
        neighbourhood_df,
        city_df["city_id"],
        left_on="city",
        right_on=city_df["city_name"],
        how="left",
    ).drop(columns="city")

    neighbourhood_df["geometry"] = neighbourhood_df["geometry"].apply(
        lambda geom: convert_to_geojson(geom)
    )
    neighbourhood_df["geometry"] = neighbourhood_df["geometry"].apply(
        lambda geom: json.dumps(geom)
    )

    dtype_dict = {
        "neighbourhood_id": INTEGER(),
        "neighbourhood": VARCHAR(100),
        "neighbourhood_group": VARCHAR(100),
        "geometry": JSONB(),
        "city_id": SMALLINT(),
    }

    neighbourhood_df.to_sql(
        "neighbourhoods",
        engine,
        schema="silver",
        index=True,
        index_label="neighbourhood_id",
        method="multi",
        dtype=dtype_dict,
    )

    session.execute(
        text(
            """
    ALTER TABLE silver.neighbourhoods
    ADD CONSTRAINT pk_neighbourhood PRIMARY KEY (neighbourhood_id),
    ADD CONSTRAINT fk_city FOREIGN KEY (city_id) REFERENCES silver.city (city_id)
    """
        )
    )
    session.commit()


neighbourhoods_table()


def date_table():
    date_df = pd.DataFrame(df["date"].unique(), columns=["date"])
    custom_order = ["Q4_23", "Q1_24", "Q2_24", "Q3_24"]
    date_df["date"] = pd.Categorical(
        date_df["date"], categories=custom_order, ordered=True
    )
    date_df = date_df.sort_values("date").reset_index(drop=True)

    dtype_dict = {
        "date_id": SMALLINT(),
        "date": VARCHAR(10),
    }

    date_df.to_sql(
        "dates",
        engine,
        schema="silver",
        if_exists="fail",
        index=True,
        index_label="date_id",
        method="multi",
        dtype=dtype_dict,
    )
    session.execute(
        text("ALTER TABLE silver.dates ADD CONSTRAINT pk_dates PRIMARY KEY (date_id);")
    )
    session.commit()
    print("Unique dates inserted into the database successfully!")


date_table()


def host_table():
    date_df = pd.read_sql("SELECT * FROM silver.dates", engine)
    host_df = pd.DataFrame(
        df[
            [
                "host_id",
                "host_name",
                "host_since",
                "host_about",
                "host_picture_url",
                "host_response_time",
                "host_is_superhost",
                "host_identity_verified",
                "date",
                "id",
            ]
        ].drop_duplicates()
    )
    host_df = pd.merge(host_df, date_df, left_on="date", right_on="date", how="left")
    host_df = host_df.drop(columns="date")
    host_df["host_since"] = pd.to_datetime(host_df["host_since"])

    host_details_df = host_df[["host_id", "host_name", "host_since"]].drop_duplicates(
        subset=["host_id"]
    )
    host_activity_df = host_df[
        [
            "host_id",
            "date_id",
            "id",
            "host_about",
            "host_picture_url",
            "host_is_superhost",
            "host_identity_verified",
            "host_response_time",
        ]
    ].rename(columns={"id": "listing_id"})

    host_details_df.to_sql(
        "host_details",
        engine,
        schema="silver",
        index=False,
        if_exists="replace",
        method="multi",
    )

    host_activity_df.to_sql(
        "host_activity",
        engine,
        schema="silver",
        index=False,
        if_exists="replace",
        method="multi",
        chunksize=20000,
    )

    session.execute(
        text(
            """
            CREATE INDEX idx_host_activity_host_id ON silver.host_activity (host_id);
            CREATE INDEX idx_host_activity_date_id ON silver.host_activity (date_id);
            CREATE INDEX idx_host_activity_listing_id ON silver.host_activity (listing_id);

            ALTER TABLE silver.host_details
            ADD CONSTRAINT pk_host_details PRIMARY KEY (host_id);

            ALTER TABLE silver.host_activity
            ADD CONSTRAINT fk_host_details FOREIGN KEY (host_id) REFERENCES silver.host_details(host_id),
            ADD CONSTRAINT fk_host_date FOREIGN KEY (date_id) REFERENCES silver.dates(date_id);
            """
        )
    )
    session.commit()

    print("Host tables created successfully!")


host_table()


def listings_table():
    listings_df = pd.DataFrame(
        df[
            [
                "id",
                "name",
                "description",
                "listing_url",
                "picture_url",
                "latitude",
                "longitude",
                "property_type",
                "room_type",
                "accommodates",
                "bedrooms",
                "bathrooms",
                "minimum_nights",
                "maximum_nights",
                "city",
                "neighbourhood",
                "season",
                "review_missing",
                "review_scores_rating",
                "categorized_amenities",
                "host_id",
                "date",
                "price_float",
            ]
        ]
    )
    property_df = pd.read_sql("SELECT * FROM silver.property_types", engine)
    room_type_df = pd.read_sql("SELECT * FROM silver.room_types", engine)
    city_df = pd.read_sql("SELECT * FROM silver.city", engine)
    neighbourhood_df = pd.read_sql("SELECT * FROM silver.neighbourhoods", engine)
    date_df = pd.read_sql("SELECT * FROM silver.dates", engine)

    listings_df = pd.merge(listings_df, property_df, on="property_type", how="left")
    listings_df = pd.merge(listings_df, room_type_df, on="room_type", how="left")
    listings_df = pd.merge(
        listings_df,
        city_df[["city_id", "city_name"]],
        left_on="city",
        right_on="city_name",
        how="left",
    )
    listings_df = pd.merge(
        listings_df,
        neighbourhood_df[["neighbourhood_id", "neighbourhood", "city_id"]],
        on=["neighbourhood", "city_id"],
        how="left",
    )
    listings_df = listings_df[~pd.isnull(listings_df["neighbourhood_id"])]
    listings_df["neighbourhood_id"] = listings_df["neighbourhood_id"].astype("int")
    listings_df = pd.merge(listings_df, date_df, on="date", how="left")

    listings_df["categorized_amenities"] = listings_df["categorized_amenities"].apply(
        json.dumps
    )
    listings_df = listings_df.drop(
        columns=[
            "property_type",
            "room_type",
            "city",
            "neighbourhood",
            "date",
            "city_name",
        ]
    )

    dtype_dict = {
        "id": BIGINT(),
        "name": VARCHAR(),
        "description": VARCHAR(),
        "listing_url": VARCHAR(),
        "picture_url": VARCHAR(),
        "latitude": FLOAT(),
        "longitude": FLOAT(),
        "accommodates": INTEGER(),
        "bedrooms": INTEGER(),
        "bathrooms": INTEGER(),
        "minimum_nights": SMALLINT(),
        "maximum_nights": INTEGER(),
        "season": VARCHAR(),
        "review_missing": SMALLINT(),
        "review_scores_rating": FLOAT(),
        "categorized_amenities": VARCHAR(),
        "host_id": INTEGER(),
        "price_float": FLOAT(),
        "property_id": INTEGER(),
        "room_type_id": SMALLINT(),
        "city_id": SMALLINT(),
        "neighbourhood_id": SMALLINT(),
        "date_id": SMALLINT(),
    }

    listings_df.to_sql(
        "listings",
        engine,
        schema="silver",
        index=False,
        method="multi",
        chunksize=20000,
        dtype=dtype_dict,
    )

    session.execute(
        text(
            """
            CREATE INDEX idx_listings_host_id ON silver.listings (host_id);
            CREATE INDEX idx_listings_date_id ON silver.listings (date_id);
            CREATE INDEX idx_listings_id ON silver.listings (id);
            """
        )
    )
    session.commit()

    session.execute(
        text(
            """
            ALTER TABLE silver.listings
            ADD CONSTRAINT pk_listings PRIMARY KEY (id, date_id),
            ADD CONSTRAINT fk_listings_property FOREIGN KEY (property_id) REFERENCES silver.property_types(property_id),
            ADD CONSTRAINT fk_listings_room_type FOREIGN KEY (room_type_id) REFERENCES silver.room_types(room_type_id),
            ADD CONSTRAINT fk_listings_city FOREIGN KEY (city_id) REFERENCES silver.city(city_id),
            ADD CONSTRAINT fk_listings_neighbourhood FOREIGN KEY (neighbourhood_id) REFERENCES silver.neighbourhoods(neighbourhood_id),
            ADD CONSTRAINT fk_listings_date FOREIGN KEY (date_id) REFERENCES silver.dates(date_id);
            """
        )
    )
    session.commit()

    print("Listings table created successfully!")


listings_table()
