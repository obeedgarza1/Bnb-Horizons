from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
import os
import duckdb
from itertools import product
import sys
import os


# Add backend folder to the path
sys.path.append("../../")
from backend.db_connection import get_sqlalchemy_session, get_sqlite, get_duckdb_sqlite

current_dir = os.path.dirname(os.path.abspath(__file__))

data_path = os.path.join(current_dir, "../../data/spain_data.parquet")
engine, session = get_sqlalchemy_session()


def create_bronze_table():
    sql_create_table = """
        CREATE SCHEMA IF NOT EXISTS bronze;
        CREATE TABLE IF NOT EXISTS bronze.listings_raw (
        id bigint,
        listing_url text,
        scrape_id bigint,
        last_scraped text,
        source text,
        name text,
        description text,
        neighborhood_overview text,
        picture_url text,
        host_id bigint,
        host_url text,
        host_name text,
        host_since text,
        host_location text,
        host_about text,
        host_response_time text,
        host_response_rate text,
        host_acceptance_rate text,
        host_is_superhost text,
        host_thumbnail_url text,
        host_picture_url text,
        host_neighbourhood text,
        host_listings_count float,
        host_total_listings_count float,
        host_verifications text,
        host_has_profile_pic text,
        host_identity_verified text,
        neighbourhood text,
        neighbourhood_cleansed text,
        neighbourhood_group_cleansed text,
        latitude float,
        longitude float,
        property_type text,
        room_type text,
        accommodates bigint,
        bathrooms float,
        bathrooms_text text,
        bedrooms float,
        beds float,
        amenities text,
        price text,
        minimum_nights bigint,
        maximum_nights bigint,
        minimum_minimum_nights bigint,
        maximum_minimum_nights bigint,
        minimum_maximum_nights bigint,
        maximum_maximum_nights bigint,
        minimum_nights_avg_ntm float,
        maximum_nights_avg_ntm float,
        calendar_updated text,
        has_availability text,
        availability_30 bigint,
        availability_60 bigint,
        availability_90 bigint,
        availability_365 bigint,
        calendar_last_scraped text,
        number_of_reviews int,
        number_of_reviews_ltm int,
        number_of_reviews_l30d int,
        first_review text,
        last_review text,
        review_scores_rating float,
        review_scores_accuracy float,
        review_scores_cleanliness float,
        review_scores_checkin float,
        review_scores_communication float,
        review_scores_location float,
        review_scores_value float,
        license text,
        instant_bookable text,
        calculated_host_listings_count int,
        calculated_host_listings_count_entire_homes int,
        calculated_host_listings_count_private_rooms int,
        calculated_host_listings_count_shared_rooms int,
        reviews_per_month float,
        city text,
        quarter text,
        year int,
        CONSTRAINT listings_raw_pkey PRIMARY KEY (id, quarter, year)
        );
        """
    try:
        session.execute(text(sql_create_table))
        session.commit()
        print("Bronze table created successfully!")
    except Exception as e:
        print(f"Error creating Bronze table: {e}")
        session.rollback()


create_bronze_table()


def insert_bronze_data(path):
    """
    Reads data from the specified Parquet file and inserts it into the 'bronze.listings_raw' table.
    """
    try:
        df = duckdb.execute(f"SELECT * FROM read_parquet('{path}')").fetchdf()
        df = df[~((df["id"] == 1176931079717865040) & (df["city"] == "Madrid"))]
        df.to_sql(
            "listings_raw",
            engine,
            schema="bronze",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=20000,
        )
        print("Data inserted into the Bronze table successfully!")
    except Exception as e:
        print(f"Error inserting data into Bronze table: {e}")

        session.rollback()
    finally:
        session.close()


insert_bronze_data(data_path)
