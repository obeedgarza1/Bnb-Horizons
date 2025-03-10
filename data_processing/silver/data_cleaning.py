import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import os
import geopandas as gpd
import shapely
from shapely.wkt import loads

sys.path.append("../..")
from backend.db_connection import get_duckdb_connection

spark = SparkSession.builder.appName("data_cleaning").getOrCreate()


from utilities.categories_dict import categories_final

con = get_duckdb_connection()

selected_columns = [
    "id",
    "name",
    "description",
    "listing_url",
    "picture_url",
    "host_id",
    "host_name",
    "host_about",
    "host_since",
    "host_response_time",
    "host_is_superhost",
    "host_identity_verified",
    "host_picture_url",
    "neighbourhood_cleansed",
    "latitude",
    "longitude",
    "property_type",
    "room_type",
    "accommodates",
    "bathrooms_text",
    "bedrooms",
    "amenities",
    "price",
    "minimum_nights",
    "maximum_nights",
    "review_scores_rating",
    "city",
    "quarter",
    "year",
]

file_path = "../../data/bronze_listings_raw.parquet"


def get_and_clean_data():
    if not os.path.exists(file_path):
        con.sql(
            """
            COPY (
                SELECT * FROM pgdb.bronze.listings_raw
            ) TO '../../data/bronze_listings_raw.parquet' (FORMAT PARQUET);
            """
        )

    spark_df = spark.read.parquet("../../data/bronze_listings_raw.parquet").select(
        selected_columns
    )
    df = spark_df.filter(spark_df.price.isNotNull())
    median_bedrooms = df.groupBy("accommodates").agg(
        F.expr("percentile_approx(bedrooms, 0.5)").alias("median_bedrooms")
    )

    df = df.join(median_bedrooms, on="accommodates", how="left")
    df = (
        df.withColumn(
            "name", F.regexp_replace(F.trim("name"), r"[^\w\s\'\.\-\p{L}]+", "")
        )
        .withColumn(
            "description",
            F.regexp_replace(F.trim("description"), r"[^\w\s\'\.\-\p{L}]+", ""),
        )
        .withColumn(
            "neighbourhood",
            F.lower(
                F.initcap(
                    F.regexp_replace(
                        "neighbourhood_cleansed", r"[^\w\s\'\.\-\p{L}]+", ""
                    )
                )
            ),
        )
        .withColumn(
            "price_float",
            F.regexp_replace(F.regexp_replace("price", r"\$", ""), r",", "").cast(
                "float"
            ),
        )
        .withColumn("bathrooms", F.regexp_extract("bathrooms_text", r"\d+", 0))
        .withColumn("bathrooms", F.col("bathrooms").cast("float"))
        .withColumn(
            "season",
            F.when(F.col("quarter") == "Q1", "Early Spring")
            .when(F.col("quarter") == "Q2", "Early Summer")
            .when(F.col("quarter") == "Q3", "Early Autumn")
            .when(F.col("quarter") == "Q4", "Early Winter"),
        )
        .withColumn(
            "host_response_time",
            F.when(F.col("host_response_time").isNull(), "unknown").otherwise(
                F.col("host_response_time")
            ),
        )
        .withColumn(
            "host_is_superhost",
            F.when(F.col("host_is_superhost").isNull(), "unknown").otherwise(
                F.col("host_is_superhost")
            ),
        )
        .withColumn("host_since", F.to_date(F.col("host_since"), "yyyy-MM-dd"))
        .withColumn(
            "review_missing",
            F.when(F.col("review_scores_rating").isNull(), 1).otherwise(0),
        )
        .withColumn(
            "bedrooms",
            F.when(F.col("bedrooms").isNull(), F.col("median_bedrooms")).otherwise(
                F.col("bedrooms")
            ),
        )
        .withColumn("date", F.concat_ws("_", F.col("quarter"), F.col("year")))
        .withColumn(
            "amenities",
            F.regexp_replace(F.col("amenities"), r"\\u[0-9a-fA-F]{4}|\\/", ""),
        )
    )

    median_bathrooms = df.groupBy("accommodates").agg(
        F.expr("percentile_approx(bathrooms, 0.5)").alias("median_bathrooms")
    )
    df = df.join(median_bathrooms, on="accommodates", how="left")
    df = df.withColumn(
        "bathrooms",
        F.when(F.col("bathrooms").isNull(), F.col("median_bathrooms")).otherwise(
            F.col("bathrooms")
        ),
    )
    df = df.withColumn("bathrooms", F.col("bathrooms").cast("int")).withColumn(
        "bedrooms", F.col("bedrooms").cast("int")
    )
    quantiles = df.approxQuantile("price_float", [0.25, 0.75, 0.5], 0.01)
    Q1 = quantiles[0]
    Q3 = quantiles[1]
    median_price = quantiles[2]

    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    df = df.withColumn(
        "price_float",
        F.when(
            (F.col("price_float") < lower_bound) | (F.col("price_float") > upper_bound),
            median_price,
        ).otherwise(F.col("price_float")),
    )
    amenities_df = df.select("id", "date", "amenities")
    amenities_df = amenities_df.withColumn(
        "amenities_split", F.split(F.col("amenities"), ",")
    )
    amenities_df = amenities_df.withColumn(
        "amenities_split", F.explode(F.col("amenities_split"))
    )

    clean_amenities_udf = F.udf(
        lambda x: (
            x.replace("'", "")
            .replace('"', "")
            .replace("  ", " ")
            .replace("[", "")
            .replace("]", "")
            .replace("\\", "")
            .strip()
            .lower()
            if x
            else None
        )
    )
    amenities_df = amenities_df.withColumn(
        "amenities_split", clean_amenities_udf(F.col("amenities_split"))
    )

    def categorize_amenity_udf(amenity):
        if not amenity:
            return "Other"

        amenity = amenity.strip().lower()
        for category, keywords in categories_final.items():
            if any(keyword.lower() in amenity for keyword in keywords):
                return category
        return "Other"

    categorize_amenity_spark_udf = F.udf(categorize_amenity_udf, StringType())

    amenities_df = amenities_df.withColumn(
        "category", categorize_amenity_spark_udf(F.col("amenities_split"))
    )
    amenities_df = amenities_df.filter(amenities_df["category"] != "Other")

    grouped = amenities_df.groupBy("id", "date", "category").agg(
        F.collect_list("amenities_split").alias("amenities_list")
    )
    grouped_dict = grouped.groupBy("id", "date").agg(
        F.map_from_entries(
            F.collect_list(F.struct("category", "amenities_list"))
        ).alias("categorized_amenities")
    )

    final_df = df.join(grouped_dict, on=["id", "date"], how="left").drop(
        "price",
        "neighbourhood_cleansed",
        "bathrooms_text",
        "median_bedrooms",
        "median_bathrooms",
        "amenities",
        "quarter",
        "year",
    )
    final_df = final_df.toPandas()

    final_df = final_df[
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
            "host_name",
            "host_about",
            "host_since",
            "host_response_time",
            "host_is_superhost",
            "host_identity_verified",
            "host_picture_url",
            "date",
            "price_float",
        ]
    ]
    return final_df


def clean_json():
    geojson_df = pd.read_csv("../../data/geojson_df.csv")
    geojson_df["neighbourhood"] = (
        geojson_df["neighbourhood"]
        .str.replace(r"[^\w\s\'.\-]+", "", regex=True)
        .str.title()
    )
    geojson_df["neighbourhood"] = geojson_df["neighbourhood"].str.lower()
    geojson_df = geojson_df.drop_duplicates(subset="neighbourhood", keep="first")
    geojson_df = geojson_df.drop(columns="Unnamed: 0")
    geojson_df["geometry"] = geojson_df["geometry"].apply(loads)
    geojson_df["geometry"].apply(
        lambda x: isinstance(x, shapely.geometry.multipolygon.MultiPolygon)
    )
    geojson_df = gpd.GeoDataFrame(
        geojson_df,
        geometry="geometry",
        crs="EPSG:4326",
    )
    return geojson_df
