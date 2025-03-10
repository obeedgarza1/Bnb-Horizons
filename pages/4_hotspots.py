import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
import json
from shapely.ops import transform
import pyproj
from streamlit.components.v1 import html
from folium.plugins import MarkerCluster
import warnings
from backend.queries import (
    get_filters,
    get_cities,
    get_neighbourhoods,
    data_query,
    geometry_query,
)

warnings.filterwarnings("ignore", category=FutureWarning, message=".*pyproj.*")

unique_accommodates, unique_room_types = get_filters()

if "selected_city" not in st.session_state:
    st.session_state.selected_city = None
if "neighborhoods" not in st.session_state:
    st.session_state.neighborhoods = []

cities = get_cities()

st.title("Explore best options for your search")
st.write("")

st.markdown(
    """
    **Explore Listings on the Map!**  
    This interactive map helps you discover available listings based on your selected filters. 
    Explore neighborhoods, view average prices, and click on markers to see detailed information about each property, 
    including photos, and seasoanl pricing. Use the legend to quickly identify price ranges and find the perfect stay for your trip!
    """
)

st.divider()

listings_df = None

col1, col2, col3, col4, col5, col6 = st.columns(6)

selected_city = col1.selectbox(
    "Select a City",
    options=cities,
    placeholder="Select an Option",
    key="selected_city",
)

if selected_city != "Select a city" and selected_city != st.session_state.get(
    "selected_city", ""
):
    st.session_state.selected_city = selected_city
    st.session_state.neighbourhoods = get_neighbourhoods(selected_city)

neighbourhoods = st.session_state.get(
    "neighbourhoods", get_neighbourhoods(selected_city)
)
neighbourhood_names = [n[0] for n in neighbourhoods]

selected_room_type = col2.selectbox(
    "Select Property Type", options=["All"] + unique_room_types
)
selected_accommodates = col3.number_input("Number of Accommodates?", 1, 50)
selected_days = col4.number_input("Number of Nights?", 1, 50)

selected_neighbourhood = col5.selectbox(
    "Select Neighbourhood", index=0, options=["All"] + neighbourhood_names
)

where_clauses = []
if selected_city != "Select a city":
    where_clauses.append(f"city_name = '{selected_city}'")
if selected_room_type != "All":
    where_clauses.append(f"room_type = '{selected_room_type}'")
if selected_accommodates:
    where_clauses.append(f"accommodates = {selected_accommodates}")
if selected_days:
    where_clauses.append(f"{selected_days} BETWEEN minimum_nights AND maximum_nights")
if selected_neighbourhood != "All":
    where_clauses.append(f"neighbourhood = '{selected_neighbourhood}'")

where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"


def simplify_geometry(geometry, tolerance=0.01):
    """Simplify geometry to reduce size."""
    project = pyproj.Transformer.from_proj(
        pyproj.Proj(init="epsg:4326"),
        pyproj.Proj(init="epsg:3857"),
    )
    geometry_transformed = transform(project.transform, geometry)
    simplified = geometry_transformed.simplify(tolerance, preserve_topology=True)
    return transform(
        lambda x, y, z=None: project.transform(x, y, direction="INVERSE"), simplified
    )


def price_color_function(price):
    """Maps prices to professional and distinguishable colors based on ranges."""
    if pd.isnull(price):
        return "#b8b8b8"  # Light Gray for null values
    elif price < 20:
        return "#d9f5d1"  # Very Light Green for extremely cheap
    elif 20 <= price < 50:
        return "#c3e2b7"  # Light Grayish Green for very cheap
    elif 50 <= price < 100:
        return "#86c77a"  # Soft Green for cheap
    elif 100 <= price < 200:
        return "#e2d0b3"  # Neutral Beige for moderate
    elif 200 <= price < 300:
        return "#e6a67b"  # Muted Orange for expensive
    else:
        return "#b24949"  # Deep Rust Red for very expensive


if "listings_df" not in st.session_state:
    st.session_state.listings_df = None

with col6:
    st.write("")
    if st.button("Search"):
        geometry_df = geometry_query()

        st.session_state.listings_df = data_query(where_clause)

listings_df = st.session_state.listings_df

if listings_df is not None:
    if listings_df.empty:
        st.warning("No listings found for the selected filters.")
        st.stop()

    geometry_df["geometry"] = geometry_df["geometry"].apply(simplify_geometry)

    geometry_df = gpd.GeoDataFrame(geometry_df, geometry="geometry", crs="EPSG:4326")

    neighborhood_stats = (
        listings_df.groupby("neighbourhood_id")["price_float"].mean().reset_index()
    )
    df = pd.merge(geometry_df, neighborhood_stats, on="neighbourhood_id", how="left")

    def create_map():
        map_center = [listings_df["latitude"].mean(), listings_df["longitude"].mean()]
        m = folium.Map(location=map_center, zoom_start=12, control_scale=True)

        folium.GeoJson(
            df,
            style_function=lambda feature: {
                "fillColor": price_color_function(
                    feature["properties"].get("price_float")
                ),
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["neighbourhood", "price_float"],
                aliases=["Neighborhood:", "Avg. Price ($):"],
            ),
        ).add_to(m)

        marker_cluster = MarkerCluster(
            maxClusterRadius=50, disableClusteringAtZoom=20
        ).add_to(m)
        listings_df["total_price"] = listings_df["price_float"] * selected_days
        listings_df["total_price"] = listings_df["total_price"].round(2)

        def format_seasonal_prices(seasonal_prices_json):
            if pd.isna(seasonal_prices_json):
                return "No seasonal pricing data available."

            try:
                seasonal_prices = json.loads(seasonal_prices_json)
                formatted_prices = "<br>".join(
                    [
                        f"{season}: ${price:.2f}"
                        for season, price in seasonal_prices.items()
                    ]
                )
                return f"<b>Seasonal Prices:</b><br>{formatted_prices}"
            except json.JSONDecodeError:
                return "Invalid seasonal pricing data."

        for _, row in listings_df.iterrows():
            seasonal_prices_html = format_seasonal_prices(row["seasonal_prices"])
            popup_html = f"""
                <div style="width: 200px;">
                    <h4>{row['neighbourhood']}</h4>
                    <img src="{row['picture_url']}" width="100%" style="border-radius: 5px;">
                    <p><b>Average Price per Night${row['price_float']:.2f}</b></p>
                    <b>Total Price ({selected_days} nights):</b> ${row['total_price']}
                    <p><b>{row['property_type']}</b></p>
                    <p><b>Bedrooms:</b> {row['bedrooms']}</p>
                    <p><b>Bathrooms:</b> {row['bathrooms']}</p>
                    <p>{seasonal_prices_html}</p>
                    <a href="{row['listing_url']}" target="_blank">View Listing</a>
                </div>
            """
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                icon=folium.Icon(color="blue"),
                tooltip=popup_html,
            ).add_to(marker_cluster)

        legend_html = """
        <div style="
            position: fixed;
            bottom: 10px; left: 20px; width: 170px; height: auto;
            background-color: white;
            border:1px solid grey;
            z-index:9999;
            font-size:12px;
            padding: 6px;
            border-radius: 6px;
        ">
            <b>Price Legend</b><br>
            <i style="background: #d9f5d1; width: 14px; height: 14px; display: inline-block;"></i> &lt;$20<br>
            <i style="background: #c3e2b7; width: 14px; height: 14px; display: inline-block;"></i> $20-$49<br>
            <i style="background: #86c77a; width: 14px; height: 14px; display: inline-block;"></i> $50-$99<br>
            <i style="background: #e2d0b3; width: 14px; height: 14px; display: inline-block;"></i> $100-$199<br>
            <i style="background: #e6a67b; width: 14px; height: 14px; display: inline-block;"></i> $200-$299<br>
            <i style="background: #b24949; width: 14px; height: 14px; display: inline-block;"></i> $300+<br>
        </div>
        """

        m.get_root().html.add_child(folium.Element(legend_html))

        return m

    m = create_map()
    map_html = m._repr_html_()

    map_col, b_col = st.columns([9, 1])
    with map_col:
        st.components.v1.html(map_html, height=850)

else:
    st.warning("Select your filters and click on Search to see the results")
