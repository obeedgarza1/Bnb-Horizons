import streamlit as st
import pandas as pd
import joblib
from backend.queries import (
    get_cities,
    get_neighbourhoods,
    get_room_types,
    get_loc,
    neigh_price_query,
)
import os
import plotly.graph_objects as go
import altair as alt


if "selected_city" not in st.session_state:
    st.session_state.selected_city = None
if "neighborhoods" not in st.session_state:
    st.session_state.neighborhoods = []
if "selected_neighbourhood" not in st.session_state:
    st.session_state.selected_neighbourhood = "All"

# City centers
city_centers = {
    "Barcelona": {"latitude": 41.3851, "longitude": 2.1734},
    "Euskadi": {"latitude": 42.9896, "longitude": -2.6189},
    "Girona": {"latitude": 41.9818, "longitude": 2.8237},
    "Madrid": {"latitude": 40.4168, "longitude": -3.7038},
    "Malaga": {"latitude": 36.7213, "longitude": -4.4213},
    "Mallorca": {"latitude": 39.6953, "longitude": 3.0176},
    "Menorca": {"latitude": 39.8895, "longitude": 4.2642},
    "Sevilla": {"latitude": 37.3891, "longitude": -5.9845},
    "Valencia": {"latitude": 39.4699, "longitude": -0.3763},
}

st.title("Host Earnings Dashboard")

st.write("")

st.markdown(
    """
    **Maximize Your Hosting Earnings!**  
    This dashboard helps you estimate your potential earnings as a host based on your property's location, type, 
    and other key factors. Select your city, neighborhood, property type, and other details to get insights into predicted prices, 
    occupancy rates, and monthly/yearly earnings. Explore seasonal trends and compare your listing's performance against 
    neighborhood averages. Start optimizing your hosting strategy today!
    """
)

st.divider()

cities = get_cities()

col1, col2, col3, col4, col5, col6, col7 = st.columns(7)

selected_city = col1.selectbox(
    "Select a City",
    options=cities,
    placeholder="Select an Option",
    key="selected_city",
)

if selected_city != st.session_state.selected_city:
    st.session_state.selected_city = selected_city
    st.session_state.neighborhoods = get_neighbourhoods(selected_city)

neighbourhoods = get_neighbourhoods(selected_city)
neighbourhood_names = [n[0] for n in neighbourhoods]

selected_neighbourhood = col6.selectbox(
    "Select Neighbourhood",
    placeholder="Choose an Option",
    index=None,
    options=neighbourhood_names,
)

selected_neighbourhood_id = next(
    (n[1] for n in neighbourhoods if n[0] == selected_neighbourhood), None
)

st.session_state.selected_neighbourhood = selected_neighbourhood

unique_room_types = get_room_types()
selected_room_type = col2.selectbox(
    "Select Property Type",
    placeholder="Choose an Option",
    index=None,
    options=list(unique_room_types),
)

selected_accommodates = col3.number_input("Accommodates?", 1, 50, 1)
selected_bathrooms = col4.number_input("Bathrooms?", 1, 50, 1)
selected_bedrooms = col5.number_input("Bedrooms?", 1, 50, 1)

superhost = col1.checkbox("Superhost?")
identity = col2.checkbox("Identity Verified?")
input_data = None

col7.write("")
if col7.button("Search"):
    if st.session_state.selected_city:
        latitude, longitude, distance = get_loc(
            st.session_state.selected_city,
            st.session_state.selected_neighbourhood,
            city_centers,
        )

        input_data = pd.DataFrame(
            [
                {
                    "city_name": st.session_state.selected_city,
                    "neighbourhood_id": selected_neighbourhood_id,
                    "room_type": selected_room_type,
                    "accommodates": selected_accommodates,
                    "bedrooms": selected_bedrooms,
                    "bathrooms": selected_bathrooms,
                    "latitude": latitude,
                    "longitude": longitude,
                    "host_is_superhost": int(superhost),
                    "host_identity_verified": int(identity),
                    "distance_to_center": distance,
                }
            ]
        )
if input_data is not None:
    if input_data.empty:
        st.warning("No listings found for the selected specs.")
        st.stop()

    seasons = ["Early Spring", "Early Autumn", "Early Summer", "Early Winter"]
    season_df = pd.DataFrame({"season": seasons})

    input_data = input_data.merge(season_df, how="cross")

    price_model = joblib.load(os.path.join("model", "price_model_xgb.pkl"))
    occupancy_model = joblib.load(os.path.join("model", "occupancy_model_xgb.pkl"))

    input_data_encoded = pd.get_dummies(
        input_data, columns=["city_name", "room_type", "season"], drop_first=True
    )

    for col in price_model.feature_names_in_:
        if col not in input_data_encoded.columns:
            input_data_encoded[col] = 0

    input_data_encoded = input_data_encoded[price_model.feature_names_in_]

    predicted_prices = price_model.predict(input_data_encoded)

    input_data["predicted_price"] = predicted_prices

    input_data_encoded["price_float"] = predicted_prices

    for col in occupancy_model.feature_names_in_:
        if col not in input_data_encoded.columns:
            input_data_encoded[col] = 0

    input_data_encoded = input_data_encoded[occupancy_model.feature_names_in_]

    predicted_occ_rate = occupancy_model.predict(input_data_encoded)

    input_data["predicted_occupancy_rate"] = predicted_occ_rate

    first, second = st.columns([5.2, 4.8])
    (
        avg_accommodates,
        avg_bedrooms,
        avg_bathrooms,
        lower_bound,
        upper_bound,
        data,
    ) = neigh_price_query(selected_neighbourhood)

    avg_price = input_data["predicted_price"].mean()
    avg_occupancy = (input_data["predicted_occupancy_rate"] * 100).mean()

    input_data["monthly_earnings"] = input_data["predicted_price"] * (
        input_data["predicted_occupancy_rate"] * 30
    )
    input_data["monthly_earnings"] = (input_data["monthly_earnings"] * 0.90).round(2)

    seasonal_earnings = (input_data["monthly_earnings"]).to_list()
    min_monthly_earnings = min(seasonal_earnings)
    max_monthly_earnings = max(seasonal_earnings)

    min_yearly_earnings = min_monthly_earnings * 12
    max_yearly_earnings = max_monthly_earnings * 12

    with first.container(border=True):
        st.subheader("📊 New Listing Insights")
        st.caption(
            "The following metrics are based on the predicted price and occupancy rate for the listing, the results depends on various factors like season, variablity of occypancy etc."
        )
        left, center = st.columns([2, 3])
        left.metric("💰 Predicted Price", f"${avg_price:.2f}")
        left.metric("📈 Predicted Occupancy Rate", f"{avg_occupancy:.2f}%")
        center.metric(
            "📅 Monthly Earnings",
            f"${min_monthly_earnings:,.2f} - ${max_monthly_earnings:,.2f}",
        )
        center.metric(
            "📆 Yearly Earnings",
            f"${min_yearly_earnings:,.2f} - ${max_yearly_earnings:,.2f}",
        )

    with second.container(border=True):
        st.subheader("🏠 Insights of the Neighborhood")
        left, center, right = st.columns(3)
        left.metric("🛏️ Avg Accommodates", f"{avg_accommodates:.1f}")
        center.metric("🛏️ Avg Bedrooms", f"{avg_bedrooms:.1f}")
        right.metric("🛁 Avg Bathrooms", f"{avg_bathrooms:.1f}")
        st.metric("💲 Price Range (95% CI)", f"${lower_bound:.2f} - ${upper_bound:.2f}")

    df = input_data[["season", "predicted_price", "predicted_occupancy_rate"]].copy()
    df["predicted_occupancy_rate"] *= 100

    uno, dos, tres = st.columns([3, 5, 3])

    base = alt.Chart(df).encode(
        x=alt.X("season:N", axis=alt.Axis(title=None, labelAngle=0))
    )

    bar = base.mark_bar(color="#003049").encode(
        y=alt.Y("predicted_price", axis=alt.Axis(title="Predicted Occupancy Rate")),
        tooltip="predicted_price",
    )

    text_labels = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-7,
        color="black",
        size=12,
    ).encode(
        y="predicted_price",
        text=alt.Text("predicted_price:Q", format=".2f"),
    )
    line = base.mark_line(color="#c1121f", interpolate="monotone", point=True).encode(
        y=alt.Y("predicted_occupancy_rate", axis=alt.Axis(title=None)),
        tooltip="predicted_occupancy_rate",
        color=alt.value("#c1121f"),
    )

    fig = (bar + text_labels + line).properties(
        height=400, title="Price and Occupancy Rate by Season"
    )

    fig = fig.encode(
        color=alt.Color(
            "series:N",
            scale=alt.Scale(
                domain=["Predicted Price", "Predicted Occupancy Rate"],
                range=["#003049", "#c1121f"],
            ),
            legend=alt.Legend(
                orient="top",
                title=None,
                labelFontSize=12,
                symbolSize=100,
            ),
        )
    ).transform_calculate(
        series="datum.predicted_price !== null ? 'Predicted Price' : 'Predicted Occupancy Rate'"
    )
    uno.write("")
    uno.altair_chart(fig, use_container_width=True)

    season_order = ["Early Spring", "Early Summer", "Early Autumn", "Early Winter"]
    input_data["season"] = pd.Categorical(
        input_data["season"], categories=season_order, ordered=True
    )

    area_chart = (
        alt.Chart(input_data)
        .mark_area(color="#003049", opacity=0.8, line=True, interpolate="monotone")
        .encode(
            x=alt.X("season:N", axis=alt.Axis(title=None, labelAngle=0)),
            y=alt.Y("monthly_earnings:Q", axis=None),
            tooltip=["season", "monthly_earnings"],
        )
    )

    text_labels = (
        alt.Chart(input_data)
        .mark_text(align="center", baseline="top", dy=-15, color="black", size=12)
        .encode(
            x="season",
            y="monthly_earnings",
            text=alt.Text("monthly_earnings:Q", format=".0f"),
        )
    )

    chart = (area_chart + text_labels).properties(
        height=400, title="Monthly Earnings by Season"
    )
    dos.write("")
    dos.altair_chart(chart, use_container_width=True)

    fig2 = go.Figure()

    fig2.add_trace(
        go.Box(
            y=data["price_float"],
            name="Neighborhood Price Range",
            marker_color="#003049",
        )
    )

    fig2.add_trace(
        go.Scatter(
            y=input_data["predicted_price"],
            x=["Your Listing"] * len(input_data["predicted_price"]),
            mode="markers",
            marker=dict(size=10, color="#c1121f"),
            name="Your Price",
        )
    )

    fig2.update_layout(
        title="Price vs. Neighborhood Average",
        yaxis_title="Price ($)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
        ),
    )

    tres.plotly_chart(fig2, use_container_width=True, height=500)
else:
    st.warning("Select your filters and click on Search to see the results")
