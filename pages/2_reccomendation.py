import streamlit as st
import pandas as pd
from backend.queries import (
    get_cities,
    get_neighbourhoods,
    get_room_types,
    reccomendation_query,
    price_ranges,
    get_seasons,
)
import json

if "selected_city" not in st.session_state:
    st.session_state.selected_city = None
if "neighborhoods" not in st.session_state:
    st.session_state.neighborhoods = []
if "selected_neighbourhood" not in st.session_state:
    st.session_state.selected_neighbourhood = "All"

st.title("Listing Reccomendator")
st.write("")

st.markdown(
    """
    **Find Your Perfect Stay!**  
    Use this tool to discover the best accommodations tailored to your preferences. Select your desired city, 
    neighborhood, season, property type, and other filters to find listings that match your needs. Explore detailed 
    information about each property, including pricing, amenities, host details, and location on the map. 
    Start your search now to find the ideal place for your next stay!
    """
)

st.divider()

cities = get_cities()

col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(
    [1, 1, 1, 1, 0.5, 0.5, 1, 1]
)

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

selected_neighbourhood = col2.selectbox(
    "Select Neighbourhood",
    placeholder="Choose an Option",
    index=None,
    options=["All"] + neighbourhood_names,
)

st.session_state.selected_neighbourhood = selected_neighbourhood

seasons = get_seasons()
selected_season = col3.selectbox(
    "Select Season",
    placeholder="Choose an Option",
    index=None,
    options=seasons,
)
unique_room_types = get_room_types()
selected_room_type = col4.selectbox(
    "Select Property Type",
    placeholder="Choose an Option",
    index=None,
    options=list(unique_room_types),
)

selected_accommodates = col5.number_input("Guests?", 1, 50, 1)
selected_nights = col6.number_input("Nights?", 1, 30, 1)

price_range = price_ranges()
selected_price_range = col7.selectbox(
    "Select a Price Range",
    placeholder="Choose an Option",
    index=None,
    options=price_range,
)

where_clauses = []
if selected_city != "Select a city":
    where_clauses.append(f"city_name = '{selected_city}'")
if selected_neighbourhood != "All":
    where_clauses.append(f"neighbourhood = '{selected_neighbourhood}'")
if selected_season:
    where_clauses.append(f"season = '{selected_season}'")
if selected_room_type != "All":
    where_clauses.append(f"room_type = '{selected_room_type}'")
if selected_accommodates:
    where_clauses.append(f"accommodates = {selected_accommodates}")
if selected_nights:
    where_clauses.append(f"{selected_nights} BETWEEN minimum_nights AND maximum_nights")
if selected_price_range:
    where_clauses.append(f"price_range = '{selected_price_range}'")


where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

reccomendation_df = None
if "rec_df" not in st.session_state:
    st.session_state.rec_df = None
if "index" not in st.session_state:
    st.session_state.index = 0

with col8:
    st.write("")
    if st.button("Search"):

        st.session_state.rec_df = reccomendation_query(where_clause)
        st.session_state.index = 0
        for col in st.session_state.rec_df.select_dtypes(include=["object"]).columns:
            st.session_state.rec_df[col] = st.session_state.rec_df[col].astype(str)

        for col in st.session_state.rec_df.select_dtypes(
            include=["int", "float"]
        ).columns:
            st.session_state.rec_df[col] = pd.to_numeric(
                st.session_state.rec_df[col], errors="coerce"
            )

if st.session_state.rec_df is not None:
    if st.session_state.rec_df.empty:
        st.warning("No listings found for the selected specs.")
        st.stop()

    sort_by = st.radio(
        "Sort by:", ["Price (Lower to Higher)", "Rating (Higher to Lower)"]
    )
    if sort_by == "Price (Lower to Higher)":
        st.session_state.rec_df = st.session_state.rec_df.sort_values(
            by="price_float", ascending=True
        )
    else:
        st.session_state.rec_df = st.session_state.rec_df.sort_values(
            by="review_scores_rating", ascending=False
        )

    if "index" not in st.session_state:
        st.session_state.index = 0

    def next_listing():
        if st.session_state.index < len(st.session_state.rec_df) - 1:
            st.session_state.index += 1

    def prev_listing():
        if st.session_state.index > 0:
            st.session_state.index -= 1

    current_listing = st.session_state.rec_df.iloc[st.session_state.index]

    base_price = current_listing["price_float"] * selected_nights
    service_fee_amount = base_price * 0.15
    total_price = base_price + service_fee_amount

    try:
        amenities_str = current_listing["categorized_amenities"].replace("'", '"')
        amenities_dict = json.loads(amenities_str)
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse amenities: {e}")
        amenities_dict = {}

    latitude = current_listing["latitude"]
    longitude = current_listing["longitude"]

    location_data = pd.DataFrame({"lat": [latitude], "lon": [longitude]})

    st.subheader(current_listing["name"])
    photo, map = st.columns([2, 3])
    with photo:
        st.image(current_listing["picture_url"], use_container_width=True)
    with map:
        st.map(location_data, zoom=13, size=(100, 100), use_container_width=True)
    st.markdown("### 🏡 Property Details")

    ll, cc, rr, blank = st.columns([1, 1, 1, 4])
    st.write(
        f"Property: {current_listing['room_type']} in {current_listing['neighbourhood']}"
    )
    ll.write(f"**Guests:** {current_listing['accommodates']}")
    cc.write(f"**Bedrooms:** {current_listing['bedrooms']}")
    rr.write(f"**Bathrooms:** {current_listing['bathrooms']}")
    st.write(f"⭐ **Rating:** {current_listing['review_scores_rating']}")

    l, c, r = st.columns([2, 0.5, 3])
    l.markdown("### 👤 Host Information")
    with r:
        st.markdown("### 💰 Booking Summary")
        with st.container(border=True):
            st.markdown(
                f"""
                - **Price per Night:** 🏷️ ${current_listing['price_float']}
                - **Total Nights Booked:** 📅 {selected_nights}
                - **Service Fee:** 💸 ${service_fee_amount:.2f}
                - **Total Price:** 💵 **${total_price:.2f}**
                """,
                unsafe_allow_html=True,
            )
    l.write(f"**Hosted by:** {current_listing['host_name']}")
    l.write(f"📝 {current_listing['host_about']}")

    l.markdown("### 📌 Description")
    l.write(current_listing["description"])

    l.markdown("### 🏠 Amenities")

    categories = list(amenities_dict.items())
    num_columns = 4
    rows = (len(categories) + num_columns - 1) // num_columns

    for row in range(rows):
        cols = st.columns(num_columns)
        for col in range(num_columns):
            index = row * num_columns + col
            if index < len(categories):
                category, items = categories[index]
                with cols[col]:
                    with st.expander(f"**{category}**"):
                        for item in items:
                            st.write(f"- {item}")

    col1, col2 = st.columns([1, 1])
    with col1:
        st.button(
            "⬅️ Previous", on_click=prev_listing, disabled=st.session_state.index == 0
        )
    with col2:
        st.button(
            "Next ➡️",
            on_click=next_listing,
            disabled=st.session_state.index == len(st.session_state.rec_df) - 1,
        )
    st.write(f"Listing {st.session_state.index + 1} of {len(st.session_state.rec_df)}")
