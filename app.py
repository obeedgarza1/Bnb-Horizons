import streamlit as st
import sys
import os

sys.path.append(os.path.abspath("backend"))

st.set_page_config(
    page_title="BnB Horizons",
    page_icon="🏡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# page_one = st.Page(page="pages/1_home_page.py", title="🏠Home Page")
# page_two = st.Page(page="pages/2_hotspots.py", title="📍Hotspots Map")

# pages = st.navigation(pages=([page_one, page_two]), expanded=True)
# pages.run()

st.sidebar.markdown(
    """
    ### 🌍 Fun Fact
    Airbnb’s first listing was a room with three air mattresses.
    """
)

home_page = st.Page(
    page="pages/1_home_page.py",
    title="Home Page",
    icon=":material/home_app_logo:",
    default=True,
)

reccomendations = st.Page(
    page="pages/2_reccomendation.py",
    title="Listing Finder",
    icon=":material/trending_up:",  # Changed to a relevant icon
)

earnings = st.Page(
    page="pages/3_earnings.py",
    title="Earnings Estimator",
    icon=":material/monetization_on:",
)

hotspots = st.Page(
    page="pages/4_hotspots.py",
    title="Raw Map",
    icon=":material/location_on:",
)

pg = st.navigation(
    {
        "About": [home_page],  # Only Home Page under About
        "Analysis": [reccomendations, earnings, hotspots],  # Grouped under Analysis
    },
)

st.sidebar.markdown("Made with ❤️ by Me")

pg.run()
