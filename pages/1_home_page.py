import streamlit as st
import random

col1, col2, col3 = st.columns([1, 5, 1])

col2.markdown(
    "<h1 style='text-align: center; color:#ef233c;'> 🌄 - Welcome to BnB Horizons - 🏡</h1>",
    unsafe_allow_html=True,
)

col2.markdown(
    """
    <h3 style='text-align: center; color: #333;'> helps travelers find top-quality stays and aspiring hosts discover the best locations to start their Airbnb journey. Explore amenities, compare price-quality ratios, and make smarter decisions effortlessly!. </h3>
    """,
    unsafe_allow_html=True,
)

col2.markdown("<hr style='border-top: 3px solid #bbb;'>", unsafe_allow_html=True)

col2.markdown("### About This App...")

col2.write(
    """
           Is the ultimate guide to discovering the best Airbnb experiences for both guests and hosts. 
           Whether you're a traveler seeking cozy stays with top-tier amenities and great price-to-quality value, 
           or an aspiring host looking for data-driven insights to start your Airbnb business in the best locations, 
           this app has you covered. Explore, compare, and plan your next move with ease!
           """
)

col2.markdown(
    """
    ### Key Features
    - 📊 **Data-driven insights:** Make informed decisions with price-quality analysis.
    - 🏠 **For hosts:** Identify high-potential Airbnb locations.
    - 🏠 **For users:** Identify best neighbourhoods for the price.
    - 🔍 **Adjusted needs:** Search for your preferences, number of beds, certain amenities etc, the election is yours.
    """
)

facts = [
    "Did you know? Airbnb has listings in over 220 countries and regions!",
    "A great photo can increase bookings by 20%.",
    "Superhosts earn 22% more on average.",
    "The busiest check-in day on Airbnb is Friday.",
]

col2.success(random.choice(facts))
