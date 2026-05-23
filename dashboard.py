import streamlit as st
import requests
import pandas as pd

import os


# 1. fetch data at the top before anything renders
FASTAPI_URL = os.getenv("FASTAPI_URL", "http://localhost:8000")
response = requests.get(f"{FASTAPI_URL}/analytics/summary")
data = response.json()
dataFrame = pd.DataFrame(data["summary"])
dataFrame = dataFrame.set_index("coin_id")

# 2. sidebar
with st.sidebar:
    st.header("Filters")
    coin_id = st.selectbox("Select a coin", dataFrame.index)

# 3. title
st.title("Crypto Dashboard")

# 4. metric cards
st.subheader("Current Prices")
cols = st.columns(len(dataFrame))
for col, (coin_id_label, row) in zip(cols, dataFrame.iterrows()):
    with col:
        st.metric(
            label=row["name"],
            value=f"${row['price_usd']:,.2f}"
        )

# 5. two charts side by side
left, right = st.columns(2)
with left:
    st.subheader("Market Cap Comparison")
    st.bar_chart(dataFrame["market_cap_usd"])
with right:
    st.subheader("Volume Comparison")
    st.bar_chart(dataFrame["total_volume_usd"])

# 6. price history line chart
st.subheader("Price History")
response2 = requests.get(f"{FASTAPI_URL}/coins/{coin_id}/prices")
data2 = response2.json()
dataframe2 = pd.DataFrame(data2["prices"])
dataframe2 = dataframe2.set_index("date")
st.line_chart(dataframe2["price_usd"])

# 7. raw data table at the bottom
st.subheader("Raw Data")
st.dataframe(dataFrame)