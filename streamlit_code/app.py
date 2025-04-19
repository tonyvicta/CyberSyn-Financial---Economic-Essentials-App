# Import necessary libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when, max, lag
from snowflake.snowpark.window import Window
import streamlit as st
import altair as alt
import pandas as pd
from datetime import timedelta

# Set Streamlit page configuration
st.set_page_config(layout="wide")

# Get active Snowflake session
session = get_active_session()

# Cache the data loading process
@st.cache_data
def load_data():
    # Load and filter daily stock price data
    stock_df = (
        session.table("FINANCE_ECONOMICS.PUBLIC.STOCK_PRICE_TIMESERIES")
        .filter(
            (col("TICKER").isin("AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA")) &
            (col("VARIABLE_NAME").isin("Nasdaq Volume", "Post-Market Close"))
        )
        .group_by("TICKER", "DATE")
        .agg(
            max(when(col("VARIABLE_NAME") == "Nasdaq Volume", col("VALUE"))).alias("NASDAQ_VOLUME"),
            max(when(col("VARIABLE_NAME") == "Post-Market Close", col("VALUE"))).alias("POSTMARKET_CLOSE")
        )
    )

    # Calculate day-over-day percentage change in post-market close
    window_spec = Window.partition_by("TICKER").order_by("DATE")
    stock_df = stock_df.with_column(
        "DAY_OVER_DAY_CHANGE",
        (col("POSTMARKET_CLOSE") - lag("POSTMARKET_CLOSE", 1).over(window_spec)) /
        lag("POSTMARKET_CLOSE", 1).over(window_spec)
    )

    # Load and filter FX rates data
    fx_df = (
        session.table("FINANCE_ECONOMICS.PUBLIC.FX_RATES_TIMESERIES")
        .filter((col("BASE_CURRENCY_ID") == "EUR") & (col("DATE") >= "2019-01-01"))
        .rename("VARIABLE_NAME", "EXCHANGE_RATE")
    )

    return stock_df.to_pandas(), fx_df.to_pandas()

# Load the data
df_stocks, df_fx = load_data()


# -------------------------
# Stock Price Visualisation
# -------------------------
def stock_prices():
    st.subheader("Stock Performance on the Nasdaq for the Magnificent 7")

    # Convert DATE column to datetime
    df_stocks["DATE"] = pd.to_datetime(df_stocks["DATE"])
    min_date = df_stocks["DATE"].min()
    max_date = df_stocks["DATE"].max()

    # Default to last 30 days of data
    default_start = max_date - timedelta(days=30)
    
    # Date input widget
    start_date, end_date = st.date_input(
        "Date range:",
        [default_start, max_date],
        min_value=min_date,
        max_value=max_date,
        key="date_range"
    )

    # Filter by selected date range
    df_filtered = df_stocks[
        (df_stocks["DATE"] >= pd.to_datetime(start_date)) &
        (df_stocks["DATE"] <= pd.to_datetime(end_date))
    ]

    # Ticker filter
    available_tickers = df_filtered["TICKER"].unique().tolist()
    default_tickers = [t for t in ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA", "NVDA"] if t in available_tickers]

    selected_tickers = st.multiselect(
        "Select Ticker(s):",
        options=available_tickers,
        default=default_tickers
    )

    df_filtered = df_filtered[df_filtered["TICKER"].isin(selected_tickers)]

    # Metric selection
    metric = st.selectbox(
        "Metric:",
        options=["DAY_OVER_DAY_CHANGE", "POSTMARKET_CLOSE", "NASDAQ_VOLUME"],
        index=0
    )

    # Line chart
    chart = alt.Chart(df_filtered).mark_line().encode(
        x=alt.X("DATE:T", title="Date"),
        y=alt.Y(metric, title=metric.replace("_", " ").title()),
        color="TICKER",
        tooltip=["TICKER", "DATE", metric]
    ).interactive()

    st.altair_chart(chart, use_container_width=True)


# -------------------------
# FX Rates Visualisation
# -------------------------
def fx_rates():
    st.subheader("EUR Exchange (FX) Rates by Currency Over Time")

    # Define available currencies
    currencies = [
        "British Pound Sterling",
        "Canadian Dollar",
        "United States Dollar",
        "Japanese Yen",
        "Polish Zloty",
        "Turkish Lira",
        "Swiss Franc"
    ]

    # Multiselect currency filter
    selected_currencies = st.multiselect(
        "Select Currencies:",
        options=currencies,
        default=[
            "British Pound Sterling",
            "Canadian Dollar",
            "United States Dollar",
            "Swiss Franc",
            "Polish Zloty"
        ]
    )

    st.markdown("___")

    # Filter FX data based on selection
    currencies_to_plot = selected_currencies or currencies
    df_filtered = df_fx[df_fx["QUOTE_CURRENCY_NAME"].isin(currencies_to_plot)]

    # Altair line chart
    chart = alt.Chart(df_filtered).mark_line().encode(
        x=alt.X("DATE:T", title="Date"),
        y=alt.Y("VALUE", title="Exchange Rate (EUR → ...)"),
        color="QUOTE_CURRENCY_NAME",
        tooltip=["QUOTE_CURRENCY_NAME", "DATE", "VALUE"]
    ).interactive()

    st.altair_chart(chart, use_container_width=True)


# -------------------------
# App Navigation
# -------------------------
st.header("Finance & Economics Dashboard")
st.sidebar.title("Navigation")

page_options = {
    "Daily Stock Performance": stock_prices,
    "Exchange (FX) Rates": fx_rates
}

selected_page = st.sidebar.selectbox("Select a Page", page_options.keys())

# Render selected page
page_options[selected_page]()
