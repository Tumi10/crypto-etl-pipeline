import requests
import json
import time
import pandas as pd
from datetime import datetime,timezone
import os
import psycopg2



# ─── API Fetching ─────────────────────────────────────────────────────────────
"""
this function is thelowest level worker . it takes one coin Id,
makes two api calls - metadata and market history - handles rate limiting if 
CoinGecko pushes back due to their api limit when we hit the api frequently ,and
saves both responses as json files to disk .It does one coin at a time and nothing else

"""
def fetch_coin_data(coin_id):
    urls = {
        "metadata": f"https://api.coingecko.com/api/v3/coins/{coin_id}",
        "market_chart": f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=365"
    }

    results = {}
    for key, url in urls.items():
        response = requests.get(url)
        if response.status_code == 429:
            print(f"Rate limit hit for {coin_id} ({key}). Retrying in 60 seconds...")
            time.sleep(60)
            response = requests.get(url)

        if response.status_code == 200:
            results[key] = response.json()
        else:
            print(f"Failed to fetch {key} for {coin_id}. Status: {response.status_code}")

        time.sleep(5)

    if "metadata" in results and "market_chart" in results:
        with open(f"/opt/airflow/dags/{coin_id}_metadata.json", "w") as f:
            json.dump(results["metadata"], f)
        with open(f"/opt/airflow/dags/{coin_id}_market_chart.json", "w") as f:
            json.dump(results["market_chart"], f)
    return results
"""
this is the controller for extraction . it calls the markets endpoint to get the top 5 coins
by market cap , then loops through them calling the fetchcoindata function on each one
. that is the first task of airflow

"""

def fetch_top_coins():
    url_list = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1"
    response = requests.get(url_list)
    if response.status_code == 200:
        coins = response.json()
        print(f"Fetched {len(coins)} coins from CoinGecko.")
        for coin in coins:
            coin_id = coin["id"]
            print(f"Fetching data for {coin_id}...")
            fetch_coin_data(coin_id)
    else:
        print(f"Failed to fetch coin list. Status: {response.status_code}")


# ─── Dimension: DimCoin ───────────────────────────────────────────────────────

def build_dim_coin(metadata):
    """
    One row per coin. Built from the /coins/{id} metadata endpoint.
    Captures static, descriptive info — never changes day to day.
    """
    categories = ", ".join(metadata.get("categories", []))
    platforms  = ", ".join(metadata.get("platforms", {}).keys())

    return pd.DataFrame([{
        "coin_id":      metadata["id"],       # PK — matches CoinGecko's own ID (e.g. "bitcoin")
        "name":         metadata["name"],
        "symbol":       metadata["symbol"],
        "category":     categories,
        "genesis_date": metadata.get("genesis_date"),
        "platform":     platforms,
    }])


# ─── Dimension: DimDate ───────────────────────────────────────────────────────

def build_dim_date(market_chart_data):
    """
    One row per unique date. Built from the timestamps inside market_chart.
    Pre-computes time attributes so queries like 'GROUP BY quarter' are trivial.

    Timestamps from CoinGecko are in milliseconds since Unix epoch,
    so we divide by 1000 before passing to utcfromtimestamp().
    """
    timestamps = [entry[0] for entry in market_chart_data["prices"]]
    dates = [datetime.fromtimestamp(ts / 1000,tz=timezone.utc) for ts in timestamps]

    dim_date = pd.DataFrame({
        "date_id": [d.strftime("%Y%m%d") for d in dates],  # PK — e.g. "20240315", sorts chronologically
        "date":    [d.date() for d in dates],
        "day":     [d.day for d in dates],
        "month":   [d.month for d in dates],
        "quarter": [(d.month - 1) // 3 + 1 for d in dates],
        "year":    [d.year for d in dates],
    }).drop_duplicates("date_id")

    return dim_date


# ─── Fact: FactMarket ─────────────────────────────────────────────────────────

def build_fact_market(coin_id, market_chart_data):
    """
    One row per coin per day — this is the grain.

    market_chart returns three parallel lists, all aligned by index:
      prices        -> [[timestamp, price], ...]
      market_caps   -> [[timestamp, market_cap], ...]
      total_volumes -> [[timestamp, volume], ...]

    zip() walks all three simultaneously so we can build one complete row
    per timestamp without needing nested loops or separate joins.

    The _ discards the duplicate timestamps from market_caps and total_volumes
    since we only need it once (from prices).
    """
    prices  = market_chart_data["prices"]
    mcaps   = market_chart_data["market_caps"]
    volumes = market_chart_data["total_volumes"]

    rows = []
    for (ts, price), (_, mcap), (_, vol) in zip(prices, mcaps, volumes):
        date = datetime.fromtimestamp(ts / 1000,tz=timezone.utc)
        rows.append({
            "coin_id":          coin_id,                  # FK -> DimCoin
            "date_id":          date.strftime("%Y%m%d"),  # FK -> DimDate
            "price_usd":        price,
            "market_cap_usd":   mcap,
            "total_volume_usd": vol,
        })

    return pd.DataFrame(rows)


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def build_star_schema(coin_id, results):
    """
    Takes the raw API results for one coin and returns the three schema tables.
    """
    dim_coin    = build_dim_coin(results["metadata"])
    dim_date    = build_dim_date(results["market_chart"])
    fact_market = build_fact_market(coin_id, results["market_chart"])
    return dim_coin, dim_date, fact_market


def transform_pipeline():
    """
    Reads the JSON files that extract_coins already saved to dags/.
    Builds the star schema tables and writes them to CSV.
    Does NOT hit the CoinGecko API — that already happened in first_task.
    """
    first_coin = True

    for filename in os.listdir("/opt/airflow/dags/"):
        if not filename.endswith("_metadata.json"):
            continue

        coin_id = filename.replace("_metadata.json", "")

        with open(f"/opt/airflow/dags/{coin_id}_metadata.json") as f:
            metadata = json.load(f)
        with open(f"/opt/airflow/dags//{coin_id}_market_chart.json") as f:
            market_chart = json.load(f)

        dim_coin, dim_date, fact_market = build_star_schema(coin_id, {
            "metadata":     metadata,
            "market_chart": market_chart
        })

        # Write header only on the first coin, append the rest
        dim_coin.to_csv("/opt/airflow/dags/dim_coin.csv",       mode="w" if first_coin else "a", header=first_coin, index=False)
        dim_date.to_csv("/opt/airflow/dags/dim_date.csv",       mode="w" if first_coin else "a", header=first_coin, index=False)
        fact_market.to_csv("/opt/airflow/dags/fact_market.csv", mode="w" if first_coin else "a", header=first_coin, index=False)



        print(f"Transformed {coin_id} — {len(fact_market)} daily rows")
        first_coin = False


def load_to_postgres():
    conn = psycopg2.connect(
        host="postgres",
        database="crypto_warehouse",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()

    dim_coin    = pd.read_csv("/opt/airflow/dags/dim_coin.csv")
    dim_date    = pd.read_csv("/opt/airflow/dags/dim_date.csv")
    fact_market = pd.read_csv("/opt/airflow/dags/fact_market.csv")

    # replace NaN with None so PostgreSQL receives NULL instead of NaN
    dim_coin = dim_coin.where(pd.notnull(dim_coin), None)
    dim_date = dim_date.where(pd.notnull(dim_date), None)
    fact_market = fact_market.where(pd.notnull(fact_market), None)

    for _, row in dim_coin.iterrows():
        cursor.execute("""
            INSERT INTO dim_coin (coin_id, name, symbol, category, genesis_date, platform)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (coin_id) DO NOTHING
        """, tuple(row))

    for _, row in dim_date.iterrows():
        cursor.execute("""
            INSERT INTO dim_date (date_id, date, day, month, quarter, year)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING
        """, tuple(row))

    for _, row in fact_market.iterrows():
        cursor.execute("""
            INSERT INTO fact_market (coin_id, date_id, price_usd, market_cap_usd, total_volume_usd)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (coin_id, date_id) DO UPDATE
                SET price_usd        = EXCLUDED.price_usd,
                    market_cap_usd   = EXCLUDED.market_cap_usd,
                    total_volume_usd = EXCLUDED.total_volume_usd
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully into crypto_warehouse.")