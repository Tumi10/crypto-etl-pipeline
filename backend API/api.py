from fastapi import FastAPI, HTTPException
import psycopg2
import psycopg2.extras
from typing import Optional

app = FastAPI(
    title="Crypto Warehouse API",
    description="Analytics endpoints for CoinGecko ETL data",
    version="1.0.0"
)

# ─── Database Connection ───────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host="crypto-pg-tumel-01.postgres.database.azure.com",
        port=5432,
        database="crypto_warehouse",
        user="airflow",
        password="airflow123!",
        sslmode="require"
    )

# ─── Helper ───────────────────────────────────────────────────────────────────

def query(sql, params=None):
    """
    Runs a SQL query and returns results as a list of dictionaries.
    RealDictCursor means each row comes back as a dict with column names
    as keys rather than a plain tuple — much easier to work with.
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results

# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/coins")
def get_all_coins():
    """
    Returns all coins in the warehouse with their descriptive info.
    Pulls from DimCoin.
    """
    results = query("SELECT * FROM dim_coin ORDER BY coin_id")
    return {"coins": results, "count": len(results)}


@app.get("/coins/{coin_id}")
def get_coin(coin_id: str):
    """
    Returns details for one specific coin.
    Raises 404 if the coin doesn't exist in the warehouse.
    """
    results = query(
        "SELECT * FROM dim_coin WHERE coin_id = %s",
        (coin_id,)
    )
    if not results:
        raise HTTPException(status_code=404, detail=f"Coin '{coin_id}' not found")
    return results[0]


@app.get("/coins/{coin_id}/prices")
def get_coin_prices(coin_id: str, limit: Optional[int] = None):
    """
    Returns full price history for a coin ordered by date.
    Optional limit parameter — e.g. /coins/bitcoin/prices?limit=30
    returns only the last 30 days.
    """
    sql = """
        SELECT f.date_id, d.date, d.month, d.quarter, d.year,
               f.price_usd, f.market_cap_usd, f.total_volume_usd
        FROM fact_market f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE f.coin_id = %s
        ORDER BY d.date DESC
    """
    if limit:
        sql += f" LIMIT {limit}"

    results = query(sql, (coin_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"No price data found for '{coin_id}'")
    return {"coin_id": coin_id, "count": len(results), "prices": results}


@app.get("/coins/{coin_id}/prices/latest")
def get_latest_price(coin_id: str):
    """
    Returns only the most recent price entry for a coin.
    """
    results = query("""
        SELECT f.date_id, d.date, f.price_usd, f.market_cap_usd, f.total_volume_usd
        FROM fact_market f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE f.coin_id = %s
        ORDER BY d.date DESC
        LIMIT 1
    """, (coin_id,))
    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for '{coin_id}'")
    return results[0]


@app.get("/analytics/top-by-volume")
def top_by_volume(limit: int = 5):
    """
    Returns coins ranked by their most recent total volume.
    Default top 5, configurable via ?limit=10
    """
    results = query("""
        SELECT f.coin_id, c.name, c.symbol,
               f.total_volume_usd, f.price_usd, d.date
        FROM fact_market f
        JOIN dim_coin c ON f.coin_id = c.coin_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.date = (SELECT MAX(date) FROM dim_date)
        ORDER BY f.total_volume_usd DESC
        LIMIT %s
    """, (limit,))
    return {"ranked_by": "volume", "results": results}


@app.get("/analytics/top-by-marketcap")
def top_by_marketcap(limit: int = 5):
    """
    Returns coins ranked by their most recent market cap.
    """
    results = query("""
        SELECT f.coin_id, c.name, c.symbol,
               f.market_cap_usd, f.price_usd, d.date
        FROM fact_market f
        JOIN dim_coin c ON f.coin_id = c.coin_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.date = (SELECT MAX(date) FROM dim_date)
        ORDER BY f.market_cap_usd DESC
        LIMIT %s
    """, (limit,))
    return {"ranked_by": "market_cap", "results": results}


@app.get("/analytics/summary")
def summary():
    """
    Returns a high level overview — latest price, market cap and volume
    for every coin in the warehouse side by side.
    """
    results = query("""
        SELECT f.coin_id, c.name, c.symbol, c.category,
               f.price_usd, f.market_cap_usd, f.total_volume_usd, d.date
        FROM fact_market f
        JOIN dim_coin c ON f.coin_id = c.coin_id
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE d.date = (SELECT MAX(date) FROM dim_date)
        ORDER BY f.market_cap_usd DESC
    """)
    return {"summary": results, "as_of": results[0]["date"] if results else None}


@app.get("/analytics/price-range/{coin_id}")
def price_range(coin_id: str, year: Optional[int] = None, quarter: Optional[int] = None):
    """
    Returns min, max and average price for a coin.
    Can filter by year and/or quarter.
    e.g. /analytics/price-range/bitcoin?year=2024&quarter=3
    """
    conditions = ["f.coin_id = %s"]
    params = [coin_id]

    if year:
        conditions.append("d.year = %s")
        params.append(year)
    if quarter:
        conditions.append("d.quarter = %s")
        params.append(quarter)

    where = " AND ".join(conditions)

    results = query(f"""
        SELECT f.coin_id,
               MIN(f.price_usd)  as min_price,
               MAX(f.price_usd)  as max_price,
               AVG(f.price_usd)  as avg_price,
               COUNT(*)          as trading_days
        FROM fact_market f
        JOIN dim_date d ON f.date_id = d.date_id
        WHERE {where}
        GROUP BY f.coin_id
    """, params)

    if not results:
        raise HTTPException(status_code=404, detail=f"No data found for '{coin_id}'")
    return results[0]