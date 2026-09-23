"""
Daily job: fetch EOD bars + market cap + news flag, upsert into Supabase.

Tables written: screener_price_bars, screener_fundamentals
Env vars:       SUPABASE_URL, SUPABASE_SERVICE_KEY (secret!), FINNHUB_API_KEY
"""
import datetime as dt
import math
import os
import sys
import time
from pathlib import Path

import requests
import yfinance as yf

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")

BARS_KEPT = 260          # newest bars stored per symbol (matches the frontend)
HISTORY_DAYS = 400       # calendar days requested (~275 trading days)
YF_CHUNK = 100           # symbols per Yahoo download
UPSERT_BATCH = 1000      # rows per Supabase request
NEWS_LOOKBACK_DAYS = 3   # same as the old SCREENER_NEWS_LOOKBACK_DAYS
FINNHUB_DELAY = 1.1      # seconds between calls (free tier = 60/min)

HEADERS = {
    "apikey": SERVICE_KEY,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal",
}
if SERVICE_KEY.startswith("eyJ"):  # legacy JWT-style key also goes in Authorization
    HEADERS["Authorization"] = f"Bearer {SERVICE_KEY}"


def load_symbols():
    path = Path(__file__).resolve().parent.parent / "symbols.txt"
    out, seen = [], set()
    for line in path.read_text().splitlines():
        s = line.strip().upper()
        if s and not s.startswith("#") and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def upsert(table, rows, conflict):
    for i in range(0, len(rows), UPSERT_BATCH):
        batch = rows[i:i + UPSERT_BATCH]
        for attempt in range(3):
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={conflict}",
                headers=HEADERS, json=batch, timeout=60,
            )
            if r.status_code < 300:
                break
            if attempt == 2:
                raise RuntimeError(f"{table} upsert failed: {r.status_code} {r.text[:300]}")
            time.sleep(2 * (attempt + 1))


def num(x):
    try:
        f = float(x)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def fetch_bars(symbols):
    """Returns (rows, loaded_symbol_count). Split-adjusted OHLC, real (unadjusted-for-dividends) prices."""
    start = (dt.date.today() - dt.timedelta(days=HISTORY_DAYS)).isoformat()
    rows, loaded = [], 0
    for i in range(0, len(symbols), YF_CHUNK):
        chunk = symbols[i:i + YF_CHUNK]
        try:
            df = yf.download(chunk, start=start, interval="1d", auto_adjust=False,
                             group_by="ticker", threads=True, progress=False)
        except Exception as e:  # noqa: BLE001
            print(f"  download failed for chunk {i}: {e}")
            continue
        for sym in chunk:
            try:
                sub = df[sym] if len(chunk) > 1 else df
                sub = sub.dropna(subset=["Open", "High", "Low", "Close"]).tail(BARS_KEPT)
            except KeyError:
                print(f"  no data: {sym}")
                continue
            if len(sub) < 30:
                print(f"  too little history: {sym} ({len(sub)} bars)")
                continue
            loaded += 1
            for idx, r in sub.iterrows():
                if not (num(r["Close"]) and r["Close"] > 0):
                    continue
                rows.append({
                    "symbol": sym,
                    "date": idx.strftime("%Y-%m-%d"),
                    "open": num(r["Open"]), "high": num(r["High"]),
                    "low": num(r["Low"]), "close": num(r["Close"]),
                    "volume": int(r["Volume"]) if num(r["Volume"]) is not None else 0,
                })
        print(f"bars: {min(i + YF_CHUNK, len(symbols))}/{len(symbols)} symbols downloaded")
    return rows, loaded


def finnhub_get(path, params):
    """One throttled Finnhub call; retries once on 429. Returns parsed JSON or None."""
    params = {**params, "token": FINNHUB_KEY}
    for attempt in range(2):
        time.sleep(FINNHUB_DELAY)
        try:
            r = requests.get(f"https://finnhub.io/api/v1/{path}", params=params, timeout=30)
        except requests.RequestException:
            return None
        if r.status_code == 429 and attempt == 0:
            time.sleep(30)
            continue
        return r.json() if r.status_code == 200 else None
    return None


def fetch_fundamentals(symbols):
    if not FINNHUB_KEY:
        print("No FINNHUB_API_KEY set - skipping market cap / news.")
        return []
    today = dt.date.today()
    frm = (today - dt.timedelta(days=NEWS_LOOKBACK_DAYS)).isoformat()
    rows = []
    for n, sym in enumerate(symbols, 1):
        profile = finnhub_get("stock/profile2", {"symbol": sym})
        news = finnhub_get("company-news", {"symbol": sym, "from": frm, "to": today.isoformat()})
        if profile is None or not isinstance(news, list):
            print(f"  fundamentals skipped (API error): {sym}")
            continue
        mc = num(profile.get("marketCapitalization"))
        rows.append({
            "symbol": sym,
            "market_cap": mc if mc and mc > 0 else None,   # $ millions
            "has_news": len(news) > 0,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        })
        if n % 50 == 0:
            print(f"fundamentals: {n}/{len(symbols)}")
    return rows


def main():
    symbols = load_symbols()
    print(f"{len(symbols)} symbols")
    print("key starts with:", SERVICE_KEY[:10])
    
    bars, loaded = fetch_bars(symbols)
    print(f"{len(bars)} bar rows for {loaded} symbols")
    upsert("screener_price_bars", bars, "symbol,date")

    funds = fetch_fundamentals(symbols)
    if funds:
        upsert("screener_fundamentals", funds, "symbol")
    print(f"fundamentals rows: {len(funds)}")

    if loaded < len(symbols) * 0.5:
        print("Fewer than half the symbols loaded - failing the run so you get an email.")
        sys.exit(1)


if __name__ == "__main__":
    main()
