import pandas as pd
import ccxt
from datetime import datetime
import time

def load_ohlcv(
    symbol="BTC/USDT",
    timeframe="1d",
    since_dt=None,
    until_dt=None,
    limit_per_call=1000,
    exchange_name="binance",
):

    exchange = getattr(ccxt, exchange_name)()

    since_ms = int(since_dt.timestamp() * 1000) if since_dt else None
    until_ms = int(until_dt.timestamp() * 1000) if until_dt else None

    all_ohlcv = []
    current_since = since_ms

    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(
                symbol,
                timeframe,
                since=current_since,
                limit=limit_per_call,
                params={"until": until_ms} if until_ms else {},
            )

            if not ohlcv:
                break

            all_ohlcv.extend(ohlcv)
            print(f"Received {len(ohlcv)} candles, total: {len(all_ohlcv)}")

            current_since = ohlcv[-1][0] + 1

            if until_ms and current_since >= until_ms:
                print("Completed")
                break

            time.sleep(exchange.rateLimit / 1000.0)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(2)
            break

    if not all_ohlcv:
        print("No data received.")
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "datetime"])

    df = pd.DataFrame(all_ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

    print("\nLast 5 candles:")
    print(df[["datetime", "open", "close"]].tail())
    print(f"\nTotal candles received: {len(df)} from {df['datetime'].min()} to {df['datetime'].max()}")

    return df