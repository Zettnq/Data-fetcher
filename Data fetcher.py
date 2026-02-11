#requirments
import pandas as pd
import ccxt
from datetime import datetime
import time

#config
exchange = ccxt.binance() #exchange. Also possible to choose futures/perpetuals 
symbol = 'BTC/USDT' #ticker
timeframe = '15m' #timeframe
limit_per_call = 1000 #candles limit per every call. For binance it is 1000c./call | For bybit it is 200c./call | For OKX it is 100c./call | For other CEXs check the API
since_dt = datetime(2026, 1, 1) #Date from fetch data. Use "None" for all available data
until_dt = None #Date to fetch data. Use "None" to fetch data for the current day

since_ms = int(since_dt.timestamp() * 1000) if since_dt else None
until_ms = int(until_dt.timestamp() * 1000) if until_dt else None

all_ohlcv = []
current_since = since_ms

while True:
    try:
        
        ohlcv = exchange.fetch_ohlcv(
            symbol, timeframe, 
            since=current_since, 
            limit=limit_per_call,
            params={'until': until_ms} if until_ms else {}
        )
        
        if not ohlcv:
            break
            
        all_ohlcv.extend(ohlcv)
        print(f"Recieved {len(ohlcv)} candles, total: {len(all_ohlcv)}")
        
        current_since = ohlcv[-1][0] + 1
        
        if until_ms and current_since >= until_ms:
            print("Completed")
            break
        if len(ohlcv) < limit_per_call // 2:
            print("By zettnq @zettnquant @iceninetrading")
            break
            
        time.sleep(exchange.rateLimit / 1000.0)
        
    except Exception as e:
        print(f"Mistake: {e}")
        time.sleep(2)
        break

df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
print("\nLast 5 candles:")
print(df[['datetime', 'open', 'close']].tail())

print(f"\nTotal candles recieved {len(df)} from {df['datetime'].min()} to {df['datetime'].max()}")