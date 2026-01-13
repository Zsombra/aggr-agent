import asyncio
import json
import os
import aiohttp
import websockets
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'aggr_data'),
    'user': os.getenv('DB_USER', 'aggr_user'),
    'password': os.getenv('DB_PASSWORD'),
}
SYMBOLS = ['BTC', 'ETH', 'SOL']

async def save_data(conn, channel, data):
    try:
        cur = conn.cursor()
        
        # --- HANDLE LIQUIDATIONS ---
        if channel == 'liquidations':
            events = data.get('data', [])
            for liq in events:
                coin = liq['coin']
                if coin not in SYMBOLS: continue
                
                side = liq['side'] 
                price = float(liq['px'])
                qty = float(liq['sz'])
                usd_size = price * qty
                ts = datetime.fromtimestamp(liq['time'] / 1000)
                symbol_std = f"{coin}USDT"
                
                sql = "INSERT INTO liquidations (timestamp, symbol, side, price, quantity, usd_size) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql, (ts, symbol_std, side, price, qty, usd_size))
                print(f"[REKT] {coin} {side} Liq: ${usd_size:,.0f} | Price: {price}")

        # --- HANDLE TRADES (NEW) ---
        elif channel == 'trades':
            trades = data.get('data', [])
            for t in trades:
                coin = t['coin']
                side = t['side'] # 'B' or 'A' (Ask/Sell) -> Hyperliquid uses B/A or B/S depending on version
                # Hyperliquid Trades: side is 'B' (Buy) or 'A' (Ask/Sell)
                # We map 'A' to 'SELL' and 'B' to 'BUY'
                normalized_side = 'BUY' if t['side'] == 'B' else 'SELL'
                
                price = float(t['px'])
                qty = float(t['sz'])
                ts = datetime.fromtimestamp(t['time'] / 1000)
                symbol_std = f"{coin}USDT"

                sql = "INSERT INTO trades (timestamp, symbol, side, price, size) VALUES (%s, %s, %s, %s, %s)"
                cur.execute(sql, (ts, symbol_std, normalized_side, price, qty))
                
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"[DB ERROR] Save failed: {e}")
        conn.rollback()

async def listen_ws(conn):
    url = "wss://api.hyperliquid.xyz/ws"
    print("[WS] Connecting to Hyperliquid...")
    
    while True:
        try:
            async with websockets.connect(url) as ws:
                # 1. Subscribe to Liquidations (Global)
                await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "liquidations"}}))
                
                # 2. Subscribe to Trades (Per Coin) - NEW
                for coin in SYMBOLS:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
                    print(f"[WS] Subscribed to Trades: {coin}")

                # 3. Ping Loop
                async def keep_alive():
                    while True:
                        await asyncio.sleep(50)
                        try:
                            await ws.send(json.dumps({"method": "ping"}))
                        except:
                            break
                asyncio.create_task(keep_alive())

                # 4. Listen
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    
                    channel = data.get('channel')
                    if channel == 'liquidations':
                        await save_data(conn, 'liquidations', data)
                    elif channel == 'trades':
                        await save_data(conn, 'trades', data)
                    elif channel == 'pong':
                        continue
                    else:
                        if data.get('channel') != 'subscriptionResponse':
                             print(f"[WS DEBUG] Received unknown channel: {channel} | Data: {msg[:200]}")

        except Exception as e:
            print(f"[WS ERROR] {e} - Reconnecting in 5s...")
            await asyncio.sleep(5)

async def fetch_metrics(conn):
    url = "https://api.hyperliquid.xyz/info"
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(url, json={"type": "metaAndAssetCtxs"}) as r:
                    if r.status != 200:
                        await asyncio.sleep(10); continue
                    
                    data = await r.json()
                    universe = data[0]['universe']
                    asset_ctxs = data[1]
                    
                    cur = conn.cursor()
                    for i, asset_info in enumerate(universe):
                        coin = asset_info['name']
                        if coin not in SYMBOLS: continue
                        
                        ctx = asset_ctxs[i]
                        mark_px = float(ctx['markPx'])
                        oi_sz = float(ctx['openInterest'])
                        oi_usd = oi_sz * mark_px
                        funding = float(ctx['funding'])
                        
                        symbol_std = f"{coin}USDT"
                        sql = """
                            INSERT INTO market_metrics 
                            (timestamp, symbol, open_interest, open_interest_usd, funding_rate, long_short_ratio, price)
                            VALUES (NOW(), %s, %s, %s, %s, 1.0, %s)
                            ON CONFLICT (timestamp, symbol) DO NOTHING
                        """
                        cur.execute(sql, (symbol_std, oi_sz, oi_usd, funding, mark_px))
                        
                    conn.commit()
                    cur.close()
            except Exception as e:
                print(f"[POLL ERROR] {e}")
            await asyncio.sleep(30)

async def main():
    conn = psycopg2.connect(**DB_CONFIG)
    await asyncio.gather(listen_ws(conn), fetch_metrics(conn))

if __name__ == "__main__":
    asyncio.run(main())
