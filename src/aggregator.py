import time
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'aggr_data'),
    'user': os.getenv('DB_USER', 'aggr_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password')
}

SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
TIMEFRAMES = {
    '1m': 60,
    '5m': 300,
    '15m': 900,
    '1h': 3600
}

def aggregate_candles(conn, symbol, timeframe, interval_seconds):
    """Aggregate trades into OHLCV candles"""
    cur = conn.cursor()
    
    try:
        # Get the last candle timestamp
        cur.execute("""
            SELECT MAX(timestamp) FROM candles 
            WHERE symbol = %s AND timeframe = %s
        """, (symbol, timeframe))
        
        last_candle = cur.fetchone()[0]
        start_time = last_candle if last_candle else datetime.now() - timedelta(hours=24)
        
        # Aggregate trades into candles
        sql = """
            WITH candle_data AS (
                SELECT 
                    date_trunc('minute', timestamp) + 
                        (EXTRACT(EPOCH FROM timestamp - date_trunc('minute', timestamp))::int / %s) * 
                        INTERVAL '1 second' * %s AS candle_time,
                    symbol,
                    MIN(price) as low,
                    MAX(price) as high,
                    (array_agg(price ORDER BY timestamp ASC))[1] as open,
                    (array_agg(price ORDER BY timestamp DESC))[1] as close,
                    SUM(size) as volume,
                    SUM(CASE WHEN side = 'BUY' THEN size ELSE -size END) as cvd_delta
                FROM trades
                WHERE symbol = %s 
                    AND timestamp > %s
                GROUP BY candle_time, symbol
            )
            INSERT INTO candles (timestamp, exchange, symbol, timeframe, open, high, low, close, volume, cvd)
            SELECT 
                candle_time,
                'hyperliquid',
                symbol,
                %s,
                open,
                high,
                low,
                close,
                volume,
                cvd_delta
            FROM candle_data
        """
        
        # First delete existing candles for this time range to avoid duplicates
        cur.execute("""
            DELETE FROM candles 
            WHERE symbol = %s AND timeframe = %s AND timestamp >= %s
        """, (symbol, timeframe, start_time))
        
        # Then insert new candles
        cur.execute(sql, (interval_seconds, interval_seconds, symbol, start_time, timeframe))
        rows_affected = cur.rowcount
        conn.commit()
        print(f"[AGGREGATOR] Updated {rows_affected} {timeframe} candles for {symbol}")
        
    except Exception as e:
        print(f"[AGGREGATOR ERROR] {symbol} {timeframe}: {e}")
        conn.rollback()
    finally:
        cur.close()

def main():
    """Main aggregation loop"""
    conn = psycopg2.connect(**DB_CONFIG)
    print("[AGGREGATOR] Starting candle aggregation...")
    
    while True:
        try:
            for symbol in SYMBOLS:
                for timeframe, interval in TIMEFRAMES.items():
                    aggregate_candles(conn, symbol, timeframe, interval)
            
            # Run every 10 seconds
            time.sleep(10)
            
        except Exception as e:
            print(f"[AGGREGATOR ERROR] {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
