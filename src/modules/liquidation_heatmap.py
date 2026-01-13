import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_NAME', 'aggr_data'),
    'user': os.getenv('DB_USER', 'aggr_user'),
    'password': os.getenv('DB_PASSWORD'),
}

class LiquidationHeatmap:
    def __init__(self, symbol='BTCUSDT'):
        self.symbol = symbol
        self.bucket_sizes = {'BTCUSDT': 250, 'ETHUSDT': 25, 'SOLUSDT': 2}
        self.bucket_size = self.bucket_sizes.get(symbol, 100)
    
    def get_connection(self):
        return psycopg2.connect(**DB_CONFIG)
    
    def bucket_price(self, price):
        return int(float(price) / self.bucket_size) * self.bucket_size
    
    def get_liquidations(self, hours_back=24):
        conn = self.get_connection()
        query = """SELECT timestamp, side, price, quantity, usd_size 
                   FROM liquidations WHERE symbol = %s 
                   AND timestamp > NOW() - INTERVAL '%s hours'
                   ORDER BY timestamp DESC"""
        df = pd.read_sql(query, conn, params=(self.symbol, hours_back))
        conn.close()
        return df
    
    def get_current_price(self):
        """Get current price from market_metrics, candles, or trades (with fallback)"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Try market_metrics first (most reliable)
        cur.execute("""
            SELECT price FROM market_metrics 
            WHERE symbol = %s AND price IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
        """, (self.symbol,))
        result = cur.fetchone()
        
        # Fallback to candles
        if not result:
            cur.execute("""
                SELECT close FROM candles 
                WHERE symbol = %s 
                ORDER BY timestamp DESC LIMIT 1
            """, (self.symbol,))
            result = cur.fetchone()
        
        # Last resort: trades
        if not result:
            cur.execute("""
                SELECT price FROM trades 
                WHERE symbol = %s 
                ORDER BY timestamp DESC LIMIT 1
            """, (self.symbol,))
            result = cur.fetchone()
        
        conn.close()
        return float(result[0]) if result else 0
    
    def generate_heatmap_data(self, hours_back=24):
        df = self.get_liquidations(hours_back)
        if df.empty:
            return pd.DataFrame()
        
        df['bucket'] = df['price'].apply(self.bucket_price)
        df['is_long'] = df['side'] == 'SELL'
        df['is_short'] = df['side'] == 'BUY'
        
        long_usd = df[df['is_long']].groupby('bucket')['usd_size'].sum()
        short_usd = df[df['is_short']].groupby('bucket')['usd_size'].sum()
        
        grouped = df.groupby('bucket').agg(
            count=('price', 'count'),
            usd_total=('usd_size', 'sum'),
            longs=('is_long', 'sum'),
            shorts=('is_short', 'sum')
        ).reset_index()
        
        grouped['long_usd'] = grouped['bucket'].map(long_usd).fillna(0)
        grouped['short_usd'] = grouped['bucket'].map(short_usd).fillna(0)
        
        if not grouped.empty:
            max_usd = grouped['usd_total'].max() or 1
            max_count = grouped['count'].max() or 1
            grouped['gravity'] = ((grouped['usd_total']/max_usd)*50 + (grouped['count']/max_count)*50).round(1)
            grouped['price_range'] = grouped['bucket'].apply(lambda x: f"${x:,.0f}-${x+self.bucket_size:,.0f}")
            grouped = grouped.sort_values('gravity', ascending=False)
        return grouped
    
    def get_summary(self, hours_back=24):
        df = self.get_liquidations(hours_back)
        if df.empty:
            return {'total_count': 0, 'total_usd': 0, 'longs': 0, 'shorts': 0, 'long_usd': 0, 'short_usd': 0, 'bias': 'neutral'}
        
        longs = (df['side'] == 'SELL').sum()
        shorts = (df['side'] == 'BUY').sum()
        long_usd = df[df['side'] == 'SELL']['usd_size'].sum()
        short_usd = df[df['side'] == 'BUY']['usd_size'].sum()
        total = longs + shorts
        if total > 0:
            short_pct = shorts / total * 100
            bias = f"SHORT HEAVY ({short_pct:.0f}%)" if short_pct > 60 else f"LONG HEAVY ({100-short_pct:.0f}%)" if short_pct < 40 else "NEUTRAL"
        else:
            bias = "NO DATA"
        return {'total_count': len(df), 'total_usd': df['usd_size'].sum(), 'longs': longs, 'shorts': shorts, 'long_usd': long_usd, 'short_usd': short_usd, 'bias': bias}
