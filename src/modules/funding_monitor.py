import pandas as pd
import plotly.graph_objects as go
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

class FundingMonitor:
    def __init__(self, symbol="BTCUSDT"):
        self.symbol = symbol
        self.db_config = {
            'host': os.getenv('DB_HOST', '127.0.0.1'),
            'port': os.getenv('DB_PORT', '5432'),
            'dbname': os.getenv('DB_NAME', 'aggr_data'),
            'user': os.getenv('DB_USER', 'aggr_user'),
            'password': os.getenv('DB_PASSWORD'),
        }

    def fetch_history(self, hours=24):
        """Fetches funding rate history."""
        try:
            conn = psycopg2.connect(**self.db_config)
            query = """
                SELECT timestamp, funding_rate, price
                FROM market_metrics
                WHERE symbol = %s
                AND timestamp >= NOW() - INTERVAL '%s hours'
                ORDER BY timestamp ASC
            """
            df = pd.read_sql_query(query, conn, params=(self.symbol, hours))
            conn.close()
            return df
        except Exception as e:
            print(f"Error fetching funding data: {e}")
            return pd.DataFrame()

    def calculate_stats(self, df):
        """Calculates APR and current state."""
        if df.empty:
            return {}

        current_rate = df.iloc[-1]['funding_rate']
        
        # Hyperliquid Funding is hourly. 
        # APR = Rate * 24 hours * 365 days
        apr = current_rate * 24 * 365 * 100 
        
        avg_rate = df['funding_rate'].mean()
        
        # Prediction (Simple Trend)
        trend = "Stable"
        if current_rate > avg_rate * 1.1: trend = "Rising ↗️"
        elif current_rate < avg_rate * 0.9: trend = "Falling ↘️"

        return {
            "current_rate": current_rate,
            "apr": apr,
            "avg_rate": avg_rate,
            "trend": trend
        }

    def create_chart(self, df):
        """Creates a Funding Rate vs Price chart."""
        if df.empty:
            return go.Figure()

        fig = go.Figure()

        # Trace 1: Funding Rate (Bar)
        colors = ['#00C805' if x > 0 else '#FF3B30' for x in df['funding_rate']]
        
        fig.add_trace(go.Bar(
            x=df['timestamp'],
            y=df['funding_rate'],
            name="Funding Rate",
            marker_color=colors,
            yaxis="y1"
        ))

        # Trace 2: Price (Line)
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['price'],
            name="Price",
            line=dict(color='rgba(255, 255, 255, 0.3)', width=1),
            yaxis="y2"
        ))

        fig.update_layout(
            title="Funding Rate History (Hourly)",
            template="plotly_dark",
            height=500,
            yaxis=dict(title="Funding Rate", side="left", tickformat=".4%"),
            yaxis2=dict(title="Price", side="right", overlaying="y", showgrid=False),
            legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center")
        )
        return fig
