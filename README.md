# MAEZTRO Aggr-Agent

A real-time cryptocurrency trading data collector and dashboard for Hyperliquid.

## Features

- **Real-time Data Collection**: Liquidations and trades via WebSockets.
- **Market Metrics**: Periodic polling of Open Interest, Funding Rates, and Price.
- **Liquidation Heatmap**: Visualize "Gravity Zones" where large liquidations occur.
- **Funding Monitor**: Track funding rate trends and APR.

## Setup

1. **Database**: Install PostgreSQL and run `init_db.sql`.
2. **Environment**: Create a `.env` file based on `.env.example`.
3. **Dependencies**: `pip install -r requirements.txt`
4. **Run Collector**: `python3 src/metrics_collector.py`
5. **Run Dashboard**: `streamlit run src/dashboard/app.py`
