-- Create database if it doesn't exist
-- CREATE DATABASE aggr_data;

-- Connect to the database
-- \c aggr_data;

-- Table for Liquidations
CREATE TABLE IF NOT EXISTS liquidations (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL NOT NULL,
    quantity DECIMAL NOT NULL,
    usd_size DECIMAL NOT NULL
);

-- Table for Trades
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    side VARCHAR(10) NOT NULL,
    price DECIMAL NOT NULL,
    size DECIMAL NOT NULL
);

-- Table for Market Metrics
CREATE TABLE IF NOT EXISTS market_metrics (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open_interest DECIMAL,
    open_interest_usd DECIMAL,
    funding_rate DECIMAL,
    long_short_ratio DECIMAL,
    price DECIMAL,
    PRIMARY KEY (timestamp, symbol)
);

-- Index for performance
CREATE INDEX IF NOT EXISTS idx_liq_ts ON liquidations(timestamp);
CREATE INDEX IF NOT EXISTS idx_liq_symbol_ts ON liquidations(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts ON trades(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_ts ON market_metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_symbol_ts ON market_metrics(symbol, timestamp);

-- Table for Candles (OHLCV data)
CREATE TABLE IF NOT EXISTS candles (
    timestamp TIMESTAMP NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    open DECIMAL NOT NULL,
    high DECIMAL NOT NULL,
    low DECIMAL NOT NULL,
    close DECIMAL NOT NULL,
    volume DECIMAL NOT NULL,
    cvd DECIMAL NOT NULL,
    PRIMARY KEY (timestamp, exchange, symbol, timeframe)
);

CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles(timestamp);
CREATE INDEX IF NOT EXISTS idx_candles_symbol ON candles(symbol, timeframe, timestamp);
