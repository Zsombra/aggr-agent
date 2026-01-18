import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class CoinbaseClient:
    """Coinbase Advanced Trade WebSocket client for spot markets."""
    
    BASE_WS_URL = "wss://advanced-trade-ws.coinbase.com"
    BASE_REST_URL = "https://api.coinbase.com"

    def __init__(self, symbols: list[str]):
        # Convert BTCUSDT -> BTC-USD format (Coinbase is spot)
        self.symbols = [self._to_coinbase_symbol(s) for s in symbols]
        self.original_symbols = {self._to_coinbase_symbol(s): s.upper() for s in symbols}
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        
        # Callbacks
        self.on_trade: Optional[Callable] = None
        self.on_depth: Optional[Callable] = None
        self.on_liquidation: Optional[Callable] = None  # Not available for spot
        self.on_mark_price: Optional[Callable] = None

        # Orderbook state
        self.orderbooks: dict[str, dict] = {}
        self.initialized: dict[str, bool] = {}

        # Market data state (limited for spot)
        self.last_prices: dict[str, float] = {}

    def _to_coinbase_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to BTC-USD (Coinbase uses USD not USDT)."""
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USD"
        elif symbol.endswith("USD"):
            base = symbol[:-3]
            return f"{base}-USD"
        return symbol

    def _to_standard_symbol(self, coinbase_symbol: str) -> str:
        """Convert BTC-USD back to BTCUSDT."""
        return self.original_symbols.get(coinbase_symbol, coinbase_symbol.replace("-", "").replace("USD", "USDT"))

    async def start(self):
        self.running = True
        asyncio.create_task(self._run_ws())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _run_ws(self):
        while self.running:
            try:
                logger.info(f"Connecting to Coinbase: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=30
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to Coinbase Advanced Trade")

                    await self._subscribe(ws)

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"Coinbase WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _subscribe(self, ws):
        """Subscribe to market_trades and level2 channels."""
        # Subscribe to market trades
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": self.symbols,
            "channel": "market_trades"
        }))
        
        # Subscribe to level2 orderbook
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": self.symbols,
            "channel": "level2"
        }))
        
        # Subscribe to heartbeats to keep connection alive
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": self.symbols,
            "channel": "heartbeats"
        }))

        logger.info(f"Subscribed to Coinbase streams for {len(self.symbols)} symbols")

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            channel = data.get("channel", "")
            
            # Handle subscription confirmations
            if data.get("type") == "subscriptions":
                logger.debug(f"Coinbase subscriptions: {data.get('channels')}")
                return

            if data.get("type") == "error":
                logger.warning(f"Coinbase error: {data}")
                return

            if channel == "market_trades":
                await self._handle_trade(data)
            elif channel == "l2_data":
                await self._handle_depth(data)
            elif channel == "heartbeats":
                pass  # Ignore heartbeats

        except Exception as e:
            logger.error(f"Error handling Coinbase message: {e}")

    async def _handle_trade(self, data: dict):
        for event in data.get("events", []):
            for t in event.get("trades", []):
                product_id = t.get("product_id", "")
                symbol = self._to_standard_symbol(product_id)
                
                price = float(t["price"])
                size = float(t["size"])
                
                trade = {
                    "exchange": "coinbase",
                    "symbol": symbol,
                    "price": price,
                    "quantity": size,
                    "quoteQty": price * size,
                    "isBuyerMaker": t["side"] == "SELL",
                    "timestamp": int(time.time() * 1000),  # Coinbase uses ISO time
                }
                
                self.last_prices[symbol] = price

                if self.on_trade:
                    await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        for event in data.get("events", []):
            product_id = event.get("product_id", "")
            symbol = self._to_standard_symbol(product_id)
            event_type = event.get("type", "")
            
            if event_type == "snapshot":
                # Full orderbook snapshot
                bids = {}
                asks = {}
                for update in event.get("updates", []):
                    price = float(update["price_level"])
                    qty = float(update["new_quantity"])
                    if update["side"] == "bid":
                        bids[price] = qty
                    else:
                        asks[price] = qty
                
                self.orderbooks[symbol] = {"bids": bids, "asks": asks}
                self.initialized[symbol] = True
                logger.info(f"Coinbase orderbook initialized for {symbol}")
            
            elif event_type == "update":
                # Incremental update
                if not self.initialized.get(symbol, False):
                    continue
                
                ob = self.orderbooks.get(symbol)
                if not ob:
                    continue
                
                for update in event.get("updates", []):
                    price = float(update["price_level"])
                    qty = float(update["new_quantity"])
                    
                    if update["side"] == "bid":
                        if qty == 0:
                            ob["bids"].pop(price, None)
                        else:
                            ob["bids"][price] = qty
                    else:
                        if qty == 0:
                            ob["asks"].pop(price, None)
                        else:
                            ob["asks"][price] = qty

            await self._emit_orderbook(symbol)

    async def _emit_orderbook(self, symbol: str):
        ob = self.orderbooks.get(symbol)
        if not ob or not self.on_depth:
            return

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        orderbook = {
            "exchange": "coinbase",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

        await self.on_depth(orderbook)

    def get_orderbook(self, symbol: str) -> Optional[dict]:
        symbol = symbol.upper()
        ob = self.orderbooks.get(symbol)
        if not ob:
            return None

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        return {
            "exchange": "coinbase",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

    def get_market_stats(self, symbol: str) -> Optional[dict]:
        """Coinbase is spot - limited market stats available."""
        symbol = symbol.upper()
        last_price = self.last_prices.get(symbol, 0)
        
        if last_price == 0:
            ob = self.orderbooks.get(symbol)
            if ob and ob["bids"] and ob["asks"]:
                best_bid = max(ob["bids"].keys())
                best_ask = min(ob["asks"].keys())
                last_price = (best_bid + best_ask) / 2

        if last_price == 0:
            return None

        return {
            "exchange": "coinbase",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "fundingRate": 0,  # No funding for spot
            "nextFundingTime": 0,
            "openInterest": 0,  # No OI for spot
            "openInterestUsd": 0,
            "indexPrice": last_price,
            "markPrice": last_price,
            "basis": 0,
            "basisBps": 0,
        }
