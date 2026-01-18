import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class BybitClient:
    """Bybit V5 Linear Perpetual WebSocket client."""
    
    BASE_WS_URL = "wss://stream.bybit.com/v5/public/linear"
    BASE_REST_URL = "https://api.bybit.com"

    def __init__(self, symbols: list[str]):
        self.symbols = [s.upper() for s in symbols]
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        
        # Callbacks
        self.on_trade: Optional[Callable] = None
        self.on_depth: Optional[Callable] = None
        self.on_liquidation: Optional[Callable] = None
        self.on_mark_price: Optional[Callable] = None

        # Orderbook state
        self.orderbooks: dict[str, dict] = {}
        self.initialized: dict[str, bool] = {}

        # Market data state
        self.mark_prices: dict[str, float] = {}
        self.index_prices: dict[str, float] = {}
        self.funding_rates: dict[str, float] = {}
        self.next_funding_times: dict[str, int] = {}
        self.open_interest: dict[str, float] = {}

    async def start(self):
        self.running = True
        asyncio.create_task(self._run_ws())
        asyncio.create_task(self._poll_open_interest())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _run_ws(self):
        while self.running:
            try:
                logger.info(f"Connecting to Bybit: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1  # Reset on successful connect
                    logger.info("Connected to Bybit Linear")

                    # Subscribe to all streams
                    await self._subscribe(ws)

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"Bybit WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _subscribe(self, ws):
        """Subscribe to all topics for all symbols."""
        args = []
        for sym in self.symbols:
            args.extend([
                f"publicTrade.{sym}",
                f"orderbook.50.{sym}",
                f"liquidation.{sym}",
                f"tickers.{sym}",
            ])

        subscribe_msg = {
            "op": "subscribe",
            "args": args
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to Bybit streams: {len(args)} topics")

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            
            # Handle subscription confirmation
            if data.get("op") == "subscribe":
                if data.get("success"):
                    logger.debug(f"Bybit subscription confirmed")
                else:
                    logger.warning(f"Bybit subscription failed: {data}")
                return

            # Handle pong
            if data.get("op") == "pong":
                return

            topic = data.get("topic", "")

            if topic.startswith("publicTrade."):
                await self._handle_trade(data)
            elif topic.startswith("orderbook."):
                await self._handle_depth(data)
            elif topic.startswith("liquidation."):
                await self._handle_liquidation(data)
            elif topic.startswith("tickers."):
                self._handle_ticker(data)

        except Exception as e:
            logger.error(f"Error handling Bybit message: {e}")

    async def _handle_trade(self, data: dict):
        trades = data.get("data", [])
        for t in trades:
            trade = {
                "exchange": "bybit",
                "symbol": t["s"],
                "price": float(t["p"]),
                "quantity": float(t["v"]),
                "quoteQty": float(t["p"]) * float(t["v"]),
                "isBuyerMaker": t["S"] == "Sell",  # Sell = buyer is taker, seller is maker
                "timestamp": t["T"],
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        symbol = data["data"]["s"]
        msg_type = data.get("type", "")
        book_data = data["data"]

        if msg_type == "snapshot":
            # Initialize orderbook from snapshot
            bids = {float(b[0]): float(b[1]) for b in book_data.get("b", [])}
            asks = {float(a[0]): float(a[1]) for a in book_data.get("a", [])}
            self.orderbooks[symbol] = {"bids": bids, "asks": asks}
            self.initialized[symbol] = True
            logger.info(f"Bybit orderbook initialized for {symbol}")
        elif msg_type == "delta":
            # Apply incremental updates
            if not self.initialized.get(symbol, False):
                return

            ob = self.orderbooks.get(symbol)
            if not ob:
                return

            # Apply bid updates
            for b in book_data.get("b", []):
                price, qty = float(b[0]), float(b[1])
                if qty == 0:
                    ob["bids"].pop(price, None)
                else:
                    ob["bids"][price] = qty

            # Apply ask updates
            for a in book_data.get("a", []):
                price, qty = float(a[0]), float(a[1])
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
            "exchange": "bybit",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

        await self.on_depth(orderbook)

    async def _handle_liquidation(self, data: dict):
        liq_data = data.get("data", {})
        
        price = float(liq_data["price"])
        qty = float(liq_data["size"])

        liquidation = {
            "exchange": "bybit",
            "symbol": liq_data["symbol"],
            "side": liq_data["side"].upper(),  # "Buy" or "Sell"
            "price": price,
            "quantity": qty,
            "notionalUsd": price * qty,
            "timestamp": liq_data["updatedTime"],
        }

        if self.on_liquidation:
            await self.on_liquidation(liquidation)

    def _handle_ticker(self, data: dict):
        ticker = data.get("data", {})
        symbol = ticker.get("symbol")
        if not symbol:
            return

        if ticker.get("markPrice"):
            self.mark_prices[symbol] = float(ticker["markPrice"])
        if ticker.get("indexPrice"):
            self.index_prices[symbol] = float(ticker["indexPrice"])
        if ticker.get("fundingRate"):
            self.funding_rates[symbol] = float(ticker["fundingRate"])
        if ticker.get("nextFundingTime"):
            self.next_funding_times[symbol] = int(ticker["nextFundingTime"])
        if ticker.get("openInterestValue"):
            self.open_interest[symbol] = float(ticker["openInterestValue"])

        if self.on_mark_price:
            asyncio.create_task(self.on_mark_price({
                "symbol": symbol,
                "markPrice": self.mark_prices.get(symbol, 0),
                "indexPrice": self.index_prices.get(symbol, 0),
                "fundingRate": self.funding_rates.get(symbol, 0),
                "nextFundingTime": self.next_funding_times.get(symbol, 0),
            }))

    async def _poll_open_interest(self):
        """Poll open interest data periodically."""
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    for symbol in self.symbols:
                        url = f"{self.BASE_REST_URL}/v5/market/open-interest"
                        params = {"category": "linear", "symbol": symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("retCode") == 0:
                                oi_list = result.get("result", {}).get("list", [])
                                if oi_list:
                                    self.open_interest[symbol] = float(oi_list[0].get("openInterest", 0))
                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error polling Bybit OI: {e}")
            await asyncio.sleep(300)  # Poll every 5 minutes

    def get_orderbook(self, symbol: str) -> Optional[dict]:
        ob = self.orderbooks.get(symbol.upper())
        if not ob:
            return None

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        return {
            "exchange": "bybit",
            "symbol": symbol.upper(),
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

    def get_market_stats(self, symbol: str) -> Optional[dict]:
        symbol = symbol.upper()
        if symbol not in self.mark_prices:
            return None

        mark = self.mark_prices.get(symbol, 0)
        index = self.index_prices.get(symbol, 0)
        oi = self.open_interest.get(symbol, 0)

        basis = mark - index if index > 0 else 0
        basis_bps = (basis / index * 10000) if index > 0 else 0

        return {
            "exchange": "bybit",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "fundingRate": self.funding_rates.get(symbol, 0),
            "nextFundingTime": self.next_funding_times.get(symbol, 0),
            "openInterest": oi,
            "openInterestUsd": oi,  # Already in USD value
            "indexPrice": index,
            "markPrice": mark,
            "basis": basis,
            "basisBps": basis_bps,
        }
