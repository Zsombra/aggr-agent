import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class DeribitClient:
    """Deribit WebSocket client for perpetual contracts using JSON-RPC 2.0."""
    
    BASE_WS_URL = "wss://www.deribit.com/ws/api/v2"
    BASE_REST_URL = "https://www.deribit.com"

    # Symbol mapping (BTCUSDT -> BTC-PERPETUAL)
    SYMBOL_MAP = {
        "BTCUSDT": "BTC-PERPETUAL",
        "ETHUSDT": "ETH-PERPETUAL",
    }
    REVERSE_MAP = {v: k for k, v in SYMBOL_MAP.items()}

    def __init__(self, symbols: list[str]):
        self.symbols = [self._to_deribit_symbol(s) for s in symbols]
        self.original_symbols = {self._to_deribit_symbol(s): s.upper() for s in symbols}
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.msg_id = 0
        
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

    def _to_deribit_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to BTC-PERPETUAL."""
        symbol = symbol.upper()
        return self.SYMBOL_MAP.get(symbol, symbol)

    def _to_standard_symbol(self, deribit_symbol: str) -> str:
        """Convert BTC-PERPETUAL back to BTCUSDT."""
        return self.original_symbols.get(deribit_symbol, self.REVERSE_MAP.get(deribit_symbol, deribit_symbol))

    def _next_id(self) -> int:
        self.msg_id += 1
        return self.msg_id

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
                logger.info(f"Connecting to Deribit: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=30
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to Deribit")

                    await self._subscribe(ws)
                    asyncio.create_task(self._heartbeat(ws))

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"Deribit WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _heartbeat(self, ws):
        """Send heartbeat to keep connection alive."""
        try:
            while self.running:
                msg = {
                    "jsonrpc": "2.0",
                    "id": self._next_id(),
                    "method": "public/test",
                    "params": {}
                }
                await ws.send(json.dumps(msg))
                await asyncio.sleep(15)
        except Exception:
            pass  # Connection closed

    async def _subscribe(self, ws):
        """Subscribe to all channels using JSON-RPC."""
        channels = []
        for sym in self.symbols:
            channels.extend([
                f"trades.{sym}.100ms",  # Use 100ms instead of .raw (raw requires auth)
                f"book.{sym}.100ms",
                f"ticker.{sym}.100ms",
            ])

        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": self._next_id(),
            "method": "public/subscribe",
            "params": {"channels": channels}
        }
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to Deribit streams: {len(channels)} channels")

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            
            # Handle RPC responses
            if "id" in data and "result" in data:
                logger.debug(f"Deribit RPC response: {data.get('id')}")
                return

            if "error" in data:
                logger.warning(f"Deribit error: {data['error']}")
                return

            # Handle subscription data
            if "params" in data:
                params = data["params"]
                channel = params.get("channel", "")
                
                if channel.startswith("trades."):
                    await self._handle_trade(params)
                elif channel.startswith("book."):
                    await self._handle_depth(params)
                elif channel.startswith("ticker."):
                    self._handle_ticker(params)

        except Exception as e:
            logger.error(f"Error handling Deribit message: {e}")

    async def _handle_trade(self, params: dict):
        channel = params.get("channel", "")
        # Extract symbol from channel: trades.BTC-PERPETUAL.raw
        parts = channel.split(".")
        if len(parts) >= 2:
            deribit_symbol = parts[1]
            symbol = self._to_standard_symbol(deribit_symbol)
        else:
            return

        for t in params.get("data", []):
            trade = {
                "exchange": "deribit",
                "symbol": symbol,
                "price": float(t["price"]),
                "quantity": float(t["amount"]),
                "quoteQty": float(t["price"]) * float(t["amount"]),
                "isBuyerMaker": t["direction"] == "sell",
                "timestamp": t["timestamp"],
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, params: dict):
        channel = params.get("channel", "")
        parts = channel.split(".")
        if len(parts) >= 2:
            deribit_symbol = parts[1]
            symbol = self._to_standard_symbol(deribit_symbol)
        else:
            return

        book_data = params.get("data", {})
        msg_type = book_data.get("type", "")

        if msg_type == "snapshot":
            bids = {float(b[0]): float(b[1]) for b in book_data.get("bids", [])}
            asks = {float(a[0]): float(a[1]) for a in book_data.get("asks", [])}
            self.orderbooks[symbol] = {"bids": bids, "asks": asks}
            self.initialized[symbol] = True
            logger.info(f"Deribit orderbook initialized for {symbol}")
        elif msg_type == "change":
            if not self.initialized.get(symbol, False):
                return

            ob = self.orderbooks.get(symbol)
            if not ob:
                return

            # Deribit sends changes as: ["change", price, quantity] or ["new", price, quantity] or ["delete", price, 0]
            for change in book_data.get("bids", []):
                if len(change) >= 3:
                    action, price, qty = change[0], float(change[1]), float(change[2])
                    if action == "delete" or qty == 0:
                        ob["bids"].pop(price, None)
                    else:
                        ob["bids"][price] = qty

            for change in book_data.get("asks", []):
                if len(change) >= 3:
                    action, price, qty = change[0], float(change[1]), float(change[2])
                    if action == "delete" or qty == 0:
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
            "exchange": "deribit",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

        await self.on_depth(orderbook)

    def _handle_ticker(self, params: dict):
        channel = params.get("channel", "")
        parts = channel.split(".")
        if len(parts) >= 2:
            deribit_symbol = parts[1]
            symbol = self._to_standard_symbol(deribit_symbol)
        else:
            return

        ticker = params.get("data", {})
        
        if ticker.get("mark_price"):
            self.mark_prices[symbol] = float(ticker["mark_price"])
        if ticker.get("index_price"):
            self.index_prices[symbol] = float(ticker["index_price"])
        if ticker.get("current_funding"):
            self.funding_rates[symbol] = float(ticker["current_funding"])
        if ticker.get("open_interest"):
            self.open_interest[symbol] = float(ticker["open_interest"])

        if self.on_mark_price:
            asyncio.create_task(self.on_mark_price({
                "symbol": symbol,
                "markPrice": self.mark_prices.get(symbol, 0),
                "indexPrice": self.index_prices.get(symbol, 0),
                "fundingRate": self.funding_rates.get(symbol, 0),
                "nextFundingTime": self.next_funding_times.get(symbol, 0),
            }))

    async def _poll_open_interest(self):
        """Poll open interest data periodically (backup to ticker)."""
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    for deribit_symbol in self.symbols:
                        symbol = self._to_standard_symbol(deribit_symbol)
                        url = f"{self.BASE_REST_URL}/api/v2/public/get_book_summary_by_instrument"
                        params = {"instrument_name": deribit_symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("result"):
                                data = result["result"][0] if isinstance(result["result"], list) else result["result"]
                                if data.get("open_interest"):
                                    self.open_interest[symbol] = float(data["open_interest"])
                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error polling Deribit OI: {e}")
            await asyncio.sleep(300)

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
            "exchange": "deribit",
            "symbol": symbol,
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
            "exchange": "deribit",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "fundingRate": self.funding_rates.get(symbol, 0),
            "nextFundingTime": self.next_funding_times.get(symbol, 0),
            "openInterest": oi,
            "openInterestUsd": oi * mark if mark > 0 else 0,
            "indexPrice": index,
            "markPrice": mark,
            "basis": basis,
            "basisBps": basis_bps,
        }
