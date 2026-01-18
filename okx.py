import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class OKXClient:
    """OKX Public WebSocket client for SWAP perpetuals."""
    
    BASE_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
    BASE_REST_URL = "https://www.okx.com"

    def __init__(self, symbols: list[str]):
        # Convert BTCUSDT -> BTC-USDT-SWAP format
        self.symbols = [self._to_okx_symbol(s) for s in symbols]
        self.original_symbols = {self._to_okx_symbol(s): s.upper() for s in symbols}
        
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

    def _to_okx_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to BTC-USDT-SWAP."""
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol

    def _to_standard_symbol(self, okx_symbol: str) -> str:
        """Convert BTC-USDT-SWAP back to BTCUSDT."""
        return self.original_symbols.get(okx_symbol, okx_symbol.replace("-", "").replace("SWAP", ""))

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
                logger.info(f"Connecting to OKX: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=25,
                    ping_timeout=10
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to OKX SWAP")

                    await self._subscribe(ws)

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"OKX WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _subscribe(self, ws):
        """Subscribe to all channels."""
        args = []
        for sym in self.symbols:
            args.extend([
                {"channel": "trades", "instId": sym},
                {"channel": "books5", "instId": sym},
                {"channel": "mark-price", "instId": sym},
                {"channel": "funding-rate", "instId": sym},
            ])
        
        # Subscribe to liquidations for SWAP instrument type
        args.append({"channel": "liquidation-orders", "instType": "SWAP"})

        subscribe_msg = {"op": "subscribe", "args": args}
        await ws.send(json.dumps(subscribe_msg))
        logger.info(f"Subscribed to OKX streams: {len(args)} channels")

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            
            # Handle subscription events
            if "event" in data:
                if data["event"] == "subscribe":
                    logger.debug(f"OKX subscription confirmed: {data.get('arg')}")
                elif data["event"] == "error":
                    logger.warning(f"OKX error: {data}")
                return

            arg = data.get("arg", {})
            channel = arg.get("channel", "")

            if channel == "trades":
                await self._handle_trade(data)
            elif channel == "books5":
                await self._handle_depth(data)
            elif channel == "mark-price":
                self._handle_mark_price(data)
            elif channel == "funding-rate":
                self._handle_funding_rate(data)
            elif channel == "liquidation-orders":
                await self._handle_liquidation(data)

        except Exception as e:
            logger.error(f"Error handling OKX message: {e}")

    async def _handle_trade(self, data: dict):
        arg = data.get("arg", {})
        inst_id = arg.get("instId", "")
        trades = data.get("data", [])
        
        for t in trades:
            trade = {
                "exchange": "okx",
                "symbol": self._to_standard_symbol(inst_id),
                "price": float(t["px"]),
                "quantity": float(t["sz"]),
                "quoteQty": float(t["px"]) * float(t["sz"]),
                "isBuyerMaker": t["side"] == "sell",
                "timestamp": int(t["ts"]),
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        arg = data.get("arg", {})
        inst_id = arg.get("instId", "")
        symbol = self._to_standard_symbol(inst_id)
        action = data.get("action", "snapshot")
        book_data = data.get("data", [{}])[0]

        if action == "snapshot":
            bids = {float(b[0]): float(b[1]) for b in book_data.get("bids", [])}
            asks = {float(a[0]): float(a[1]) for a in book_data.get("asks", [])}
            self.orderbooks[symbol] = {"bids": bids, "asks": asks}
            self.initialized[symbol] = True
            logger.info(f"OKX orderbook initialized for {symbol}")
        elif action == "update":
            if not self.initialized.get(symbol, False):
                return

            ob = self.orderbooks.get(symbol)
            if not ob:
                return

            for b in book_data.get("bids", []):
                price, qty = float(b[0]), float(b[1])
                if qty == 0:
                    ob["bids"].pop(price, None)
                else:
                    ob["bids"][price] = qty

            for a in book_data.get("asks", []):
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
            "exchange": "okx",
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
        for liq in data.get("data", []):
            inst_id = liq.get("instId", "")
            # Only process if it's one of our symbols
            if inst_id not in self.symbols:
                continue

            price = float(liq.get("bkPx", 0))
            qty = float(liq.get("sz", 0))

            liquidation = {
                "exchange": "okx",
                "symbol": self._to_standard_symbol(inst_id),
                "side": liq.get("side", "").upper(),
                "price": price,
                "quantity": qty,
                "notionalUsd": price * qty,
                "timestamp": int(liq.get("ts", time.time() * 1000)),
            }

            if self.on_liquidation:
                await self.on_liquidation(liquidation)

    def _handle_mark_price(self, data: dict):
        for item in data.get("data", []):
            inst_id = item.get("instId", "")
            symbol = self._to_standard_symbol(inst_id)
            
            if item.get("markPx"):
                self.mark_prices[symbol] = float(item["markPx"])

            if self.on_mark_price:
                asyncio.create_task(self.on_mark_price({
                    "symbol": symbol,
                    "markPrice": self.mark_prices.get(symbol, 0),
                    "indexPrice": self.index_prices.get(symbol, 0),
                    "fundingRate": self.funding_rates.get(symbol, 0),
                    "nextFundingTime": self.next_funding_times.get(symbol, 0),
                }))

    def _handle_funding_rate(self, data: dict):
        for item in data.get("data", []):
            inst_id = item.get("instId", "")
            symbol = self._to_standard_symbol(inst_id)
            
            if item.get("fundingRate"):
                self.funding_rates[symbol] = float(item["fundingRate"])
            if item.get("nextFundingTime"):
                self.next_funding_times[symbol] = int(item["nextFundingTime"])

    async def _poll_open_interest(self):
        """Poll open interest data periodically."""
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    for okx_symbol in self.symbols:
                        symbol = self._to_standard_symbol(okx_symbol)
                        url = f"{self.BASE_REST_URL}/api/v5/public/open-interest"
                        params = {"instType": "SWAP", "instId": okx_symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("code") == "0":
                                oi_list = result.get("data", [])
                                if oi_list:
                                    self.open_interest[symbol] = float(oi_list[0].get("oi", 0))
                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error polling OKX OI: {e}")
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
            "exchange": "okx",
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
            "exchange": "okx",
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
