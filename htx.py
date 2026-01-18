import asyncio
import json
import logging
import time
import gzip
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class HTXClient:
    """HTX (Huobi) Linear Swap WebSocket client with GZIP decompression."""
    
    BASE_WS_URL = "wss://api.hbdm.com/linear-swap-ws"
    BASE_REST_URL = "https://api.hbdm.com"

    def __init__(self, symbols: list[str]):
        # Convert BTCUSDT -> BTC-USDT format
        self.symbols = [self._to_htx_symbol(s) for s in symbols]
        self.original_symbols = {self._to_htx_symbol(s): s.upper() for s in symbols}
        
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

    def _to_htx_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to BTC-USDT."""
        symbol = symbol.upper()
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}-USDT"
        return symbol

    def _to_standard_symbol(self, htx_symbol: str) -> str:
        """Convert BTC-USDT back to BTCUSDT."""
        return self.original_symbols.get(htx_symbol, htx_symbol.replace("-", ""))

    async def start(self):
        self.running = True
        asyncio.create_task(self._run_ws())
        asyncio.create_task(self._poll_market_data())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _run_ws(self):
        while self.running:
            try:
                logger.info(f"Connecting to HTX: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=None  # HTX uses custom ping/pong
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to HTX Linear Swap")

                    await self._subscribe(ws)

                    async for msg in ws:
                        await self._handle_message(msg, ws)

            except Exception as e:
                logger.error(f"HTX WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _subscribe(self, ws):
        """Subscribe to all topics."""
        for sym in self.symbols:
            # Trade subscription
            await ws.send(json.dumps({
                "sub": f"market.{sym}.trade.detail",
                "id": f"trade-{sym}"
            }))
            
            # Depth subscription (step0 = highest precision)
            await ws.send(json.dumps({
                "sub": f"market.{sym}.depth.step0",
                "id": f"depth-{sym}"
            }))
            
            # Liquidation subscription
            await ws.send(json.dumps({
                "sub": f"public.{sym}.liquidation_orders",
                "id": f"liq-{sym}"
            }))

            await asyncio.sleep(0.1)

        logger.info(f"Subscribed to HTX streams for {len(self.symbols)} symbols")

    async def _handle_message(self, msg: bytes, ws):
        try:
            # HTX sends GZIP compressed messages
            if isinstance(msg, bytes):
                try:
                    msg = gzip.decompress(msg).decode('utf-8')
                except Exception:
                    msg = msg.decode('utf-8')
            
            data = json.loads(msg)
            
            # Handle ping - must respond with pong
            if "ping" in data:
                pong = {"pong": data["ping"]}
                await ws.send(json.dumps(pong))
                return

            # Handle subscription confirmation
            if "subbed" in data:
                logger.debug(f"HTX subscribed: {data.get('subbed')}")
                return

            if "status" in data and data["status"] == "error":
                logger.warning(f"HTX error: {data}")
                return

            ch = data.get("ch", "")

            if ".trade.detail" in ch:
                await self._handle_trade(data)
            elif ".depth." in ch:
                await self._handle_depth(data)
            elif ".liquidation_orders" in ch:
                await self._handle_liquidation(data)

        except Exception as e:
            logger.error(f"Error handling HTX message: {e}")

    async def _handle_trade(self, data: dict):
        ch = data.get("ch", "")
        # Extract symbol from channel: market.BTC-USDT.trade.detail
        parts = ch.split(".")
        if len(parts) >= 2:
            htx_symbol = parts[1]
            symbol = self._to_standard_symbol(htx_symbol)
        else:
            return

        tick = data.get("tick", {})
        for t in tick.get("data", []):
            price = float(t["price"])
            amount = float(t["amount"])
            
            trade = {
                "exchange": "htx",
                "symbol": symbol,
                "price": price,
                "quantity": amount,
                "quoteQty": price * amount,
                "isBuyerMaker": t["direction"] == "sell",
                "timestamp": t["ts"],
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        ch = data.get("ch", "")
        parts = ch.split(".")
        if len(parts) >= 2:
            htx_symbol = parts[1]
            symbol = self._to_standard_symbol(htx_symbol)
        else:
            return

        tick = data.get("tick", {})
        
        # HTX sends full snapshot with each update
        bids = {float(b[0]): float(b[1]) for b in tick.get("bids", [])}
        asks = {float(a[0]): float(a[1]) for a in tick.get("asks", [])}
        
        self.orderbooks[symbol] = {"bids": bids, "asks": asks}
        if not self.initialized.get(symbol, False):
            self.initialized[symbol] = True
            logger.info(f"HTX orderbook initialized for {symbol}")

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
            "exchange": "htx",
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
            htx_symbol = liq.get("contract_code", "")
            symbol = self._to_standard_symbol(htx_symbol)
            
            price = float(liq.get("price", 0))
            volume = float(liq.get("volume", 0))

            liquidation = {
                "exchange": "htx",
                "symbol": symbol,
                "side": liq.get("direction", "").upper(),
                "price": price,
                "quantity": volume,
                "notionalUsd": price * volume,
                "timestamp": liq.get("created_at", int(time.time() * 1000)),
            }

            if self.on_liquidation:
                await self.on_liquidation(liquidation)

    async def _poll_market_data(self):
        """Poll market data (funding, OI, etc.) periodically."""
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    for htx_symbol in self.symbols:
                        symbol = self._to_standard_symbol(htx_symbol)
                        
                        # Get funding rate
                        url = f"{self.BASE_REST_URL}/linear-swap-api/v1/swap_funding_rate"
                        params = {"contract_code": htx_symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("status") == "ok":
                                data = result.get("data", {})
                                if data.get("funding_rate"):
                                    self.funding_rates[symbol] = float(data["funding_rate"])
                                if data.get("next_funding_time"):
                                    self.next_funding_times[symbol] = int(data["next_funding_time"])
                        
                        # Get open interest
                        url = f"{self.BASE_REST_URL}/linear-swap-api/v1/swap_open_interest"
                        params = {"contract_code": htx_symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("status") == "ok":
                                data_list = result.get("data", [])
                                if data_list:
                                    self.open_interest[symbol] = float(data_list[0].get("volume", 0))
                        
                        # Get index and mark price
                        url = f"{self.BASE_REST_URL}/linear-swap-api/v1/swap_index"
                        params = {"contract_code": htx_symbol}
                        resp = await client.get(url, params=params, timeout=10)
                        if resp.status_code == 200:
                            result = resp.json()
                            if result.get("status") == "ok":
                                data_list = result.get("data", [])
                                if data_list:
                                    self.index_prices[symbol] = float(data_list[0].get("index_price", 0))

                        await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Error polling HTX market data: {e}")
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
            "exchange": "htx",
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
        mark = self.mark_prices.get(symbol, 0)
        index = self.index_prices.get(symbol, 0)
        oi = self.open_interest.get(symbol, 0)

        # Use best bid/ask as mark price if not available
        if mark == 0:
            ob = self.orderbooks.get(symbol)
            if ob and ob["bids"] and ob["asks"]:
                best_bid = max(ob["bids"].keys())
                best_ask = min(ob["asks"].keys())
                mark = (best_bid + best_ask) / 2

        if mark == 0:
            return None

        basis = mark - index if index > 0 else 0
        basis_bps = (basis / index * 10000) if index > 0 else 0

        return {
            "exchange": "htx",
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
