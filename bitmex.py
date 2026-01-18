import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class BitMEXClient:
    """BitMEX Realtime WebSocket client for perpetual contracts."""
    
    BASE_WS_URL = "wss://ws.bitmex.com/realtime"
    BASE_REST_URL = "https://www.bitmex.com"

    # BitMEX symbol mapping (BTCUSDT -> XBTUSD)
    SYMBOL_MAP = {
        "BTCUSDT": "XBTUSD",
        "ETHUSDT": "ETHUSD",
    }
    REVERSE_MAP = {v: k for k, v in SYMBOL_MAP.items()}

    def __init__(self, symbols: list[str]):
        self.symbols = [self._to_bitmex_symbol(s) for s in symbols]
        self.original_symbols = {self._to_bitmex_symbol(s): s.upper() for s in symbols}
        
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 1
        self.max_reconnect_delay = 60
        
        # Callbacks
        self.on_trade: Optional[Callable] = None
        self.on_depth: Optional[Callable] = None
        self.on_liquidation: Optional[Callable] = None
        self.on_mark_price: Optional[Callable] = None

        # Orderbook state - BitMEX uses ID-keyed orderbook
        self.orderbooks: dict[str, dict] = {}  # symbol -> {id: {price, size, side}}
        self.orderbook_prices: dict[str, dict] = {}  # symbol -> {bids: {price: qty}, asks: {price: qty}}
        self.initialized: dict[str, bool] = {}

        # Market data state
        self.mark_prices: dict[str, float] = {}
        self.index_prices: dict[str, float] = {}
        self.funding_rates: dict[str, float] = {}
        self.next_funding_times: dict[str, int] = {}
        self.open_interest: dict[str, float] = {}

    def _to_bitmex_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to XBTUSD."""
        symbol = symbol.upper()
        return self.SYMBOL_MAP.get(symbol, symbol)

    def _to_standard_symbol(self, bitmex_symbol: str) -> str:
        """Convert XBTUSD back to BTCUSDT."""
        return self.original_symbols.get(bitmex_symbol, self.REVERSE_MAP.get(bitmex_symbol, bitmex_symbol))

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
                # Build subscription URL
                subs = []
                for sym in self.symbols:
                    subs.extend([
                        f"trade:{sym}",
                        f"orderBookL2_25:{sym}",
                        f"liquidation:{sym}",
                        f"instrument:{sym}",
                    ])
                url = f"{self.BASE_WS_URL}?subscribe={','.join(subs)}"
                
                logger.info(f"Connecting to BitMEX: {url[:100]}...")
                async with websockets.connect(url, ping_interval=30) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to BitMEX")

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"BitMEX WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            
            # Handle info/welcome messages
            if "info" in data or "success" in data:
                logger.debug(f"BitMEX: {data.get('info', data.get('success'))}")
                return

            if "error" in data:
                logger.warning(f"BitMEX error: {data}")
                return

            table = data.get("table", "")
            action = data.get("action", "")

            if table == "trade":
                await self._handle_trade(data)
            elif table == "orderBookL2_25":
                await self._handle_depth(data, action)
            elif table == "liquidation":
                await self._handle_liquidation(data)
            elif table == "instrument":
                self._handle_instrument(data)

        except Exception as e:
            logger.error(f"Error handling BitMEX message: {e}")

    async def _handle_trade(self, data: dict):
        for t in data.get("data", []):
            bitmex_symbol = t["symbol"]
            symbol = self._to_standard_symbol(bitmex_symbol)
            
            # BitMEX size is in contracts, price is in USD
            price = float(t["price"])
            size = t["size"]
            
            # Convert contracts to base currency (approximate)
            # For XBT: 1 contract = 1 USD worth of BTC
            quantity = size / price if price > 0 else 0

            trade = {
                "exchange": "bitmex",
                "symbol": symbol,
                "price": price,
                "quantity": quantity,
                "quoteQty": float(size),  # Size in USD
                "isBuyerMaker": t["side"] == "Sell",
                "timestamp": int(time.time() * 1000),  # BitMEX uses ISO timestamp
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, data: dict, action: str):
        items = data.get("data", [])
        if not items:
            return

        symbol = items[0].get("symbol", "")
        std_symbol = self._to_standard_symbol(symbol)

        if action == "partial":
            # Full snapshot - initialize orderbook
            self.orderbooks[symbol] = {}
            self.orderbook_prices[std_symbol] = {"bids": {}, "asks": {}}
            
            for item in items:
                self.orderbooks[symbol][item["id"]] = {
                    "price": item["price"],
                    "size": item["size"],
                    "side": item["side"],
                }
                
                price = float(item["price"])
                qty = float(item["size"])
                if item["side"] == "Buy":
                    self.orderbook_prices[std_symbol]["bids"][price] = qty
                else:
                    self.orderbook_prices[std_symbol]["asks"][price] = qty
            
            self.initialized[std_symbol] = True
            logger.info(f"BitMEX orderbook initialized for {std_symbol}")

        elif action in ["insert", "update", "delete"]:
            if not self.initialized.get(std_symbol, False):
                return

            for item in items:
                id_ = item["id"]
                
                if action == "delete":
                    if id_ in self.orderbooks.get(symbol, {}):
                        old = self.orderbooks[symbol].pop(id_)
                        price = float(old["price"])
                        if old["side"] == "Buy":
                            self.orderbook_prices[std_symbol]["bids"].pop(price, None)
                        else:
                            self.orderbook_prices[std_symbol]["asks"].pop(price, None)
                
                elif action == "insert":
                    self.orderbooks.setdefault(symbol, {})[id_] = {
                        "price": item["price"],
                        "size": item["size"],
                        "side": item["side"],
                    }
                    price = float(item["price"])
                    qty = float(item["size"])
                    if item["side"] == "Buy":
                        self.orderbook_prices[std_symbol]["bids"][price] = qty
                    else:
                        self.orderbook_prices[std_symbol]["asks"][price] = qty
                
                elif action == "update":
                    if id_ in self.orderbooks.get(symbol, {}):
                        old = self.orderbooks[symbol][id_]
                        old["size"] = item.get("size", old["size"])
                        price = float(old["price"])
                        qty = float(old["size"])
                        if old["side"] == "Buy":
                            self.orderbook_prices[std_symbol]["bids"][price] = qty
                        else:
                            self.orderbook_prices[std_symbol]["asks"][price] = qty

        await self._emit_orderbook(std_symbol)

    async def _emit_orderbook(self, symbol: str):
        ob = self.orderbook_prices.get(symbol)
        if not ob or not self.on_depth:
            return

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        orderbook = {
            "exchange": "bitmex",
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
            bitmex_symbol = liq.get("symbol", "")
            symbol = self._to_standard_symbol(bitmex_symbol)
            
            price = float(liq.get("price", 0))
            leaves_qty = liq.get("leavesQty", 0)
            
            # Convert contracts to approximate quantity
            quantity = leaves_qty / price if price > 0 else 0

            liquidation = {
                "exchange": "bitmex",
                "symbol": symbol,
                "side": liq.get("side", "").upper(),
                "price": price,
                "quantity": quantity,
                "notionalUsd": float(leaves_qty),
                "timestamp": int(time.time() * 1000),
            }

            if self.on_liquidation:
                await self.on_liquidation(liquidation)

    def _handle_instrument(self, data: dict):
        for item in data.get("data", []):
            bitmex_symbol = item.get("symbol", "")
            symbol = self._to_standard_symbol(bitmex_symbol)
            
            if item.get("markPrice"):
                self.mark_prices[symbol] = float(item["markPrice"])
            if item.get("indicativeSettlePrice"):
                self.index_prices[symbol] = float(item["indicativeSettlePrice"])
            if item.get("fundingRate"):
                self.funding_rates[symbol] = float(item["fundingRate"])
            if item.get("fundingTimestamp"):
                # Parse ISO timestamp
                self.next_funding_times[symbol] = int(time.time() * 1000)  # Simplified
            if item.get("openInterest"):
                self.open_interest[symbol] = float(item["openInterest"])

            if self.on_mark_price and item.get("markPrice"):
                asyncio.create_task(self.on_mark_price({
                    "symbol": symbol,
                    "markPrice": self.mark_prices.get(symbol, 0),
                    "indexPrice": self.index_prices.get(symbol, 0),
                    "fundingRate": self.funding_rates.get(symbol, 0),
                    "nextFundingTime": self.next_funding_times.get(symbol, 0),
                }))

    def get_orderbook(self, symbol: str) -> Optional[dict]:
        symbol = symbol.upper()
        ob = self.orderbook_prices.get(symbol)
        if not ob:
            return None

        sorted_bids = sorted(ob["bids"].items(), key=lambda x: -x[0])
        sorted_asks = sorted(ob["asks"].items(), key=lambda x: x[0])

        best_bid = sorted_bids[0][0] if sorted_bids else 0
        best_ask = sorted_asks[0][0] if sorted_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

        return {
            "exchange": "bitmex",
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
            "exchange": "bitmex",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "fundingRate": self.funding_rates.get(symbol, 0),
            "nextFundingTime": self.next_funding_times.get(symbol, 0),
            "openInterest": oi,
            "openInterestUsd": oi,  # BitMEX OI is in contracts (USD)
            "indexPrice": index,
            "markPrice": mark,
            "basis": basis,
            "basisBps": basis_bps,
        }
