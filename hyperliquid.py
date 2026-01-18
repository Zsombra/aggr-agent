import asyncio
import json
import logging
import time
from typing import Callable, Optional
import websockets
import httpx

logger = logging.getLogger(__name__)


class HyperliquidClient:
    """Hyperliquid WebSocket client for perpetual contracts."""
    
    BASE_WS_URL = "wss://api.hyperliquid.xyz/ws"
    BASE_REST_URL = "https://api.hyperliquid.xyz"

    # Hyperliquid uses asset indices - map common symbols
    SYMBOL_MAP = {
        "BTCUSDT": "BTC",
        "ETHUSDT": "ETH",
    }
    REVERSE_MAP = {v: k for k, v in SYMBOL_MAP.items()}

    def __init__(self, symbols: list[str]):
        self.symbols = [self._to_hl_symbol(s) for s in symbols]
        self.original_symbols = {self._to_hl_symbol(s): s.upper() for s in symbols}
        
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
        
        # Asset metadata
        self.asset_to_idx: dict[str, int] = {}

    def _to_hl_symbol(self, symbol: str) -> str:
        """Convert BTCUSDT to BTC (Hyperliquid format)."""
        symbol = symbol.upper()
        return self.SYMBOL_MAP.get(symbol, symbol.replace("USDT", ""))

    def _to_standard_symbol(self, hl_symbol: str) -> str:
        """Convert BTC back to BTCUSDT."""
        return self.original_symbols.get(hl_symbol, f"{hl_symbol}USDT")

    async def start(self):
        self.running = True
        await self._fetch_asset_metadata()
        asyncio.create_task(self._run_ws())
        asyncio.create_task(self._poll_market_data())

    async def stop(self):
        self.running = False
        if self.ws:
            await self.ws.close()

    async def _fetch_asset_metadata(self):
        """Fetch asset indices from Hyperliquid."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{self.BASE_REST_URL}/info",
                    json={"type": "meta"},
                    timeout=10
                )
                if resp.status_code == 200:
                    data = resp.json()
                    universe = data.get("universe", [])
                    for idx, asset in enumerate(universe):
                        name = asset.get("name", "")
                        self.asset_to_idx[name] = idx
                    logger.info(f"Loaded {len(self.asset_to_idx)} Hyperliquid assets")
        except Exception as e:
            logger.error(f"Error fetching Hyperliquid metadata: {e}")

    async def _run_ws(self):
        while self.running:
            try:
                logger.info(f"Connecting to Hyperliquid: {self.BASE_WS_URL}")
                async with websockets.connect(
                    self.BASE_WS_URL,
                    ping_interval=30
                ) as ws:
                    self.ws = ws
                    self.reconnect_delay = 1
                    logger.info("Connected to Hyperliquid")

                    await self._subscribe(ws)

                    async for msg in ws:
                        await self._handle_message(msg)

            except Exception as e:
                logger.error(f"Hyperliquid WebSocket error: {e}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(
                    self.reconnect_delay * 2,
                    self.max_reconnect_delay
                )

    async def _subscribe(self, ws):
        """Subscribe to all streams."""
        for sym in self.symbols:
            # Subscribe to trades
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": sym}
            }))
            
            # Subscribe to L2 orderbook
            await ws.send(json.dumps({
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": sym}
            }))
            
            await asyncio.sleep(0.1)
        
        # Subscribe to all liquidations
        await ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {"type": "allMids"}  # For price updates
        }))

        logger.info(f"Subscribed to Hyperliquid streams for {len(self.symbols)} symbols")

    async def _handle_message(self, msg: str):
        try:
            data = json.loads(msg)
            
            channel = data.get("channel", "")
            
            if channel == "trades":
                await self._handle_trade(data)
            elif channel == "l2Book":
                await self._handle_depth(data)
            elif channel == "allMids":
                self._handle_all_mids(data)
            elif channel == "subscriptionResponse":
                logger.debug(f"Hyperliquid subscription: {data}")

        except Exception as e:
            logger.error(f"Error handling Hyperliquid message: {e}")

    async def _handle_trade(self, data: dict):
        trades = data.get("data", [])
        for t in trades:
            coin = t.get("coin", "")
            if coin not in self.symbols:
                continue
                
            symbol = self._to_standard_symbol(coin)
            price = float(t["px"])
            size = float(t["sz"])
            
            trade = {
                "exchange": "hyperliquid",
                "symbol": symbol,
                "price": price,
                "quantity": size,
                "quoteQty": price * size,
                "isBuyerMaker": t.get("side", "").upper() == "A",  # A = ask/sell
                "timestamp": t.get("time", int(time.time() * 1000)),
            }

            if self.on_trade:
                await self.on_trade(trade)

    async def _handle_depth(self, data: dict):
        book_data = data.get("data", {})
        coin = book_data.get("coin", "")
        
        if coin not in self.symbols:
            return
            
        symbol = self._to_standard_symbol(coin)
        
        # Hyperliquid L2 book format
        levels = book_data.get("levels", [[], []])
        
        bids = {}
        asks = {}
        
        # levels[0] = bids, levels[1] = asks
        for level in levels[0]:
            price = float(level.get("px", 0))
            qty = float(level.get("sz", 0))
            if qty > 0:
                bids[price] = qty
                
        for level in levels[1]:
            price = float(level.get("px", 0))
            qty = float(level.get("sz", 0))
            if qty > 0:
                asks[price] = qty

        self.orderbooks[symbol] = {"bids": bids, "asks": asks}
        
        if not self.initialized.get(symbol, False):
            self.initialized[symbol] = True
            logger.info(f"Hyperliquid orderbook initialized for {symbol}")

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
            "exchange": "hyperliquid",
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "midPrice": mid_price,
            "bids": [{"price": p, "quantity": q} for p, q in sorted_bids],
            "asks": [{"price": p, "quantity": q} for p, q in sorted_asks],
        }

        await self.on_depth(orderbook)

    def _handle_all_mids(self, data: dict):
        """Handle mid price updates for all assets."""
        mids = data.get("data", {}).get("mids", {})
        for coin, mid in mids.items():
            if coin in self.symbols:
                symbol = self._to_standard_symbol(coin)
                self.mark_prices[symbol] = float(mid)

    async def _poll_market_data(self):
        """Poll funding rates and open interest."""
        while self.running:
            try:
                async with httpx.AsyncClient() as client:
                    # Get asset contexts (funding, OI, mark price)
                    resp = await client.post(
                        f"{self.BASE_REST_URL}/info",
                        json={"type": "metaAndAssetCtxs"},
                        timeout=10
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        if len(result) >= 2:
                            asset_ctxs = result[1]
                            meta = result[0]
                            universe = meta.get("universe", [])
                            
                            for idx, ctx in enumerate(asset_ctxs):
                                if idx < len(universe):
                                    coin = universe[idx].get("name", "")
                                    if coin in self.symbols:
                                        symbol = self._to_standard_symbol(coin)
                                        
                                        if ctx.get("markPx"):
                                            self.mark_prices[symbol] = float(ctx["markPx"])
                                        if ctx.get("oraclePx"):
                                            self.index_prices[symbol] = float(ctx["oraclePx"])
                                        if ctx.get("funding"):
                                            self.funding_rates[symbol] = float(ctx["funding"])
                                        if ctx.get("openInterest"):
                                            self.open_interest[symbol] = float(ctx["openInterest"])

            except Exception as e:
                logger.error(f"Error polling Hyperliquid market data: {e}")
            await asyncio.sleep(60)  # Poll every minute

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
            "exchange": "hyperliquid",
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
        
        if mark == 0:
            return None

        index = self.index_prices.get(symbol, 0)
        oi = self.open_interest.get(symbol, 0)

        basis = mark - index if index > 0 else 0
        basis_bps = (basis / index * 10000) if index > 0 else 0

        return {
            "exchange": "hyperliquid",
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
