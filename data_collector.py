import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_FUTURES_DATA_URL = "https://fapi.binance.com"
BYBIT_BASE_URL = "https://api.bybit.com"
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"


class APIError(RuntimeError):
    pass


@dataclass(frozen=True)
class HTTPConfig:
    timeout_seconds: float = 10
    max_retries: int = 5
    backoff_seconds: float = 1.5


class APIClient:
    def __init__(self, config: HTTPConfig) -> None:
        self.config = config
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "trade-bot-data-collector/1.0",
            }
        )

    def get(self, url: str, params: Optional[dict[str, Any]] = None) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.config.max_retries + 1):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds,
                )
                if response.status_code in {418, 429}:
                    retry_after = _parse_retry_after(response)
                    sleep_for = retry_after or self._sleep_seconds(attempt)
                    logger.warning(
                        "Rate limited by %s. Sleeping %.2fs",
                        response.url,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
                    continue

                if 500 <= response.status_code < 600:
                    raise APIError(f"Server error {response.status_code}: {response.text[:200]}")

                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError, APIError) as exc:
                last_error = exc
                if attempt >= self.config.max_retries:
                    break
                sleep_for = self._sleep_seconds(attempt)
                logger.warning(
                    "API request failed (%s/%s): %s. Retrying in %.2fs",
                    attempt,
                    self.config.max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)

        raise APIError(f"API request failed after retries: {url}") from last_error

    def _sleep_seconds(self, attempt: int) -> float:
        return self.config.backoff_seconds * (2 ** (attempt - 1))


class DataCollector:
    def __init__(self, http_config: HTTPConfig) -> None:
        self.client = APIClient(http_config)

    def collect_symbol_snapshot(self, symbol: str, interval: str = "5m") -> dict[str, Any]:
        market_row = self.fetch_binance_market_data(symbol, interval)
        bybit = self.fetch_bybit_microstructure(symbol)
        return {"market_data": market_row, "bybit": bybit}

    def fetch_binance_market_data(self, symbol: str, interval: str = "5m") -> dict[str, Any]:
        kline = self.fetch_latest_closed_kline(symbol, interval)
        return {
            "timestamp": kline["timestamp"],
            "symbol": symbol,
            "open": kline["open"],
            "high": kline["high"],
            "low": kline["low"],
            "close": kline["close"],
            "volume": kline["volume"],
            "open_interest": self.fetch_open_interest(symbol),
            "funding_rate": self.fetch_funding_rate(symbol),
            "long_short_ratio": self.fetch_long_short_ratio(symbol, interval),
        }

    def fetch_latest_closed_kline(self, symbol: str, interval: str) -> dict[str, Any]:
        payload = self.client.get(
            f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": 3},
        )
        if not payload:
            raise APIError(f"No kline returned for {symbol}")

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        closed = [row for row in payload if int(row[6]) <= now_ms]
        row = closed[-1] if closed else payload[-1]

        return {
            "timestamp": int(row[0]) // 1000,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
        }

    def fetch_open_interest(self, symbol: str) -> Optional[float]:
        payload = self.client.get(
            f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/openInterest",
            {"symbol": symbol},
        )
        return _safe_float(payload.get("openInterest"))

    def fetch_funding_rate(self, symbol: str) -> Optional[float]:
        payload = self.client.get(
            f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": 1},
        )
        if not payload:
            return None
        return _safe_float(payload[-1].get("fundingRate"))

    def fetch_long_short_ratio(self, symbol: str, interval: str = "5m") -> Optional[float]:
        period = interval if interval in {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"} else "5m"
        payload = self.client.get(
            f"{BINANCE_FUTURES_DATA_URL}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": 1},
        )
        if not payload:
            return None
        return _safe_float(payload[-1].get("longShortRatio"))

    def fetch_bybit_microstructure(self, symbol: str) -> dict[str, Any]:
        result = {
            "orderbook": None,
            "trades": [],
            "liquidations": [],
        }
        try:
            orderbook = self.client.get(
                f"{BYBIT_BASE_URL}/v5/market/orderbook",
                {"category": "linear", "symbol": symbol, "limit": 50},
            )
            result["orderbook"] = orderbook.get("result")
        except APIError as exc:
            logger.warning("Bybit orderbook unavailable for %s: %s", symbol, exc)

        try:
            trades = self.client.get(
                f"{BYBIT_BASE_URL}/v5/market/recent-trade",
                {"category": "linear", "symbol": symbol, "limit": 50},
            )
            result["trades"] = trades.get("result", {}).get("list", [])
        except APIError as exc:
            logger.warning("Bybit trades unavailable for %s: %s", symbol, exc)

        try:
            result["liquidations"] = self.fetch_bybit_liquidations(symbol)
        except APIError as exc:
            logger.warning("Bybit liquidations unavailable for %s: %s", symbol, exc)

        return result

    def fetch_bybit_liquidations(self, symbol: str) -> list[dict[str, Any]]:
        """Return recent liquidation events when a REST source is available.

        Bybit V5 currently exposes public liquidation flow through WebSocket
        topics such as allLiquidation. The REST collector keeps this method as
        a stable extension point and returns an empty batch until the optional
        WebSocket worker is added.
        """

        logger.debug("Bybit liquidation REST source not available for %s", symbol)
        return []

    def fetch_coingecko_global(self) -> dict[str, Optional[float]]:
        payload = self.client.get(f"{COINGECKO_BASE_URL}/global")
        data = payload.get("data", {})
        market_caps = data.get("total_market_cap", {})
        dominance = data.get("market_cap_percentage", {})
        return {
            "global_market_cap_usd": _safe_float(market_caps.get("usd")),
            "btc_dominance": _safe_float(dominance.get("btc")),
        }


def summarize_bybit(bybit: dict[str, Any]) -> dict[str, Any]:
    orderbook = bybit.get("orderbook") or {}
    bids = orderbook.get("b") or []
    asks = orderbook.get("a") or []
    best_bid = _safe_float(bids[0][0]) if bids else None
    best_ask = _safe_float(asks[0][0]) if asks else None
    spread = (best_ask - best_bid) if best_bid is not None and best_ask is not None else None
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "trades_count": len(bybit.get("trades") or []),
        "liquidations_count": len(bybit.get("liquidations") or []),
    }


def _parse_retry_after(response: requests.Response) -> Optional[float]:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
