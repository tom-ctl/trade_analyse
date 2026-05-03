import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


BINANCE_FUTURES_BASE_URL = "https://fapi.binance.com"
BINANCE_FUTURES_DATA_URL = "https://fapi.binance.com"


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
                "User-Agent": "crypto-trading-data-collector/1.0",
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
                    sleep_for = _retry_after(response) or self._backoff(attempt)
                    logger.warning("Rate limited. sleeping=%.2fs url=%s", sleep_for, response.url)
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

                sleep_for = self._backoff(attempt)
                logger.warning(
                    "API request failed attempt=%s/%s error=%s sleeping=%.2fs",
                    attempt,
                    self.config.max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)

        raise APIError(f"API request failed after retries: {url}") from last_error

    def _backoff(self, attempt: int) -> float:
        return self.config.backoff_seconds * (2 ** (attempt - 1))


class MarketDataCollector:
    def __init__(self, http_config: HTTPConfig) -> None:
        self.client = APIClient(http_config)

    def collect(self, symbol: str, interval: str = "5m") -> dict[str, object]:
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

    def fetch_latest_closed_kline(self, symbol: str, interval: str) -> dict[str, float | int]:
        payload = self.client.get(
            f"{BINANCE_FUTURES_BASE_URL}/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": 3},
        )
        if not payload:
            raise APIError(f"No kline returned for {symbol}")

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        closed_klines = [row for row in payload if int(row[6]) <= now_ms]
        row = closed_klines[-1] if closed_klines else payload[-1]

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

    def fetch_long_short_ratio(self, symbol: str, interval: str) -> Optional[float]:
        valid_periods = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
        period = interval if interval in valid_periods else "5m"
        payload = self.client.get(
            f"{BINANCE_FUTURES_DATA_URL}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": period, "limit": 1},
        )
        if not payload:
            return None
        return _safe_float(payload[-1].get("longShortRatio"))


def _retry_after(response: requests.Response) -> Optional[float]:
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
