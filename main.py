import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv

from collector import HTTPConfig, MarketDataCollector
from database import Database
from features import latest_feature_row


logger = logging.getLogger(__name__)
shutdown_requested = False


@dataclass(frozen=True)
class AppConfig:
    symbols: list[str]
    interval: str
    loop_seconds: int
    db_path: str
    log_level: str
    http: HTTPConfig


def load_config() -> AppConfig:
    load_dotenv()
    symbols = [
        symbol.strip().upper()
        for symbol in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
        if symbol.strip()
    ]
    return AppConfig(
        symbols=symbols,
        interval=os.getenv("INTERVAL", "5m"),
        loop_seconds=int(os.getenv("LOOP_SECONDS", "300")),
        db_path=os.getenv("DB_PATH", "market_data.sqlite3"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        http=HTTPConfig(
            timeout_seconds=float(os.getenv("HTTP_TIMEOUT_SECONDS", "10")),
            max_retries=int(os.getenv("HTTP_MAX_RETRIES", "5")),
            backoff_seconds=float(os.getenv("HTTP_BACKOFF_SECONDS", "1.5")),
        ),
    )


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)sZ %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.Formatter.converter = time.gmtime


def handle_shutdown(signum: int, _frame: object) -> None:
    global shutdown_requested
    shutdown_requested = True
    logger.info("Shutdown signal received: %s", signum)


def run_once(config: AppConfig, collector: MarketDataCollector, db: Database) -> None:
    loop_started = datetime.now(timezone.utc)
    logger.info(
        "Collector tick started at %s for symbols=%s interval=%s",
        loop_started.isoformat(),
        ",".join(config.symbols),
        config.interval,
    )

    rows = []
    for symbol in config.symbols:
        try:
            row = collector.collect(symbol, config.interval)
            rows.append(row)
            logger.info(
                "%s close=%.4f timestamp=%s oi=%s funding=%s ls_ratio=%s",
                symbol,
                row["close"],
                row["timestamp"],
                _fmt(row.get("open_interest")),
                _fmt(row.get("funding_rate")),
                _fmt(row.get("long_short_ratio")),
            )
        except Exception:
            logger.exception("Failed to collect symbol=%s", symbol)

    changed = db.upsert_market_data(rows)
    logger.info("DB upsert complete rows_changed=%s", changed)

    for symbol in config.symbols:
        try:
            history = db.load_market_data(symbol, limit=500)
            feature_row = latest_feature_row(history)
            if feature_row:
                logger.info(
                    "%s features rsi=%s ema20=%s ema50=%s returns=%s volatility=%s oi_delta=%s",
                    symbol,
                    _fmt(feature_row.get("rsi")),
                    _fmt(feature_row.get("ema_20")),
                    _fmt(feature_row.get("ema_50")),
                    _fmt(feature_row.get("returns")),
                    _fmt(feature_row.get("volatility_rolling")),
                    _fmt(feature_row.get("open_interest_delta")),
                )
        except Exception:
            logger.exception("Failed to calculate features for symbol=%s", symbol)


def run_forever() -> None:
    config = load_config()
    setup_logging(config.log_level)
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    collector = MarketDataCollector(config.http)
    db = Database(config.db_path)

    logger.info("Starting collector loop. db=%s loop_seconds=%s", config.db_path, config.loop_seconds)
    while not shutdown_requested:
        started = time.monotonic()
        run_once(config, collector, db)
        elapsed = time.monotonic() - started
        sleep_for = max(0, config.loop_seconds - elapsed)
        logger.info("Tick finished elapsed=%.2fs sleeping=%.2fs", elapsed, sleep_for)
        _interruptible_sleep(sleep_for)

    logger.info("Collector stopped cleanly")


def _interruptible_sleep(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not shutdown_requested and time.monotonic() < deadline:
        time.sleep(min(1, deadline - time.monotonic()))


def _fmt(value: object) -> str:
    if value is None:
        return "NA"
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return str(value)


if __name__ == "__main__":
    run_forever()
