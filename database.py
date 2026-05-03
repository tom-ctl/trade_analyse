import logging
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Optional

import pandas as pd

logger = logging.getLogger(__name__)


MARKET_DATA_COLUMNS = (
    "timestamp",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "funding_rate",
    "long_short_ratio",
)


class Database:
    """Small SQLite repository for market data.

    SQLite is enough for a VPS collector if writes are short, indexed, and
    idempotent. WAL mode keeps reads from blocking the 5-minute writer loop.
    """

    def __init__(self, path: str = "market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data (
                    timestamp INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    open_interest REAL,
                    funding_rate REAL,
                    long_short_ratio REAL,
                    PRIMARY KEY (timestamp, symbol)
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_market_data_timestamp "
                "ON market_data(timestamp);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_market_data_symbol "
                "ON market_data(symbol);"
            )

    def upsert_market_data(self, rows: Iterable[Mapping[str, object]]) -> int:
        rows = list(rows)
        if not rows:
            return 0

        values = [
            tuple(row.get(column) for column in MARKET_DATA_COLUMNS)
            for row in rows
        ]

        with self.connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT INTO market_data (
                    timestamp, symbol, open, high, low, close, volume,
                    open_interest, funding_rate, long_short_ratio
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(timestamp, symbol) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    open_interest = excluded.open_interest,
                    funding_rate = excluded.funding_rate,
                    long_short_ratio = excluded.long_short_ratio;
                """,
                values,
            )
            changed = conn.total_changes - before

        logger.debug("Upserted %s market_data rows", changed)
        return changed

    def load_market_data(
        self,
        symbol: str,
        limit: int = 500,
        since_timestamp: Optional[int] = None,
    ) -> pd.DataFrame:
        query = "SELECT * FROM market_data WHERE symbol = ?"
        params: list[object] = [symbol]
        if since_timestamp is not None:
            query += " AND timestamp >= ?"
            params.append(since_timestamp)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with self.connect() as conn:
            df = pd.read_sql_query(query, conn, params=params)

        if df.empty:
            return df
        return df.sort_values("timestamp").reset_index(drop=True)
