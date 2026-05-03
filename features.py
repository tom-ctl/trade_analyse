import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_features(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate trading features from market_data rows.

    The input must be sorted ascending by timestamp. The output keeps original
    columns and appends indicators used by downstream strategy code.
    """

    if df.empty:
        return df.copy()

    data = df.copy().sort_values("timestamp").reset_index(drop=True)
    data["returns"] = _log_returns(data["close"])
    data["rsi"] = _rsi(data["close"], window=14)
    data["ema_20"] = data["close"].ewm(span=20, adjust=False).mean()
    data["ema_50"] = data["close"].ewm(span=50, adjust=False).mean()
    data["atr"] = _atr(data, window=14)
    data["volatility_rolling"] = data["returns"].rolling(20).std()

    volume_mean = data["volume"].rolling(20).mean()
    volume_std = data["volume"].rolling(20).std()
    data["volume_spike"] = data["volume"] > (volume_mean + 2 * volume_std)
    data["open_interest_delta"] = data["open_interest"].diff()

    return data


def latest_feature_row(df: pd.DataFrame) -> dict[str, object]:
    features = calculate_features(df)
    if features.empty:
        return {}
    return features.iloc[-1].to_dict()


def _log_returns(close: pd.Series) -> pd.Series:
    return (close / close.shift(1)).apply(
        lambda value: float("nan") if pd.isna(value) else math.log(value)
    )


def _rsi(close: pd.Series, window: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))


def _atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    previous_close = df["close"].shift(1)
    true_range = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - previous_close).abs(),
            (df["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / window, min_periods=window, adjust=False).mean()
