import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

import pandas as pd
import yfinance as yf

from nasdaq_stream.config import PATHS
from nasdaq_stream.ingest.symbols import NASDAQ_SYMBOLS

LOGGER = logging.getLogger("download")

OHLC = ["Open", "High", "Low", "Close"]
COLUMNS = ["symbol", "bar_time", "open", "high", "low", "close", "volume"]


def _download(symbols: Sequence[str], period: str, threads: bool) -> pd.DataFrame:
    return yf.download(
        tickers=list(symbols),
        period=period,
        interval="1m",
        group_by="ticker",
        auto_adjust=True,
        threads=threads,
        progress=False,
    )


def _extract(raw: pd.DataFrame, symbol: str) -> pd.DataFrame | None:
    if isinstance(raw.columns, pd.MultiIndex):
        if symbol not in set(raw.columns.get_level_values(0)):
            return None
        raw = raw[symbol]
    frame = raw.dropna(subset=OHLC)
    if frame.empty:
        return None
    return pd.DataFrame(
        {
            "symbol": symbol,
            "bar_time": frame.index.tz_convert("UTC").astype("datetime64[us, UTC]"),
            "open": frame["Open"].round(4).to_numpy(dtype="float64"),
            "high": frame["High"].round(4).to_numpy(dtype="float64"),
            "low": frame["Low"].round(4).to_numpy(dtype="float64"),
            "close": frame["Close"].round(4).to_numpy(dtype="float64"),
            "volume": frame["Volume"].fillna(0).to_numpy(dtype="int64"),
        }
    )


def fetch_bars(symbols: Sequence[str], period: str) -> pd.DataFrame:
    raw = _download(symbols, period, threads=True)
    frames = {}
    for symbol in symbols:
        frame = _extract(raw, symbol)
        if frame is not None:
            frames[symbol] = frame
    for symbol in [s for s in symbols if s not in frames]:
        LOGGER.warning("retrying %s sequentially", symbol)
        frame = _extract(_download([symbol], period, threads=False), symbol)
        if frame is None:
            LOGGER.warning("no data for %s", symbol)
        else:
            frames[symbol] = frame
    if not frames:
        raise RuntimeError("yfinance returned no usable data")
    bars = pd.concat(frames.values(), ignore_index=True)
    bars = bars[(bars["open"] > 0) & (bars["high"] >= bars["low"])]
    return bars.sort_values(["bar_time", "symbol"], ignore_index=True)[COLUMNS]


def write_bars(bars: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    bars.to_parquet(output, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download 1-minute NASDAQ bars to Parquet")
    parser.add_argument("--symbols", nargs="+", default=list(NASDAQ_SYMBOLS))
    parser.add_argument("--period", default="7d")
    parser.add_argument("--output", type=Path, default=PATHS.bars)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    cache_dir = PATHS.data_dir / "yfinance_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    yf.set_tz_cache_location(str(cache_dir))
    bars = fetch_bars(args.symbols, args.period)
    write_bars(bars, args.output)
    LOGGER.info(
        "wrote %d bars for %d symbols (%s to %s) to %s",
        len(bars),
        bars["symbol"].nunique(),
        bars["bar_time"].min(),
        bars["bar_time"].max(),
        args.output,
    )


if __name__ == "__main__":
    main()
