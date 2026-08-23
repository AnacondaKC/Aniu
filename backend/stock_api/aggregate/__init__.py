"""Composite research snapshots built from the active stock-data clients."""

from backend.stock_api.aggregate.market_snapshot import MarketSnapshotAggregator
from backend.stock_api.aggregate.snapshots import (
    IndustrySnapshotAggregator,
    PortfolioStockSnapshotAggregator,
    StockAnalysisAggregator,
)

__all__ = [
    "IndustrySnapshotAggregator",
    "MarketSnapshotAggregator",
    "PortfolioStockSnapshotAggregator",
    "StockAnalysisAggregator",
]
