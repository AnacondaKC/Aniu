"""Strong business contracts for normalized public A-share data."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date
from typing import ClassVar, Literal

from backend.stock_api.public.errors import InvalidStockRequest

type ProviderName = Literal["eastmoney", "tencent", "sina"]

_SH_A_SHARE = re.compile(r"^(?:600|601|603|605|688)\d{3}$")
_SZ_A_SHARE = re.compile(r"^(?:000|001|002|003|300|301)\d{3}$")
_SH_INDEX = re.compile(r"^000\d{3}$")
_SZ_INDEX = re.compile(r"^399\d{3}$")
_SYMBOL = re.compile(r"^(\d{6})(?:\.(SH|SZ))?$")


def normalize_symbol(value: str) -> str:
    """Return one accepted A-share code in ``600519.SH`` form."""

    if not isinstance(value, str):
        raise InvalidStockRequest("证券代码必须是字符串。")
    normalized = value.strip().upper()
    match = _SYMBOL.fullmatch(normalized)
    if match is None:
        raise InvalidStockRequest(
            "证券代码格式无效，请使用 600519、600519.SH 或 000001.SZ。"
        )
    code, market = match.groups()
    expected_market: Literal["SH", "SZ"]
    if _SH_A_SHARE.fullmatch(code):
        expected_market = "SH"
    elif _SZ_A_SHARE.fullmatch(code):
        expected_market = "SZ"
    else:
        raise InvalidStockRequest(
            "仅支持沪深 A 股证券代码；指数行情请使用 query_market_data。"
        )
    if market is not None and market != expected_market:
        raise InvalidStockRequest(
            "仅支持沪深 A 股证券代码；指数行情请使用 query_market_data。"
        )
    return f"{code}.{expected_market}"


def normalize_index_symbol(value: str) -> str:
    """Return one accepted mainland index code with an explicit exchange."""

    if not isinstance(value, str):
        raise InvalidStockRequest("指数代码必须是字符串。")
    normalized = value.strip().upper()
    match = _SYMBOL.fullmatch(normalized)
    if match is None or match.group(2) is None:
        raise InvalidStockRequest("指数代码必须使用 000001.SH 或 399006.SZ 格式。")
    code, market = match.groups()
    valid = (market == "SH" and _SH_INDEX.fullmatch(code)) or (
        market == "SZ" and _SZ_INDEX.fullmatch(code)
    )
    if not valid:
        raise InvalidStockRequest("仅支持沪深指数代码。")
    return f"{code}.{market}"


def market_symbol_code(value: str) -> str:
    match = _SYMBOL.fullmatch(value.strip().upper()) if isinstance(value, str) else None
    if match is None or match.group(2) is None:
        raise InvalidStockRequest("证券代码必须包含交易所后缀。")
    return match.group(1)


def market_symbol_market(value: str) -> Literal["SH", "SZ"]:
    match = _SYMBOL.fullmatch(value.strip().upper()) if isinstance(value, str) else None
    if match is None or match.group(2) is None:
        raise InvalidStockRequest("证券代码必须包含交易所后缀。")
    return "SH" if match.group(2) == "SH" else "SZ"


def symbol_code(value: str) -> str:
    return normalize_symbol(value)[:6]


def symbol_market(value: str) -> Literal["SH", "SZ"]:
    market = normalize_symbol(value)[-2:]
    if market == "SH":
        return "SH"
    return "SZ"


def _choice(value: object, name: str, choices: tuple[object, ...]) -> None:
    if value not in choices:
        raise InvalidStockRequest(
            f"{name} 必须为：{'、'.join(str(item) for item in choices)}。"
        )


def _integer(value: object, name: str, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise InvalidStockRequest(f"{name} 必须是 {minimum} 到 {maximum} 之间的整数。")


def _page_limit(page: int, limit: int, *, maximum_page: int = 100) -> None:
    _integer(page, "page", 1, maximum_page)
    _integer(limit, "limit", 1, 50)


def _date(value: str | None, name: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise InvalidStockRequest(f"{name} 必须为 yyyy-MM-dd 日期。")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise InvalidStockRequest(f"{name} 不是有效日期。") from exc


@dataclass(frozen=True, slots=True)
class MarketDataRequest:
    """Base type for normalized public market-data requests."""

    operation: ClassVar[str]

    def cache_payload(self) -> dict[str, object]:
        return {"operation": self.operation, **asdict(self)}


@dataclass(frozen=True, slots=True)
class StockMarketRequest(MarketDataRequest):
    """Base type for requests restricted to A-share securities."""


@dataclass(frozen=True, slots=True)
class IndexQuoteRequest(MarketDataRequest):
    symbols: tuple[str, ...]
    detail: ClassVar[Literal["full"]] = "full"

    operation: ClassVar[str] = "index.quote"

    def __post_init__(self) -> None:
        values = tuple(normalize_index_symbol(value) for value in self.symbols)
        if not 1 <= len(values) <= 20:
            raise InvalidStockRequest("symbols 必须包含 1 到 20 个指数代码。")
        if len(set(values)) != len(values):
            raise InvalidStockRequest("symbols 不允许包含重复指数代码。")
        object.__setattr__(self, "symbols", values)


@dataclass(frozen=True, slots=True)
class MarketBreadthRequest(MarketDataRequest):
    operation: ClassVar[str] = "market.breadth"


@dataclass(frozen=True, slots=True)
class IndexKlineRequest(MarketDataRequest):
    symbol: str
    period: Literal["1m", "5m", "15m", "30m", "60m", "day", "week", "month"] = "day"
    start_date: str | None = None
    end_date: str | None = None
    limit: int = 5
    adjust: ClassVar[Literal["none"]] = "none"

    operation: ClassVar[str] = "index.kline"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_index_symbol(self.symbol))
        _choice(
            self.period,
            "period",
            ("1m", "5m", "15m", "30m", "60m", "day", "week", "month"),
        )
        _date(self.start_date, "start_date")
        _date(self.end_date, "end_date")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise InvalidStockRequest("start_date 不能晚于 end_date。")
        _integer(self.limit, "limit", 1, 300)


@dataclass(frozen=True, slots=True)
class QuoteSnapshotRequest(StockMarketRequest):
    symbols: tuple[str, ...]
    detail: Literal["basic", "full"] = "basic"

    operation: ClassVar[str] = "quote.snapshot"

    def __post_init__(self) -> None:
        values = tuple(normalize_symbol(value) for value in self.symbols)
        if not 1 <= len(values) <= 60:
            raise InvalidStockRequest("symbols 必须包含 1 到 60 个证券代码。")
        if len(set(values)) != len(values):
            raise InvalidStockRequest("symbols 不允许包含重复证券代码。")
        _choice(self.detail, "detail", ("basic", "full"))
        if self.detail == "full" and len(values) > 20:
            raise InvalidStockRequest("detail=full 时最多查询 20 个证券代码。")
        object.__setattr__(self, "symbols", values)


@dataclass(frozen=True, slots=True)
class KlineRequest(StockMarketRequest):
    symbol: str
    period: Literal["1m", "5m", "15m", "30m", "60m", "day", "week", "month"] = "day"
    adjust: Literal["none", "qfq", "hfq"] = "qfq"
    start_date: str | None = None
    end_date: str | None = None
    limit: int = 120

    operation: ClassVar[str] = "chart.kline"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _choice(
            self.period,
            "period",
            ("1m", "5m", "15m", "30m", "60m", "day", "week", "month"),
        )
        _choice(self.adjust, "adjust", ("none", "qfq", "hfq"))
        _date(self.start_date, "start_date")
        _date(self.end_date, "end_date")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise InvalidStockRequest("start_date 不能晚于 end_date。")
        _integer(self.limit, "limit", 1, 300)


@dataclass(frozen=True, slots=True)
class IntradayRequest(StockMarketRequest):
    symbol: str
    days: Literal[1, 5] = 1
    limit: int = 120

    operation: ClassVar[str] = "chart.intraday"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _choice(self.days, "days", (1, 5))
        _integer(self.limit, "limit", 1, 300)


def _ranking_values(sort: object, order: object, page: int, limit: int) -> None:
    _choice(
        sort,
        "sort",
        ("price", "change_percent", "volume", "amount", "turnover_rate", "net_inflow"),
    )
    _choice(order, "order", ("asc", "desc"))
    _page_limit(page, limit)


@dataclass(frozen=True, slots=True)
class StockRankingRequest(StockMarketRequest):
    market: Literal["all_a", "sh_a", "sz_a", "chinext", "star"] = "all_a"
    sort: Literal[
        "price", "change_percent", "volume", "amount", "turnover_rate", "net_inflow"
    ] = "change_percent"
    order: Literal["asc", "desc"] = "desc"
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "ranking.stocks"

    def __post_init__(self) -> None:
        _choice(self.market, "market", ("all_a", "sh_a", "sz_a", "chinext", "star"))
        _ranking_values(self.sort, self.order, self.page, self.limit)


@dataclass(frozen=True, slots=True)
class SectorRankingRequest(StockMarketRequest):
    sector_type: Literal["industry", "concept"] = "industry"
    sort: Literal[
        "price", "change_percent", "volume", "amount", "turnover_rate", "net_inflow"
    ] = "change_percent"
    order: Literal["asc", "desc"] = "desc"
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "ranking.sectors"

    def __post_init__(self) -> None:
        _choice(self.sector_type, "sector_type", ("industry", "concept"))
        _ranking_values(self.sort, self.order, self.page, self.limit)


@dataclass(frozen=True, slots=True)
class StockMoneyFlowHistoryRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "money_flow.stock_history"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit, maximum_page=20)


@dataclass(frozen=True, slots=True)
class StockMoneyFlowIntradayRequest(StockMarketRequest):
    symbol: str
    limit: int = 120

    operation: ClassVar[str] = "money_flow.stock_intraday"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _integer(self.limit, "limit", 1, 300)


@dataclass(frozen=True, slots=True)
class SectorMoneyFlowRequest(StockMarketRequest):
    sector_type: Literal["industry", "concept"] = "industry"
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "money_flow.sector"

    def __post_init__(self) -> None:
        _choice(self.sector_type, "sector_type", ("industry", "concept"))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class ConnectMoneyFlowRequest(StockMarketRequest):
    direction: Literal["all", "northbound", "southbound"] = "all"
    limit: int = 120

    operation: ClassVar[str] = "money_flow.connect"

    def __post_init__(self) -> None:
        _choice(self.direction, "direction", ("all", "northbound", "southbound"))
        _integer(self.limit, "limit", 1, 300)


@dataclass(frozen=True, slots=True)
class FinancialsRequest(StockMarketRequest):
    symbol: str
    mode: Literal["latest", "quarterly"] = "latest"
    page: int | None = None
    limit: int | None = None

    operation: ClassVar[str] = "fundamentals.financials"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _choice(self.mode, "mode", ("latest", "quarterly"))
        if self.mode == "latest":
            if self.page is not None or self.limit is not None:
                raise InvalidStockRequest(
                    "financials mode=latest 不接受 page 或 limit。"
                )
            return
        page = 1 if self.page is None else self.page
        limit = 20 if self.limit is None else self.limit
        _page_limit(page, limit)
        object.__setattr__(self, "page", page)
        object.__setattr__(self, "limit", limit)


@dataclass(frozen=True, slots=True)
class ShareholdersRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "fundamentals.shareholders"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class ValuationRequest(StockMarketRequest):
    symbol: str

    operation: ClassVar[str] = "fundamentals.valuation"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))


@dataclass(frozen=True, slots=True)
class IndustryComparisonRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "fundamentals.industry_comparison"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class OperatingIndicatorsRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "fundamentals.operating_indicators"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class MarketReportsRequest(StockMarketRequest):
    category: Literal["strategy", "macro", "broker", "industry"] = "strategy"
    days: int | None = None
    top: int = 3
    index: int | None = None
    content_max_characters: int | None = 4_000

    operation: ClassVar[str] = "research.market_reports"

    def __post_init__(self) -> None:
        _choice(self.category, "category", ("strategy", "macro", "broker", "industry"))
        days = (
            (0 if self.category == "broker" else 1) if self.days is None else self.days
        )
        _integer(days, "days", 0, 30)
        _integer(self.top, "top", 1, 10)
        if self.content_max_characters is not None:
            _integer(self.content_max_characters, "content_max_characters", 1, 100_000)
        if self.index is not None:
            _integer(self.index, "index", 1, 100)
        object.__setattr__(self, "days", days)


@dataclass(frozen=True, slots=True)
class StockReportsRequest(StockMarketRequest):
    symbol: str
    page: int | None = None
    limit: int | None = None
    content: Literal["summary", "full"] = "summary"
    report_id: str | None = None
    summary_max_characters: int | None = 1_000

    operation: ClassVar[str] = "research.stock_reports"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _choice(self.content, "content", ("summary", "full"))
        if self.report_id is not None and (
            not isinstance(self.report_id, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.report_id) is None
        ):
            raise InvalidStockRequest("report_id 格式无效。")
        if self.content == "full":
            if self.page is not None or self.limit is not None:
                raise InvalidStockRequest(
                    "stock_reports content=full 不接受 page 或 limit。"
                )
            if self.report_id is None:
                raise InvalidStockRequest("content=full 时必须提供 report_id。")
            return
        if self.report_id is not None:
            raise InvalidStockRequest("report_id 只能与 content=full 一起使用。")
        if self.summary_max_characters is not None:
            _integer(
                self.summary_max_characters,
                "summary_max_characters",
                1,
                100_000,
            )
        page = 1 if self.page is None else self.page
        limit = 5 if self.limit is None else self.limit
        _page_limit(page, limit)
        if limit > 5:
            raise InvalidStockRequest("stock_reports 最多返回 5 篇研报。")
        object.__setattr__(self, "page", page)
        object.__setattr__(self, "limit", limit)


@dataclass(frozen=True, slots=True)
class ForecastRequest(StockMarketRequest):
    symbol: str
    mode: Literal["summary", "institutions"] = "summary"
    page: int | None = None
    limit: int | None = None

    operation: ClassVar[str] = "research.forecast"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _choice(self.mode, "mode", ("summary", "institutions"))
        if self.mode == "summary":
            if self.page is not None or self.limit is not None:
                raise InvalidStockRequest(
                    "forecast mode=summary 不接受 page 或 limit。"
                )
            return
        page = 1 if self.page is None else self.page
        limit = 20 if self.limit is None else self.limit
        _page_limit(page, limit)
        object.__setattr__(self, "page", page)
        object.__setattr__(self, "limit", limit)


@dataclass(frozen=True, slots=True)
class RatingsRequest(StockMarketRequest):
    symbol: str

    operation: ClassVar[str] = "research.ratings"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))


@dataclass(frozen=True, slots=True)
class NewsFeedRequest(StockMarketRequest):
    feed: Literal["headlines", "flash", "finance", "global", "stocks", "money"] = (
        "headlines"
    )
    page: int = 1
    limit: int | None = None

    operation: ClassVar[str] = "news.feed"

    def __post_init__(self) -> None:
        _choice(
            self.feed,
            "feed",
            ("headlines", "flash", "finance", "global", "stocks", "money"),
        )
        if self.feed in {"headlines", "flash"} and self.page != 1:
            raise InvalidStockRequest(f"feed={self.feed} 仅支持 page=1。")
        _integer(self.page, "page", 1, 100)
        default = 30 if self.feed == "flash" else 10
        limit = default if self.limit is None else self.limit
        maximum = 21 if self.feed == "headlines" else 50
        _integer(limit, "limit", 1, maximum)
        object.__setattr__(self, "limit", limit)


@dataclass(frozen=True, slots=True)
class StockNewsRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 10

    operation: ClassVar[str] = "news.stock_news"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class AnnouncementsRequest(StockMarketRequest):
    symbol: str
    page: int = 1
    limit: int = 20

    operation: ClassVar[str] = "news.announcements"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", normalize_symbol(self.symbol))
        _page_limit(self.page, self.limit)


@dataclass(frozen=True, slots=True)
class NewsSearchRequest(StockMarketRequest):
    keyword: str
    page: int = 1
    limit: int = 10

    operation: ClassVar[str] = "news.search"

    def __post_init__(self) -> None:
        if not isinstance(self.keyword, str):
            raise InvalidStockRequest("keyword 必须是字符串。")
        keyword = self.keyword.strip()
        if not 1 <= len(keyword) <= 100:
            raise InvalidStockRequest("keyword 长度必须为 1 到 100 个字符。")
        _integer(self.page, "page", 1, 100)
        _integer(self.limit, "limit", 1, 20)
        object.__setattr__(self, "keyword", keyword)


type PublicStockRequest = (
    IndexQuoteRequest
    | MarketBreadthRequest
    | IndexKlineRequest
    | QuoteSnapshotRequest
    | KlineRequest
    | IntradayRequest
    | StockRankingRequest
    | SectorRankingRequest
    | StockMoneyFlowHistoryRequest
    | StockMoneyFlowIntradayRequest
    | SectorMoneyFlowRequest
    | ConnectMoneyFlowRequest
    | FinancialsRequest
    | ShareholdersRequest
    | ValuationRequest
    | IndustryComparisonRequest
    | OperatingIndicatorsRequest
    | MarketReportsRequest
    | StockReportsRequest
    | ForecastRequest
    | RatingsRequest
    | NewsFeedRequest
    | StockNewsRequest
    | AnnouncementsRequest
    | NewsSearchRequest
)


__all__ = [
    "AnnouncementsRequest",
    "ConnectMoneyFlowRequest",
    "FinancialsRequest",
    "ForecastRequest",
    "IndustryComparisonRequest",
    "IndexKlineRequest",
    "IndexQuoteRequest",
    "IntradayRequest",
    "KlineRequest",
    "MarketBreadthRequest",
    "MarketDataRequest",
    "MarketReportsRequest",
    "NewsFeedRequest",
    "NewsSearchRequest",
    "OperatingIndicatorsRequest",
    "ProviderName",
    "PublicStockRequest",
    "QuoteSnapshotRequest",
    "RatingsRequest",
    "SectorMoneyFlowRequest",
    "SectorRankingRequest",
    "ShareholdersRequest",
    "StockMarketRequest",
    "StockMoneyFlowHistoryRequest",
    "StockMoneyFlowIntradayRequest",
    "StockNewsRequest",
    "StockRankingRequest",
    "StockReportsRequest",
    "ValuationRequest",
    "market_symbol_code",
    "market_symbol_market",
    "normalize_index_symbol",
    "normalize_symbol",
    "symbol_code",
    "symbol_market",
]
