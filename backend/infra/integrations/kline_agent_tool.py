"""Agent-facing K-line query with deterministic instrument routing."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, cast

from backend.infra.integrations.tool_policy import SideEffectLevel
from backend.llm import AbortSignal, ProviderJsonObject, ToolDefinition
from backend.stock_api.public import (
    IndexKlineRequest,
    InvalidStockRequest,
    KlineRequest,
    StockMarketDataService,
    bound_agent_result,
)
from backend.stock_api.public.contracts import normalize_symbol

READ_STAGES = ("Run",)

type KlineInstrumentType = Literal["stock", "index"]
type KlinePeriod = Literal["1m", "5m", "15m", "30m", "60m", "day", "week", "month"]
type KlineAdjust = Literal["none", "qfq", "hfq"]

_KLINE_PERIODS: tuple[KlinePeriod, ...] = (
    "1m",
    "5m",
    "15m",
    "30m",
    "60m",
    "day",
    "week",
    "month",
)
_KLINE_ADJUSTMENTS: tuple[KlineAdjust, ...] = ("none", "qfq", "hfq")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SH_INDEX = re.compile(r"^000\d{3}\.SH$")
_SZ_INDEX = re.compile(r"^399\d{3}\.SZ$")
_BARE_SZ_INDEX = re.compile(r"^399\d{3}$")

_INDEX_NAME_BY_SYMBOL = {
    "000001.SH": "上证指数",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "000688.SH": "科创50",
    "000852.SH": "中证1000",
    "000905.SH": "中证500",
    "399001.SZ": "深证成指",
    "399005.SZ": "中小100",
    "399006.SZ": "创业板指",
    "399330.SZ": "深证100",
}
_INDEX_SYMBOL_BY_ALIAS = {
    "上证指数": "000001.SH",
    "上证综指": "000001.SH",
    "上证50": "000016.SH",
    "沪深300": "000300.SH",
    "科创50": "000688.SH",
    "中证1000": "000852.SH",
    "中证500": "000905.SH",
    "深证成指": "399001.SZ",
    "深证综指": "399106.SZ",
    "中小100": "399005.SZ",
    "创业板指": "399006.SZ",
    "深证100": "399330.SZ",
}


@dataclass(frozen=True, slots=True)
class ResolvedKlineInstrument:
    instrument_type: KlineInstrumentType
    symbol: str
    name: str | None = None


def resolve_kline_instrument(value: object) -> ResolvedKlineInstrument:
    """Resolve an unambiguous A-share or mainland index identifier."""

    if not isinstance(value, str):
        raise InvalidStockRequest("K 线标的必须是字符串。")
    normalized = value.strip().upper()
    if not normalized:
        raise InvalidStockRequest("K 线标的不能为空。")

    alias_symbol = _INDEX_SYMBOL_BY_ALIAS.get(normalized)
    if alias_symbol is not None:
        return ResolvedKlineInstrument(
            instrument_type="index",
            symbol=alias_symbol,
            name=_INDEX_NAME_BY_SYMBOL.get(alias_symbol, value.strip()),
        )

    if _BARE_SZ_INDEX.fullmatch(normalized):
        normalized = f"{normalized}.SZ"
    if _SH_INDEX.fullmatch(normalized) or _SZ_INDEX.fullmatch(normalized):
        return ResolvedKlineInstrument(
            instrument_type="index",
            symbol=normalized,
            name=_INDEX_NAME_BY_SYMBOL.get(normalized),
        )

    try:
        symbol = normalize_symbol(normalized)
    except InvalidStockRequest as exc:
        raise InvalidStockRequest(
            "K 线标的无法识别；请使用沪深 A 股代码，或明确的指数代码/名称。"
        ) from exc
    return ResolvedKlineInstrument(instrument_type="stock", symbol=symbol)


def _validated_options(
    period: object,
    adjust: object,
    start_date: object,
    end_date: object,
    limit: object,
) -> tuple[KlinePeriod, KlineAdjust, str | None, str | None, int]:
    if period not in _KLINE_PERIODS:
        raise InvalidStockRequest("period 参数无效。")
    if adjust not in _KLINE_ADJUSTMENTS:
        raise InvalidStockRequest("adjust 参数无效。")
    normalized_dates: list[str | None] = []
    for value, name in ((start_date, "start_date"), (end_date, "end_date")):
        if value is None:
            normalized_dates.append(None)
            continue
        if not isinstance(value, str) or _DATE.fullmatch(value) is None:
            raise InvalidStockRequest(f"{name} 必须是 YYYY-MM-DD 格式。")
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise InvalidStockRequest(f"{name} 不是有效日期。") from exc
        normalized_dates.append(value)
    normalized_start, normalized_end = normalized_dates
    if normalized_start and normalized_end and normalized_start > normalized_end:
        raise InvalidStockRequest("start_date 不能晚于 end_date。")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 300:
        raise InvalidStockRequest("limit 必须是 1 到 300 之间的整数。")
    return (
        period,
        adjust,
        normalized_start,
        normalized_end,
        limit,
    )


@dataclass(slots=True)
class QueryKlineTool:
    public_service: StockMarketDataService
    name: str = "query_kline"
    enabled_stages: tuple[str, ...] = field(default=READ_STAGES)
    side_effect_level: SideEffectLevel = SideEffectLevel.READ
    execution_mode: str = "parallel"

    def to_tool_definition(self) -> ToolDefinition:
        return {
            "name": self.name,
            "description": (
                "统一查询沪深 A 股个股或指数的日、周、月及分钟 K 线。"
                "工具会自动识别标的类型并选择数据源；所有个股和指数 K 线查询"
                "都应使用本工具，不支持行业或概念板块。"
            ),
            "parameters": cast(
                ProviderJsonObject,
                {
                    "type": "object",
                    "properties": {
                        "instrument": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 32,
                            "description": (
                                "个股代码或指数代码/名称。示例：600519、000001.SZ、"
                                "000001.SH、399006.SZ、上证指数、创业板指。"
                            ),
                        },
                        "period": {
                            "type": "string",
                            "enum": list(_KLINE_PERIODS),
                            "default": "day",
                        },
                        "adjust": {
                            "type": "string",
                            "enum": list(_KLINE_ADJUSTMENTS),
                            "default": "qfq",
                            "description": "个股复权方式；查询指数时忽略。",
                        },
                        "start_date": {
                            "type": "string",
                            "format": "date",
                            "pattern": r"^\d{4}-\d{2}-\d{2}$",
                        },
                        "end_date": {
                            "type": "string",
                            "format": "date",
                            "pattern": r"^\d{4}-\d{2}-\d{2}$",
                        },
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 300,
                            "default": 120,
                        },
                    },
                    "required": ["instrument"],
                    "additionalProperties": False,
                },
            ),
        }

    def stock_api_log_parameters(
        self, parameters: dict[str, object]
    ) -> dict[str, object]:
        try:
            instrument = resolve_kline_instrument(parameters.get("instrument"))
        except InvalidStockRequest:
            return parameters
        return {
            **parameters,
            "instrument_type": instrument.instrument_type,
            "resolved_instrument": instrument.symbol,
            "data_source": "public",
        }

    async def run(
        self,
        instrument: str,
        period: str = "day",
        adjust: str = "qfq",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 120,
    ) -> object:
        return await self.run_with_abort(
            instrument,
            period,
            adjust,
            start_date,
            end_date,
            limit,
            abort_signal=None,
        )

    async def run_with_abort(
        self,
        instrument: str,
        period: str = "day",
        adjust: str = "qfq",
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 120,
        *,
        abort_signal: AbortSignal | None,
    ) -> object:
        resolved = resolve_kline_instrument(instrument)
        normalized = _validated_options(period, adjust, start_date, end_date, limit)
        normalized_period, normalized_adjust, normalized_start, normalized_end, size = (
            normalized
        )
        if resolved.instrument_type == "stock":
            stock_request = KlineRequest(
                symbol=resolved.symbol,
                period=normalized_period,
                adjust=normalized_adjust,
                start_date=normalized_start,
                end_date=normalized_end,
                limit=size,
            )
            result = await self.public_service.execute(stock_request, abort_signal)
            meta = result.get("meta")
            annotated = {
                **result,
                "meta": {
                    **(meta if isinstance(meta, dict) else {}),
                    "instrument_type": "stock",
                    "resolved_instrument": resolved.symbol,
                    "data_source": "public",
                },
            }
            return bound_agent_result(annotated, stock_request)

        index_request = IndexKlineRequest(
            symbol=resolved.symbol,
            period=normalized_period,
            start_date=normalized_start,
            end_date=normalized_end,
            limit=size,
        )
        result = await self.public_service.execute(index_request, abort_signal)
        meta = result.get("meta")
        annotated = {
            **result,
            "meta": {
                **(meta if isinstance(meta, dict) else {}),
                "instrument_type": "index",
                "resolved_instrument": resolved.symbol,
                "data_source": "public",
            },
        }
        return bound_agent_result(annotated, index_request)


__all__ = [
    "QueryKlineTool",
    "ResolvedKlineInstrument",
    "resolve_kline_instrument",
]
