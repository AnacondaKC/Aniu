"""Tencent Finance adapter for the fixed public A-share catalog."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import cast

from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.contracts import (
    IndexKlineRequest,
    IndexQuoteRequest,
    IntradayRequest,
    KlineRequest,
    QuoteSnapshotRequest,
    SectorRankingRequest,
    StockNewsRequest,
    StockRankingRequest,
    market_symbol_code,
    market_symbol_market,
)
from backend.stock_api.public.errors import UpstreamUnavailable
from backend.stock_api.public.providers.base import (
    FixedPublicAdapter,
    build_url,
    public_headers,
)


class TencentAdapter(FixedPublicAdapter):
    provider = "tencent"

    async def quote_snapshot(
        self,
        request: QuoteSnapshotRequest | IndexQuoteRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        symbols = ",".join(_tencent_symbol(item) for item in request.symbols)
        body = await self._text(
            operation=request.operation,
            endpoint="tx_realtime_quotes",
            url=f"https://qt.gtimg.cn/q={symbols}",
            parameters={"symbols": symbols},
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(),
            encoding="gbk",
        )
        return {"quotes": _parse_quotes(body)}

    async def kline(
        self,
        request: KlineRequest | IndexKlineRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        period = (
            request.period
            if request.period in {"day", "week", "month"}
            else f"m{request.period[:-1]}"
        )
        suffix = "" if request.adjust == "none" else request.adjust
        parameter = ",".join(
            (
                _tencent_symbol(request.symbol),
                period,
                request.start_date or "",
                request.end_date or "",
                str(request.limit),
                suffix,
            )
        )
        return await self._success_json(
            operation=request.operation,
            endpoint="tx_price_history",
            url=build_url(
                "https://web.ifzq.gtimg.cn",
                "/appstock/app/fqkline/get",
                {"param": parameter},
            ),
            parameters={
                "symbol": _tencent_symbol(request.symbol),
                "period": period,
                "limit": request.limit,
                "adjust": request.adjust,
            },
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def intraday(
        self,
        request: IntradayRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        symbol = _tencent_symbol(request.symbol)
        previous_close: float | None = None
        try:
            quote_body = await self._text(
                operation=request.operation,
                endpoint="tx_realtime_quotes",
                url=f"https://qt.gtimg.cn/q={symbol}",
                parameters={"symbol": symbol},
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
                headers=public_headers(),
                encoding="gbk",
            )
            quotes = _parse_quotes(quote_body)
            if quotes:
                value = quotes[0].get("previous_close")
                previous_close = (
                    float(value) if isinstance(value, (int, float)) else None
                )
        except UpstreamUnavailable:
            pass
        payload = await self._success_json(
            operation=request.operation,
            endpoint="tx_intraday_trends",
            url=build_url(
                "https://web.ifzq.gtimg.cn",
                "/appstock/app/minute/query",
                {"code": symbol},
            ),
            parameters={"symbol": symbol},
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )
        if isinstance(payload, dict):
            return {**payload, "previous_close": previous_close}
        return {"payload": payload, "previous_close": previous_close}

    async def stock_ranking(
        self,
        request: StockRankingRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        mapping = {
            "price": "price",
            "volume": "volume",
            "amount": "turnover",
        }
        sort = mapping.get(request.sort)
        if sort is None:
            raise UpstreamUnavailable("腾讯不支持该个股排行指标。", retryable=False)
        params = {
            "board_code": "aStock",
            "sort_type": sort,
            "direct": "down" if request.order == "desc" else "up",
            "offset": (request.page - 1) * request.limit,
            "count": request.limit,
        }
        return await self._success_json(
            operation=request.operation,
            endpoint="tx_stock_rank",
            url=build_url(
                "https://proxy.finance.qq.com",
                "/cgi/cgi-bin/rank/hs/getBoardRankList",
                params,
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def sector_ranking(
        self,
        request: SectorRankingRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        if request.sort not in {"price", "change_percent"}:
            raise UpstreamUnavailable("腾讯不支持该板块排行指标。", retryable=False)
        params = {
            "board_type": "hy" if request.sector_type == "industry" else "gn",
            "sort_type": "price",
            "direct": "down" if request.order == "desc" else "up",
            "offset": (request.page - 1) * request.limit,
            "count": request.limit,
        }
        return await self._success_json(
            operation=request.operation,
            endpoint="tx_sector_rank",
            url=build_url(
                "https://proxy.finance.qq.com", "/cgi/cgi-bin/rank/pt/getRank", params
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def stock_news(
        self,
        request: StockNewsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        symbol = _tencent_symbol(request.symbol)
        params = {
            "symbol": symbol,
            "n": request.limit,
            "page": request.page,
            "limit": request.limit,
            "type": 2,
        }
        return await self._success_json(
            operation=request.operation,
            endpoint="tx_stock_news",
            url=build_url(
                "https://web.ifzq.gtimg.cn", "/appstock/news/info/search", params
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def _success_json(
        self,
        *,
        operation: str,
        endpoint: str,
        url: str,
        parameters: Mapping[str, object],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        payload = await self._json(
            operation=operation,
            endpoint=endpoint,
            url=url,
            parameters=parameters,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(),
        )
        root = payload if isinstance(payload, dict) else None
        if root is None or root.get("code") not in {0, "0"}:
            message = (
                ""
                if root is None
                else str(root.get("message") or root.get("msg") or "")
            )
            raise UpstreamUnavailable(
                f"腾讯财经业务请求失败：{message or '未知错误'}。"
            )
        return payload


def _tencent_symbol(symbol: str) -> str:
    return f"{market_symbol_market(symbol).lower()}{market_symbol_code(symbol)}"


def _parse_quotes(source: str) -> list[dict[str, object]]:
    quotes: list[dict[str, object]] = []
    pattern = re.compile(r'(?:^|[\r\n])\s*v_([A-Za-z0-9_]+)="((?:\\.|[^"])*)";?')
    for match in pattern.finditer(source):
        encoded = match.group(2)
        try:
            fields = cast(str, json.loads(f'"{encoded}"')).split("~")
        except (TypeError, ValueError, json.JSONDecodeError):
            fields = encoded.split("~")
        quotes.append(
            {
                "symbol": match.group(1),
                "name": _field(fields, 1),
                "code": _field(fields, 2),
                "price": _number_field(fields, 3),
                "previous_close": _number_field(fields, 4),
                "open": _number_field(fields, 5),
                "volume_lots": _number_field(fields, 6),
                "market_time": _field(fields, 30),
                "change": _number_field(fields, 31),
                "change_percent": _number_field(fields, 32),
                "high": _number_field(fields, 33),
                "low": _number_field(fields, 34),
                "amount_ten_thousand_cny": _number_field(fields, 37),
                "turnover_rate": _number_field(fields, 38),
            }
        )
    if not quotes:
        raise UpstreamUnavailable("腾讯财经返回了无法解析的行情文本。")
    return quotes


def _field(values: list[str], index: int) -> str:
    return values[index] if index < len(values) else ""


def _number_field(values: list[str], index: int) -> float | None:
    try:
        return float(_field(values, index))
    except ValueError:
        return None


__all__ = ["TencentAdapter"]
