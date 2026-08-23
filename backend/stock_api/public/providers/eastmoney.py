"""EastMoney adapter for fixed public live-market, announcement, and news APIs."""

from __future__ import annotations

import json
from collections.abc import Mapping

from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.contracts import (
    AnnouncementsRequest,
    ConnectMoneyFlowRequest,
    IndexKlineRequest,
    IndexQuoteRequest,
    IntradayRequest,
    KlineRequest,
    MarketBreadthRequest,
    NewsFeedRequest,
    NewsSearchRequest,
    QuoteSnapshotRequest,
    SectorMoneyFlowRequest,
    SectorRankingRequest,
    StockMoneyFlowHistoryRequest,
    StockMoneyFlowIntradayRequest,
    StockRankingRequest,
    market_symbol_code,
    market_symbol_market,
)
from backend.stock_api.public.errors import UpstreamUnavailable
from backend.stock_api.public.providers.base import (
    build_url,
    parse_jsonp,
    public_headers,
)
from backend.stock_api.public.providers.eastmoney_research import EastMoneyF10Adapter

_UT = "fa5fd1943c7b386f172d6893dbfba10b"
_EASTMONEY_REFERER = "https://data.eastmoney.com/"
_FIELDS_QUOTE = ",".join(
    (
        "f43",
        "f44",
        "f45",
        "f46",
        "f47",
        "f48",
        "f57",
        "f58",
        "f60",
        "f107",
        "f116",
        "f117",
        "f86",
        "f162",
        "f167",
        "f168",
        "f169",
        "f170",
        "f171",
        "f177",
        "f292",
    )
)
_FIELDS_BATCH = "f1,f2,f3,f4,f5,f6,f8,f12,f13,f14,f15,f16,f17,f18,f104,f105,f106,f124"
_BREADTH_SECIDS = "1.000001,0.399001"
_FIELDS_LIST = "f2,f3,f4,f5,f6,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f8,f10,f62"
_FIELDS_KLINE_1 = "f1,f2,f3,f4,f5,f6"
_FIELDS_KLINE_2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
_FIELDS_TREND_1 = "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13"
_FIELDS_TREND_2 = "f51,f52,f53,f54,f55,f56,f57,f58"
_FIELDS_FLOW_1 = "f1,f2,f3,f7"
_FIELDS_FLOW_2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
_MARKETS = {
    "all_a": "m:0+t:6,m:1+t:2",
    "sh_a": "m:1+t:2",
    "sz_a": "m:0+t:6",
    "chinext": "m:0+t:13+s:2048",
    "star": "m:0+t:81+s:2048",
    "industry": "m:90+t:2",
    "concept": "m:90+t:3",
}
_SORT_FIELDS = {
    "price": "f2",
    "change_percent": "f3",
    "volume": "f5",
    "amount": "f6",
    "turnover_rate": "f8",
    "net_inflow": "f62",
}
_PERIODS = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "60m": 60,
    "day": 101,
    "week": 102,
    "month": 103,
}
_ADJUSTMENTS = {"none": 0, "qfq": 1, "hfq": 2}


class EastMoneyAdapter(EastMoneyF10Adapter):
    """Concrete adapter combining live-market and F10 source actions."""

    async def quote_snapshot(
        self,
        request: QuoteSnapshotRequest | IndexQuoteRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        if len(request.symbols) == 1 and request.detail == "full":
            secid = _secid(request.symbols[0])
            params = {
                "secid": secid,
                "fltt": 2,
                "invt": 2,
                "ut": _UT,
                "fields": _FIELDS_QUOTE,
            }
            return await self._market_json(
                operation=request.operation,
                endpoint="em_realtime_quote",
                origin="https://push2.eastmoney.com",
                path="/api/qt/stock/get",
                parameters=params,
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
            )
        secids = ",".join(_secid(symbol) for symbol in request.symbols)
        params = {"secids": secids, "fltt": 2, "invt": 2, "fields": _FIELDS_BATCH}
        return await self._market_json(
            operation=request.operation,
            endpoint="em_batch_quotes",
            origin="https://push2.eastmoney.com",
            path="/api/qt/ulist.np/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def breadth(
        self,
        request: MarketBreadthRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._market_json(
            operation=request.operation,
            endpoint="em_batch_quotes",
            origin="https://push2.eastmoney.com",
            path="/api/qt/ulist.np/get",
            parameters={
                "secids": _BREADTH_SECIDS,
                "fltt": 2,
                "invt": 2,
                "fields": _FIELDS_BATCH,
            },
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def kline(
        self,
        request: KlineRequest | IndexKlineRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        index_request = isinstance(request, IndexKlineRequest)
        params = {
            "secid": _secid(request.symbol),
            "fields1": _FIELDS_KLINE_1,
            "fields2": _FIELDS_KLINE_2,
            "ut": _UT,
            "klt": _PERIODS[request.period],
            "fqt": _ADJUSTMENTS[request.adjust],
            "beg": (
                request.start_date.replace("-", "")
                if request.start_date
                else "0"
                if index_request
                else "19900101"
            ),
            "end": (
                request.end_date.replace("-", "")
                if request.end_date
                else "20500000"
                if index_request
                else "20500101"
            ),
            "lmt": request.limit,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_price_history",
            origin="https://push2his.eastmoney.com",
            path="/api/qt/stock/kline/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def index_kline_1m(
        self,
        request: IndexKlineRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        params = {
            "secid": _secid(request.symbol),
            "fields1": _FIELDS_TREND_1,
            "fields2": _FIELDS_TREND_2,
            "ndays": 5,
            "iscr": 0,
            "iscca": 0,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_index_intraday",
            origin="https://push2his.eastmoney.com",
            path="/api/qt/stock/trends2/get",
            parameters=params,
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
        params = {
            "secid": _secid(request.symbol),
            "fields1": _FIELDS_TREND_1,
            "fields2": _FIELDS_TREND_2,
            "ndays": request.days,
            "iscr": 0,
            "iscca": 0,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_intraday_trends",
            origin="https://push2his.eastmoney.com",
            path="/api/qt/stock/trends2/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def stock_ranking(
        self,
        request: StockRankingRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._market_snapshot(
            operation=request.operation,
            market=request.market,
            sort=request.sort,
            order=request.order,
            page=request.page,
            limit=request.limit,
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
        return await self._market_snapshot(
            operation=request.operation,
            market=request.sector_type,
            sort=request.sort,
            order=request.order,
            page=request.page,
            limit=request.limit,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def stock_money_flow_history(
        self,
        request: StockMoneyFlowHistoryRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        params = {
            "secid": _secid(request.symbol),
            "lmt": request.page * request.limit,
            "klt": 101,
            "fields1": _FIELDS_FLOW_1,
            "fields2": _FIELDS_FLOW_2,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_money_flow_history",
            origin="https://push2his.eastmoney.com",
            path="/api/qt/stock/fflow/daykline/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def stock_money_flow_intraday(
        self,
        request: StockMoneyFlowIntradayRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        params = {
            "secid": _secid(request.symbol),
            "lmt": request.limit,
            "klt": 1,
            "fields1": _FIELDS_FLOW_1,
            "fields2": _FIELDS_FLOW_2,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_money_flow_intraday",
            origin="https://push2.eastmoney.com",
            path="/api/qt/stock/fflow/kline/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def sector_money_flow(
        self,
        request: SectorMoneyFlowRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._market_snapshot(
            operation=request.operation,
            market=request.sector_type,
            sort="net_inflow",
            order="desc",
            page=request.page,
            limit=request.limit,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def connect_money_flow(
        self,
        request: ConnectMoneyFlowRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        params = {
            "fields1": "f1,f2,f3,f4",
            "fields2": "f51,f54,f52,f58,f53,f62,f56,f57,f60,f61",
            "ut": _UT,
        }
        return await self._market_json(
            operation=request.operation,
            endpoint="em_connect_flow",
            origin="https://push2.eastmoney.com",
            path="/api/qt/kamtbs.rtmin/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def announcements(
        self,
        request: AnnouncementsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        code = market_symbol_code(request.symbol)
        params = {
            "market_stock_list": f"{_market_id(request.symbol)}.{code}",
            "page_index": request.page,
            "page_size": request.limit,
        }
        payload = await self._json(
            operation=request.operation,
            endpoint="em_stock_announcements",
            url=build_url(
                "https://np-anotice-pc.eastmoney.com", "/api/security/ann", params
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        root = _record(payload, "东方财富公告")
        data = _record(root.get("data"), "东方财富公告")
        rows = data.get("list")
        if not isinstance(rows, list):
            raise UpstreamUnavailable("东方财富公告响应缺少列表。")
        return {"total": data.get("total_hits", 0), "announcements": rows}

    async def feed(
        self,
        request: NewsFeedRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        if request.feed in {"headlines", "flash"}:
            column = 101 if request.feed == "headlines" else 102
            endpoint = "em_top_news" if column == 101 else "em_kuaixun"
            source = await self._text(
                operation=request.operation,
                endpoint=endpoint,
                url=(
                    "https://newsapi.eastmoney.com/kuaixun/v1/"
                    f"getlist_{column}_ajaxResult_{request.limit}_1_.html"
                ),
                parameters={"limit": request.limit},
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
                headers=public_headers(referer=_EASTMONEY_REFERER),
                lane="eastmoney_f10",
                parser=parse_jsonp,
            )
            root = _record(source, "东方财富资讯")
            rows = root.get("LivesList")
            if not isinstance(rows, list):
                raise UpstreamUnavailable("东方财富资讯响应缺少列表。")
            return {"items": rows}
        params = {
            "client": "web",
            "biz": "web_news_col",
            "column": 345 if request.feed == "finance" else 346,
            "order": 1,
            "needInteractData": 0,
            "page_index": request.page,
            "page_size": request.limit,
            "req_trace": 1,
        }
        payload = await self._json(
            operation=request.operation,
            endpoint="em_channel_news",
            url=build_url(
                "https://np-listapi.eastmoney.com", "/comm/web/getNewsByColumns", params
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
        )
        root = _record(payload, "东方财富频道资讯")
        data = _record(root.get("data"), "东方财富频道资讯")
        rows = data.get("list")
        if not isinstance(rows, list):
            raise UpstreamUnavailable("东方财富频道资讯响应缺少列表。")
        return {"items": rows}

    async def news_search(
        self,
        request: NewsSearchRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._search(
            request,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def _search(
        self,
        request: NewsSearchRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        page = request.page
        parameter = {
            "uid": "",
            "keyword": request.keyword,
            "type": ["cmsArticleWebOld"],
            "client": "web",
            "clientType": "web",
            "clientVersion": "curr",
            "param": {
                "cmsArticleWebOld": {
                    "searchScope": "default",
                    "sort": "default",
                    "pageIndex": page,
                    "pageSize": request.limit,
                    "preTag": "",
                    "postTag": "",
                }
            },
        }
        params = {
            "cb": "aniuStockCallback",
            "param": json.dumps(parameter, ensure_ascii=False, separators=(",", ":")),
        }
        source = await self._text(
            operation=request.operation,
            endpoint="em_global_search",
            url=build_url(
                "https://search-api-web.eastmoney.com", "/search/jsonp", params
            ),
            parameters={
                "keyword": request.keyword,
                "page": page,
                "limit": request.limit,
            },
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer="https://so.eastmoney.com/"),
            lane="eastmoney_f10",
            parser=parse_jsonp,
        )
        return source

    async def _market_snapshot(
        self,
        *,
        operation: str,
        market: str,
        sort: str,
        order: str,
        page: int,
        limit: int,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        params = {
            "pn": page,
            "pz": limit,
            "po": 1 if order == "desc" else 0,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": _SORT_FIELDS[sort],
            "fs": _MARKETS[market],
            "fields": _FIELDS_LIST,
        }
        return await self._market_json(
            operation=operation,
            endpoint="em_market_snapshot",
            origin="https://push2.eastmoney.com",
            path="/api/qt/clist/get",
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def _market_json(
        self,
        *,
        operation: str,
        endpoint: str,
        origin: str,
        path: str,
        parameters: Mapping[str, object],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        url = build_url(origin, path, parameters)
        fallback_origin = (
            "https://push2delay.eastmoney.com"
            if origin
            in {"https://push2.eastmoney.com", "https://push2his.eastmoney.com"}
            else None
        )
        payload = await self._json(
            operation=operation,
            endpoint=endpoint,
            url=url,
            parameters=parameters,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_push",
            fallback_urls=(build_url(fallback_origin, path, parameters),)
            if fallback_origin
            else (),
        )
        root = _record(payload, "东方财富")
        if root.get("rc") not in {0, "0"}:
            message = root.get("msg") or root.get("message") or "未知错误"
            raise UpstreamUnavailable(f"东方财富业务请求失败：{message}。")
        return payload


def _secid(symbol: str) -> str:
    market = "1" if market_symbol_market(symbol) == "SH" else "0"
    return f"{market}.{market_symbol_code(symbol)}"


def _market_id(symbol: str) -> int:
    return 1 if market_symbol_market(symbol) == "SH" else 0


def _record(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise UpstreamUnavailable(f"{label}响应无效。")
    return value


__all__ = ["EastMoneyAdapter"]
