"""EastMoney F10 and research actions kept separate from live-market paths."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from backend.stock_api.public.cancellation import CancellationToken as AbortSignal
from backend.stock_api.public.contracts import (
    FinancialsRequest,
    ForecastRequest,
    IndustryComparisonRequest,
    MarketReportsRequest,
    OperatingIndicatorsRequest,
    RatingsRequest,
    ShareholdersRequest,
    StockReportsRequest,
    ValuationRequest,
    symbol_code,
    symbol_market,
)
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable
from backend.stock_api.public.providers.base import (
    FixedPublicAdapter,
    build_url,
    public_headers,
)

_EASTMONEY_REFERER = "https://data.eastmoney.com/"
_F10_REPORTS: dict[str, tuple[str, str | None, int]] = {
    "fundamentals.financials.latest": (
        "RPT_PCF10_FINANCEMAINFINADATA",
        "REPORT_DATE",
        -1,
    ),
    "fundamentals.financials.quarterly": (
        "RPT_F10_QTR_MAINFINADATA",
        "REPORT_DATE",
        -1,
    ),
    "fundamentals.shareholders": ("RPT_F10_EH_HOLDERNUM", "END_DATE", -1),
    "fundamentals.valuation": ("RPT_STOCKVALUATIONTANTILE", None, -1),
    "fundamentals.industry_comparison": (
        "RPT_F10_INDUSTRY_COMPARED",
        "REPORT_DATE",
        -1,
    ),
    "fundamentals.operating_indicators": ("RPTA_DATA_IF_INDICATOR", "UPDATE_DATE", -1),
    "research.forecast.institutions": (
        "RPT_HSF10_RES_ORGPREDICT",
        "PUBLISH_DATE",
        -1,
    ),
}


class EastMoneyF10Adapter(FixedPublicAdapter):
    """F10 and research methods shared by the concrete EastMoney adapter."""

    provider = "eastmoney"

    async def financials(
        self,
        request: FinancialsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        suffix = "latest" if request.mode == "latest" else "quarterly"
        return await self._f10(
            operation=request.operation,
            endpoint=(
                "em_financial_summary"
                if request.mode == "latest"
                else "em_quarterly_financials"
            ),
            symbol=request.symbol,
            page=request.page or 1,
            limit=request.limit or 1,
            descriptor=_F10_REPORTS[f"fundamentals.financials.{suffix}"],
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def shareholders(
        self,
        request: ShareholdersRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._f10(
            operation=request.operation,
            endpoint="em_shareholder_counts",
            symbol=request.symbol,
            page=request.page,
            limit=request.limit,
            descriptor=_F10_REPORTS[request.operation],
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def valuation(
        self,
        request: ValuationRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._f10(
            operation=request.operation,
            endpoint="em_valuation_percentiles",
            symbol=request.symbol,
            page=1,
            limit=20,
            descriptor=_F10_REPORTS[request.operation],
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def industry_comparison(
        self,
        request: IndustryComparisonRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._f10(
            operation=request.operation,
            endpoint="em_industry_comparison",
            symbol=request.symbol,
            page=request.page,
            limit=request.limit,
            descriptor=_F10_REPORTS[request.operation],
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def operating_indicators(
        self,
        request: OperatingIndicatorsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._f10(
            operation=request.operation,
            endpoint="em_supplementary_indicators",
            symbol=request.symbol,
            page=request.page,
            limit=request.limit,
            descriptor=_F10_REPORTS[request.operation],
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def market_reports(
        self,
        request: MarketReportsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        qtype = {"strategy": 2, "macro": 3, "broker": 4, "industry": 1}[
            request.category
        ]
        begin, end = _date_range(request.days or 0)
        params = {
            "cb": "",
            "pageSize": 100,
            "pageNo": 1,
            "fields": "",
            "qType": qtype,
            "orgCode": "",
            "code": "*",
            "beginTime": begin,
            "endTime": end,
        }
        payload = await self._json(
            operation=request.operation,
            endpoint=f"em_{request.category}_reports",
            url=build_url("https://reportapi.eastmoney.com", "/report/jg", params),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        root = _record(payload, "东方财富市场研报")
        rows = root.get("data")
        if not isinstance(rows, list):
            raise UpstreamUnavailable("东方财富市场研报响应缺少列表。")
        reports = [row for row in rows if isinstance(row, dict)]
        if not reports:
            raise NoStockData("市场研报没有可用记录。")
        art_codes = await self._market_report_art_codes(
            request, qtype, begin, end, timeout_seconds, cancellation_token
        )
        selected = (
            reports[request.index - 1 : request.index]
            if request.index
            else reports[: request.top]
        )
        contents, errors = await self._load_market_contents(
            request, selected, art_codes, timeout_seconds, cancellation_token
        )
        return {
            "begin_date": begin,
            "end_date": end,
            "reports": reports,
            "contents": contents,
            "errors": errors,
        }

    async def stock_reports(
        self,
        request: StockReportsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        if request.content == "full":
            assert request.report_id is not None
            detail = await self._report_detail(
                request.operation,
                request.report_id,
                timeout_seconds,
                cancellation_token,
            )
            codes = detail.get("stock_codes")
            matches = isinstance(codes, list) and symbol_code(request.symbol) in codes
            return {
                "stock_code": symbol_code(request.symbol),
                "art_code": request.report_id,
                **detail,
                "found": bool(detail.get("found")) and matches,
                "error": None if matches else "report_id 与证券代码不匹配。",
            }
        code = symbol_code(request.symbol)
        params = {
            "page_index": request.page,
            "page_size": request.limit,
            "client_source": "web",
            "stock_list": f"{_market_id(request.symbol)}.{code}",
            "type": "A",
        }
        payload = await self._json(
            operation=request.operation,
            endpoint="em_stock_reports",
            url=build_url(
                "https://np-areport-pc.eastmoney.com", "/api/security/rep", params
            ),
            parameters=params,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        root = _record(payload, "东方财富个股研报")
        data = _record(root.get("data"), "东方财富个股研报")
        values = data.get("list")
        if not isinstance(values, list):
            raise UpstreamUnavailable("东方财富个股研报响应缺少列表。")
        reports = [row for row in values if isinstance(row, dict)]
        errors: dict[str, str] = {}
        outcomes = await asyncio.gather(
            *(
                self._stock_report_summary(
                    request.operation,
                    row,
                    request.summary_max_characters,
                    timeout_seconds,
                    cancellation_token,
                )
                for row in reports
            ),
            return_exceptions=True,
        )
        for position, outcome in enumerate(outcomes):
            if isinstance(outcome, asyncio.CancelledError):
                raise outcome
            if isinstance(outcome, BaseException):
                errors[f"summary_{position + 1}"] = str(outcome)
                reports[position].update({"summary": "", "summary_available": False})
            else:
                reports[position].update(outcome)
        return {
            "stock_code": code,
            "page": request.page,
            "limit": request.limit,
            "total": data.get("total_hits", 0),
            "reports": reports,
            "errors": errors,
        }

    async def forecast(
        self,
        request: ForecastRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        if request.mode == "institutions":
            return await self._f10(
                operation=request.operation,
                endpoint="em_institutional_forecasts",
                symbol=request.symbol,
                page=request.page or 1,
                limit=request.limit or 20,
                descriptor=_F10_REPORTS["research.forecast.institutions"],
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
            )
        if request.mode == "summary":
            return await self._web_respredict(
                operation=request.operation,
                symbol=request.symbol,
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
            )

    async def ratings(
        self,
        request: RatingsRequest,
        *,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        return await self._web_respredict(
            operation=request.operation,
            symbol=request.symbol,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

    async def _web_respredict(
        self,
        *,
        operation: str,
        symbol: str,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        code = symbol_code(symbol)
        params = {
            "reportName": "RPT_WEB_RESPREDICT",
            "columns": "ALL",
            "pageNumber": 1,
            "pageSize": 1,
            "source": "WEB",
            "client": "WEB",
            "filter": f'(SECUCODE="{code}.{symbol_market(symbol)}")',
        }
        payload = await self._json(
            operation=operation,
            endpoint="em_web_respredict",
            url=build_url(
                "https://datacenter.eastmoney.com",
                "/securities/api/data/v1/get",
                params,
            ),
            parameters={"stock_code": code},
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        result = _datacenter_result(payload, "RPT_WEB_RESPREDICT")
        rows = result.get("data")
        return {
            "found": isinstance(rows, list) and bool(rows),
            "item": rows[0] if isinstance(rows, list) and rows else None,
        }

    async def _f10(
        self,
        *,
        operation: str,
        endpoint: str,
        symbol: str,
        page: int,
        limit: int,
        descriptor: tuple[str, str | None, int],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> object:
        report_name, sort_column, sort_type = descriptor
        code = symbol_code(symbol)
        params: dict[str, object] = {
            "reportName": report_name,
            "columns": "ALL",
            "filter": f'(SECUCODE="{code}.{symbol_market(symbol)}")',
            "pageNumber": page,
            "pageSize": limit,
            "source": "HSF10",
            "client": "PC",
        }
        if sort_column is not None:
            params.update({"sortColumns": sort_column, "sortTypes": sort_type})
        payload = await self._json(
            operation=operation,
            endpoint=endpoint,
            url=build_url(
                "https://datacenter.eastmoney.com",
                "/securities/api/data/v1/get",
                params,
            ),
            parameters={"stock_code": code, "page": page, "limit": limit},
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        result = _datacenter_result(payload, endpoint)
        return {
            "total": result.get("count", 0),
            "pages": result.get("pages", 0),
            "items": result.get("data", []),
        }

    async def _market_report_art_codes(
        self,
        request: MarketReportsRequest,
        qtype: int,
        begin: str,
        end: str,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> dict[str, str]:
        params = {
            "cb": "",
            "industryCode": "*",
            "pageSize": 100,
            "pageNo": 1,
            "fields": "",
            "qType": 1 if qtype == 1 else 2,
            "orgCode": "",
            "code": "*",
            "rcode": "",
            "beginTime": begin,
            "endTime": end,
        }
        try:
            payload = await self._json(
                operation=request.operation,
                endpoint="em_market_report_art_codes",
                url=build_url(
                    "https://reportapi.eastmoney.com", "/report/list", params
                ),
                parameters=params,
                timeout_seconds=timeout_seconds,
                cancellation_token=cancellation_token,
                headers=public_headers(referer=_EASTMONEY_REFERER),
                lane="eastmoney_f10",
            )
        except UpstreamUnavailable:
            return {}
        root = _record(payload, "东方财富研报目录")
        rows = root.get("data")
        if not isinstance(rows, list):
            return {}
        return {
            str(row.get("title")): str(row.get("infoCode"))
            for row in rows
            if isinstance(row, dict) and row.get("title") and row.get("infoCode")
        }

    async def _load_market_contents(
        self,
        request: MarketReportsRequest,
        reports: list[dict[str, object]],
        art_codes: dict[str, str],
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> tuple[list[dict[str, object]], dict[str, str]]:
        async def one(report: dict[str, object]) -> dict[str, object]:
            title = str(report.get("title") or "")
            art_code = art_codes.get(title, "")
            if not art_code:
                return {"title": title, "art_code": None, "content": ""}
            detail = await self._report_detail(
                request.operation,
                art_code,
                timeout_seconds,
                cancellation_token,
                content_max_characters=request.content_max_characters,
            )
            return {
                "title": title,
                "art_code": art_code,
                "content": detail.get("content", ""),
                "content_truncated": detail.get("content_truncated", False),
            }

        outcomes = await asyncio.gather(
            *(one(report) for report in reports), return_exceptions=True
        )
        contents: list[dict[str, object]] = []
        errors: dict[str, str] = {}
        for position, outcome in enumerate(outcomes):
            if isinstance(outcome, asyncio.CancelledError):
                raise outcome
            if isinstance(outcome, BaseException):
                errors[f"content_{position + 1}"] = str(outcome)
            else:
                contents.append(outcome)
        return contents, errors

    async def _stock_report_summary(
        self,
        operation: str,
        report: dict[str, object],
        summary_max_characters: int | None,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
    ) -> dict[str, object]:
        art_code = str(report.get("art_code") or report.get("artCode") or "")
        if not art_code:
            return {"summary": "", "summary_available": False}
        detail = await self._report_detail(
            operation,
            art_code,
            timeout_seconds,
            cancellation_token,
            content_max_characters=summary_max_characters,
        )
        content = str(detail.get("content") or "")
        summary, summary_truncated = _report_summary(content, summary_max_characters)
        return {
            "summary": summary,
            "summary_available": bool(summary),
            "summary_truncated": summary_truncated
            or bool(detail.get("content_truncated")),
            "attach_url": detail.get("attach_url", ""),
        }

    async def _report_detail(
        self,
        operation: str,
        art_code: str,
        timeout_seconds: float,
        cancellation_token: AbortSignal | None,
        content_max_characters: int | None = 4_000,
    ) -> dict[str, object]:
        params = {"art_code": art_code, "client_source": "web"}
        payload = await self._json(
            operation=operation,
            endpoint="em_report_content",
            url=build_url(
                "https://np-creport-pc.eastmoney.com", "/api/content/rep", params
            ),
            parameters={"art_code": art_code},
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
            headers=public_headers(referer=_EASTMONEY_REFERER),
            lane="eastmoney_f10",
        )
        root = _record(payload, "东方财富研报正文")
        data = root.get("data")
        if data is None:
            return {"found": False, "stock_codes": [], "content": "", "attach_url": ""}
        detail = _record(data, "东方财富研报正文")
        content = str(detail.get("notice_content") or "")
        truncated = (
            content_max_characters is not None and len(content) > content_max_characters
        )
        security = detail.get("security")
        security_rows = (
            security
            if isinstance(security, list)
            else [security]
            if isinstance(security, dict)
            else []
        )
        codes = [
            str(item.get("stock"))
            for item in security_rows
            if isinstance(item, dict) and item.get("stock")
        ]
        return {
            "found": True,
            "title": detail.get("title")
            or detail.get("title_ch")
            or detail.get("notice_title")
            or "",
            "stock_name": detail.get("short_name") or "",
            "stock_codes": codes,
            "content": (
                content
                if content_max_characters is None
                else content[:content_max_characters]
            ),
            "content_truncated": truncated,
            "attach_url": detail.get("attach_url") or "",
        }


def _market_id(symbol: str) -> int:
    return 1 if symbol_market(symbol) == "SH" else 0


def _record(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise UpstreamUnavailable(f"{label}响应无效。")
    return value


def _datacenter_result(value: object, label: str) -> dict[str, object]:
    root = _record(value, label)
    if root.get("success") is not True or root.get("code") not in {0, "0", None}:
        message = root.get("message") or root.get("msg") or "未知错误"
        raise UpstreamUnavailable(f"{label}业务请求失败：{message}。")
    result = root.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("data"), list):
        raise UpstreamUnavailable(f"{label}响应缺少数据列表。")
    return result


def _date_range(days: int) -> tuple[str, str]:
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    return ((today - timedelta(days=days)).isoformat(), today.isoformat())


def _report_summary(value: str, maximum: int | None = 1_000) -> tuple[str, bool]:
    text = " ".join(value.replace("\r", "\n").split())
    if maximum is None:
        return text, False
    return text[:maximum], len(text) > maximum


__all__ = ["EastMoneyF10Adapter"]
