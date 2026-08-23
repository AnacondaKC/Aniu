"""Fundamental, research, announcement, and news standardizers."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import cast

from backend.stock_api.public.contracts import (
    AnnouncementsRequest,
    FinancialsRequest,
    ForecastRequest,
    IndustryComparisonRequest,
    MarketReportsRequest,
    NewsFeedRequest,
    NewsSearchRequest,
    OperatingIndicatorsRequest,
    RatingsRequest,
    ShareholdersRequest,
    StockNewsRequest,
    StockReportsRequest,
    ValuationRequest,
)
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable
from backend.stock_api.public.normalizers.common import (
    JsonRecord,
    NormalizedData,
    as_record,
    canonical_symbol,
    clean_html,
    clean_text,
    find_rows,
    first_number,
    first_text,
    first_value,
    normalize_date,
    normalize_datetime,
    number,
    redact_absolute_urls,
    require_items,
    require_valid_items,
    text,
)


def normalize_financials(raw: object, request: FinancialsRequest) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _items(root)
    if not rows:
        raise NoStockData("财务数据没有可用记录。")
    items = [_financial_item(row, request.mode) for row in rows]
    if request.mode == "latest":
        item = items[0]
        _require_record_fields(
            item, "最新财务", ("report_period", "revenue", "net_profit")
        )
        return NormalizedData(
            {
                "symbol": request.symbol,
                "mode": "latest",
                "item": item,
            }
        )
    valid, dropped = require_valid_items(
        items,
        "季度财务",
        ("report_period", "revenue", "net_profit"),
    )
    return NormalizedData(
        {
            "symbol": request.symbol,
            "mode": "quarterly",
            "page": request.page,
            "limit": request.limit,
            "total": _integer_or_zero(root.get("total")),
            "items": valid,
        },
        degraded=dropped > 0,
        warnings=(f"季度财务有 {dropped} 条记录因关键字段无效被丢弃。",)
        if dropped
        else (),
    )


def normalize_shareholders(raw: object, request: ShareholdersRequest) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _items(root)
    if not rows:
        raise NoStockData("股东户数没有可用记录。")
    items = [_shareholder_item(row) for row in rows]
    valid, dropped = require_valid_items(
        items,
        "股东户数",
        ("as_of", "shareholder_count"),
    )
    return NormalizedData(
        _paged_symbol_result_data(
            root, request.symbol, request.page, request.limit, valid
        ),
        degraded=dropped > 0,
        warnings=(f"股东户数有 {dropped} 条记录因关键字段无效被丢弃。",)
        if dropped
        else (),
    )


def normalize_valuation(raw: object, request: ValuationRequest) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _items(root)
    if not rows:
        raise NoStockData("估值数据没有可用记录。")
    metric_names = {1: "pe", 2: "pb", 3: "ps", 4: "pcf"}
    window_years = {1: 1, 2: 3, 3: 5, 4: 10}
    items: list[dict[str, object]] = []
    warnings: list[str] = []
    seen: set[tuple[str, int]] = set()
    invalid_rows = 0
    null_rows = 0
    partial_combinations: list[str] = []
    for row in rows:
        raw_symbol = row.get("SECUCODE")
        if not isinstance(raw_symbol, str) or not raw_symbol.strip():
            invalid_rows += 1
            continue
        symbol = raw_symbol.strip().upper()
        if symbol != request.symbol:
            raise UpstreamUnavailable("估值数据包含了其他证券。")
        raw_metric = number(row.get("INDEX_TYPE"))
        raw_window = number(row.get("STATISTICS_CYCLE"))
        if raw_metric not in metric_names or raw_window not in window_years:
            invalid_rows += 1
            continue
        metric = metric_names[int(raw_metric)]
        years = window_years[int(raw_window)]
        key = (metric, years)
        if key in seen:
            raise UpstreamUnavailable("估值数据包含重复的指标周期。")
        seen.add(key)
        threshold_keys = (
            "PERCENTILE_THIRTY",
            "PERCENTILE_FIFTY",
            "PERCENTILE_SEVENTY",
        )
        if any(key not in row for key in threshold_keys):
            invalid_rows += 1
            continue
        threshold_values = tuple(first_number(row, key) for key in threshold_keys)
        if all(value is None for value in threshold_values):
            null_rows += 1
            continue
        if any(value is None for value in threshold_values):
            partial_combinations.append(f"{metric}/{years}年")
            invalid_rows += 1
            continue
        items.append(
            {
                "metric": metric,
                "window_years": years,
                "percentile_30_threshold": threshold_values[0],
                "percentile_50_threshold": threshold_values[1],
                "percentile_70_threshold": threshold_values[2],
            }
        )
    if not items:
        if invalid_rows and null_rows != len(rows):
            raise UpstreamUnavailable("估值数据中的关键字段无效。")
        raise NoStockData("估值数据没有适用的估值组合。")
    items.sort(
        key=lambda item: (
            ("pe", "pb", "ps", "pcf").index(str(item["metric"])),
            int(cast(int, item["window_years"])),
        )
    )
    missing = 16 - len(items)
    if invalid_rows or null_rows or missing:
        warnings.append(f"估值数据缺少 {missing} 个指标周期组合。")
        if partial_combinations:
            warnings.append(
                "估值指标周期阈值不完整：" + "、".join(partial_combinations) + "。"
            )
    return NormalizedData(
        {"symbol": request.symbol, "items": items},
        degraded=bool(warnings),
        warnings=tuple(warnings),
    )


def normalize_industry_comparison(
    raw: object, request: IndustryComparisonRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _items(root)
    if not rows:
        raise NoStockData("行业对比没有可用记录。")
    items: list[dict[str, object]] = []
    industry = ""
    dropped = 0
    for row in rows:
        industry = industry or first_text(row, "INDUSTRY")
        symbol = canonical_symbol(row.get("CORRE_SECUCODE"))
        name = first_text(row, "CORRE_SECURITY_NAME")
        item: dict[str, object] = {
            "symbol": symbol,
            "name": _safe_text(name) or None,
            "revenue": first_number(row, "TOTALOPERATEREVE"),
            "net_profit": first_number(row, "PARENTNETPROFIT"),
            "roe": first_number(row, "ROE"),
            "pe": None,
            "pb": first_number(row, "PB"),
        }
        if (
            symbol is None
            or not name
            or not any(
                item[field] is not None
                for field in ("revenue", "net_profit", "roe", "pb")
            )
        ):
            dropped += 1
            continue
        items.append(item)
    if not items:
        raise UpstreamUnavailable("行业对比响应中的关键字段无效。")
    return NormalizedData(
        {
            **_paged_symbol_result_data(
                root, request.symbol, request.page, request.limit, items
            ),
            "industry": _safe_text(industry) or None,
        },
        degraded=dropped > 0,
        warnings=(f"行业对比有 {dropped} 条记录因关键字段无效被丢弃。",)
        if dropped
        else (),
    )


def normalize_operating_indicators(
    raw: object, request: OperatingIndicatorsRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _items(root)
    if not rows:
        raise NoStockData("经营指标没有可用记录。")
    items: list[dict[str, object]] = []
    dropped = 0
    for row in rows:
        name = first_text(row, "INDICATOR_NAME")
        raw_value = row.get("VALUE")
        value: object = first_number(row, "VALUE")
        if value is None and text(raw_value) not in {"", "-", "--", "null"}:
            value = _safe_text(text(raw_value))
        if not name or value is None:
            dropped += 1
            continue
        items.append(
            {
                "name": _safe_text(name),
                "value": value,
                "unit": _safe_text(first_text(row, "UNIT")) or None,
                "updated_at": normalize_datetime(row.get("UPDATE_DATE")),
                "update_frequency": _safe_text(first_text(row, "UPDATE_FREQUENCY"))
                or None,
                "granularity": _safe_text(first_text(row, "INDICATOR_GRANULARITY"))
                or None,
                "source": _safe_text(first_text(row, "SOURCE")) or None,
            }
        )
    if not items:
        raise UpstreamUnavailable("经营指标响应中的关键字段无效。")
    return NormalizedData(
        _paged_symbol_result_data(
            root, request.symbol, request.page, request.limit, items
        ),
        degraded=dropped > 0,
        warnings=(f"经营指标有 {dropped} 条记录因关键字段无效被丢弃。",)
        if dropped
        else (),
    )


def normalize_market_reports(
    raw: object, request: MarketReportsRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    rows = root.get("reports")
    reports = (
        [_report_item(row) for row in rows if isinstance(row, dict)]
        if isinstance(rows, list)
        else []
    )
    require_items(reports, "市场研报")
    contents = root.get("contents")
    excerpts: list[dict[str, object]] = []
    if isinstance(contents, list):
        for value in contents:
            row = as_record(value)
            if row is None:
                continue
            excerpt, truncated = clean_text(
                row.get("content"),
                request.content_max_characters,
            )
            excerpts.append(
                {
                    "title": _safe_text(first_text(row, "title")) or None,
                    "report_id": _safe_text(first_text(row, "art_code", "report_id"))
                    or None,
                    "content": excerpt,
                    "content_truncated": truncated
                    or bool(row.get("content_truncated")),
                }
            )
    errors = _safe_errors(root.get("errors"))
    return NormalizedData(
        {
            "category": request.category,
            "begin_date": first_text(root, "begin_date") or None,
            "end_date": first_text(root, "end_date") or None,
            "reports": reports,
            "contents": excerpts,
            "content_errors": errors if isinstance(errors, dict) else {},
        },
        degraded=bool(errors),
        warnings=("部分研报正文暂时不可用。",) if errors else (),
    )


def normalize_stock_reports(
    raw: object, request: StockReportsRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    if request.content == "full":
        found_value = root.get("found")
        found = (
            found_value is True
            or (isinstance(found_value, str) and found_value.lower() == "true")
            or (
                isinstance(found_value, (int, float))
                and not isinstance(found_value, bool)
                and found_value == 1
            )
        )
        content, truncated = clean_text(root.get("content"), 4_000)
        if not found:
            raise NoStockData("未找到对应研报正文。")
        if not content:
            raise UpstreamUnavailable("研报正文响应为空。")
        return NormalizedData(
            {
                "symbol": request.symbol,
                "content": "full",
                "report": {
                    "report_id": _safe_text(first_text(root, "art_code", "report_id"))
                    or request.report_id,
                    "found": True,
                    "title": _safe_text(first_text(root, "title")) or None,
                    "stock_name": _safe_text(first_text(root, "stock_name")) or None,
                    "content": content,
                    "content_truncated": truncated
                    or bool(root.get("content_truncated")),
                },
            }
        )
    rows = root.get("reports")
    raw_reports = (
        [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
    )
    reports = [
        _stock_report_item(row, request.summary_max_characters) for row in raw_reports
    ]
    valid, dropped = require_valid_items(
        reports, "个股研报", ("report_id", "title", "published_at")
    )
    errors = _safe_errors(root.get("errors"))
    return NormalizedData(
        {
            "symbol": request.symbol,
            "content": "summary",
            "page": request.page,
            "limit": request.limit,
            "total": _integer_or_zero(root.get("total")),
            "reports": valid,
            "summary_errors": errors,
        },
        degraded=bool(errors) or dropped > 0,
        warnings=(
            (("部分研报摘要暂时不可用。",) if errors else ())
            + (
                (f"个股研报有 {dropped} 条记录因关键字段无效被丢弃。",)
                if dropped
                else ()
            )
        ),
    )


def normalize_forecast(raw: object, request: ForecastRequest) -> NormalizedData:
    root = as_record(raw) or {}
    if request.mode == "summary":
        item = as_record(root.get("item"))
        if item is None:
            raise NoStockData("盈利预测没有可用记录。")
        normalized = _forecast_summary(item)
        if normalized["institution_count"] is None or not normalized["estimates"]:
            raise UpstreamUnavailable("盈利预测汇总响应中的关键字段无效。")
        return NormalizedData(
            {"symbol": request.symbol, "mode": "summary", **normalized}
        )
    rows = _items(root)
    if not rows:
        raise NoStockData("机构盈利预测没有可用记录。")
    items = [_forecast_item(row, include_pe=True) for row in rows]
    valid: list[dict[str, object]] = []
    dropped = 0
    for item in items:
        estimates = item.get("estimates")
        if text(item.get("institution")) and isinstance(estimates, list) and estimates:
            valid.append(item)
        else:
            dropped += 1
    if not valid:
        raise UpstreamUnavailable("机构盈利预测响应中的关键字段无效。")
    return NormalizedData(
        {
            "symbol": request.symbol,
            "mode": "institutions",
            "page": request.page,
            "limit": request.limit,
            "total": _integer_or_zero(root.get("total")),
            "items": valid,
        },
        degraded=dropped > 0,
        warnings=(f"机构盈利预测有 {dropped} 条记录因关键字段无效被丢弃。",)
        if dropped
        else (),
    )


def normalize_ratings(raw: object, request: RatingsRequest) -> NormalizedData:
    root = as_record(raw) or {}
    item = as_record(root.get("item"))
    if item is None:
        rows = _items(root)
        item = rows[0] if rows else None
    if item is None:
        raise NoStockData("机构评级没有可用记录。")
    result: dict[str, object] = {
        "symbol": request.symbol,
        "institution_count": first_number(item, "RATING_ORG_NUM"),
        "buy": first_number(item, "RATING_BUY_NUM"),
        "increase": first_number(item, "RATING_ADD_NUM"),
        "neutral": first_number(item, "RATING_NEUTRAL_NUM"),
        "reduce": first_number(item, "RATING_REDUCE_NUM"),
        "sell": first_number(item, "RATING_SALE_NUM"),
        "long_count": first_number(item, "RATING_LONG_NUM"),
        "target_price_min": first_number(item, "DEC_AIMPRICEMIN"),
        "target_price_max": first_number(item, "DEC_AIMPRICEMAX"),
    }
    if result["institution_count"] is None:
        raise UpstreamUnavailable("机构评级响应中的机构数无效。")
    if all(
        result[field] is None
        for field in ("buy", "increase", "neutral", "reduce", "sell", "long_count")
    ):
        raise UpstreamUnavailable("机构评级响应中的评级字段无效。")
    return NormalizedData(result)


def normalize_announcements(
    raw: object, request: AnnouncementsRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    values = root.get("announcements")
    rows = (
        [row for row in values if isinstance(row, dict)]
        if isinstance(values, list)
        else []
    )
    items = _news_items(rows, content_kind="announcement", official=True)
    require_items(items, "公司公告")
    return NormalizedData(
        {
            "symbol": request.symbol,
            "page": request.page,
            "limit": request.limit,
            "total": _integer_or_zero(root.get("total")),
            "content_kind": "announcement",
            "official": True,
            "items": items,
        }
    )


def normalize_news_feed(
    provider: str, raw: object, request: NewsFeedRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    values = root.get("items")
    rows = (
        [row for row in values if isinstance(row, dict)]
        if isinstance(values, list)
        else find_rows(raw) or []
    )
    items = _news_items(rows, content_kind="news", official=False)[: request.limit or 0]
    require_items(items, "资讯流")
    return NormalizedData(
        {
            "feed": request.feed,
            "page": request.page,
            "limit": request.limit,
            "content_kind": "news",
            "official": False,
            "items": items,
        },
        degraded=provider == "sina"
        and request.feed in {"headlines", "flash", "finance"},
        warnings=(
            ("新浪滚动流与首选资讯源的选稿口径不同。",)
            if provider == "sina" and request.feed in {"headlines", "flash", "finance"}
            else ()
        ),
    )


def normalize_stock_news(
    provider: str, raw: object, request: StockNewsRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    if provider == "sina" and isinstance(root.get("body"), str):
        body, truncated = clean_html(root["body"], 12_000)
        if not body:
            raise NoStockData("个股新闻页面没有可用内容。")
        return NormalizedData(
            {
                "symbol": request.symbol,
                "page": request.page,
                "limit": request.limit,
                "content_kind": "news",
                "official": False,
                "items": [],
                "text": body,
                "text_truncated": truncated,
            },
            degraded=True,
            warnings=("新浪备用源仅提供经清洗的个股新闻页面文本。",),
        )
    rows = find_rows(raw) or []
    items = _news_items(rows, content_kind="news", official=False)[: request.limit]
    require_items(items, "个股新闻")
    return NormalizedData(
        {
            "symbol": request.symbol,
            "page": request.page,
            "limit": request.limit,
            "content_kind": "news",
            "official": False,
            "items": items,
        }
    )


def normalize_news_search(raw: object, request: NewsSearchRequest) -> NormalizedData:
    root = as_record(raw) or {}
    rows = _search_rows(root)
    items = _news_items(rows, content_kind="news", official=False)[: request.limit]
    require_items(items, "新闻搜索")
    return NormalizedData(
        {
            "keyword": request.keyword,
            "page": request.page,
            "limit": request.limit,
            "content_kind": "news",
            "official": False,
            "items": items,
        }
    )


def _financial_item(row: JsonRecord, mode: str) -> dict[str, object]:
    if mode == "latest":
        revenue_key = "TOTAL_OPERATEINCOME"
        roe_key = "ROEJQ"
        gross_margin_key = "XSMLL"
        debt_key = "ZCFZL"
        revenue_yoy_key = "TOTALOPERATEREVETZ"
        profit_yoy_key = "PARENTNETPROFITTZ"
        revenue_qoq_key = "YYZSRGDHBZC"
        profit_qoq_key = "NETPROFITRPHBZC"
    else:
        revenue_key = "TOTALOPERATEREVE"
        roe_key = "ROE_DILUTED"
        gross_margin_key = "GROSS_PROFIT_RATIO"
        debt_key = "ZCFZL"
        revenue_yoy_key = "TOTALOPERATEREVETZ"
        profit_yoy_key = "PARENTNETPROFITTZ"
        revenue_qoq_key = "YYZSRGDHBZC"
        profit_qoq_key = "NETPROFITRPHBZC"
    return {
        "report_period": normalize_date(row.get("REPORT_DATE")),
        "eps": first_number(row, "EPSJB"),
        "roe": first_number(row, roe_key),
        "revenue": first_number(row, revenue_key),
        "net_profit": first_number(
            row, "PARENT_NETPROFIT" if mode == "latest" else "PARENTNETPROFIT"
        ),
        "gross_margin": first_number(row, gross_margin_key),
        "debt_asset_ratio": first_number(row, debt_key),
        "revenue_yoy": first_number(row, revenue_yoy_key),
        "net_profit_yoy": first_number(row, profit_yoy_key),
        "revenue_rolling_qoq": first_number(row, revenue_qoq_key),
        "net_profit_rolling_qoq": first_number(row, profit_qoq_key),
    }


def _shareholder_item(row: JsonRecord) -> dict[str, object]:
    return {
        "as_of": normalize_date(row.get("END_DATE")),
        "shareholder_count": first_number(row, "HOLDER_TOTAL_NUM"),
        "average_holding_shares": first_number(row, "AVG_FREE_SHARES"),
        "change": first_number(row, "HOLDER_TOTAL_NUMCHANGE"),
        "change_percent": first_number(row, "TOTAL_NUM_RATIO"),
    }


def _require_record_fields(
    item: Mapping[str, object], label: str, fields: tuple[str, ...]
) -> None:
    if any(item.get(field) is None or item.get(field) == "" for field in fields):
        raise UpstreamUnavailable(f"{label}响应中的关键字段无效。")


def _safe_text(value: object) -> str:
    return redact_absolute_urls(text(value))


def _safe_errors(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {
        str(_safe_text(key)): _safe_text(item)[:500]
        for key, item in value.items()
        if _safe_text(item)
    }


def _report_item(row: JsonRecord) -> dict[str, object]:
    return {
        "report_id": _safe_text(
            first_text(row, "art_code", "artCode", "infoCode", "INFO_CODE")
        )
        or None,
        "title": _safe_text(first_text(row, "title", "TITLE")) or None,
        "institution": _safe_text(
            first_text(row, "org_name", "org", "orgName", "source")
        )
        or None,
        "author": _report_author(row),
        "published_at": normalize_datetime(
            first_value(row, "publish_date", "publishDate", "eiTime")
        ),
    }


def _stock_report_item(
    row: JsonRecord, summary_max_characters: int | None
) -> dict[str, object]:
    summary, truncated = clean_text(row.get("summary"), summary_max_characters)
    return {
        **_report_item(row),
        "rating": _safe_text(
            first_text(row, "em_rating_name", "s_rating_name", "rating")
        )
        or None,
        "target_price": first_number(row, "aim_price", "target_price"),
        "industry": _safe_text(first_text(row, "industry")) or None,
        "rating_change": _rating_change(row.get("rating_change")),
        "summary": summary or None,
        "summary_available": bool(row.get("summary_available", bool(summary))),
        "summary_truncated": truncated or bool(row.get("summary_truncated")),
    }


def _report_author(row: JsonRecord) -> str | None:
    values = row.get("author_items")
    if isinstance(values, list):
        names = [
            _safe_text(first_text(item, "author_name"))
            for item in values
            if isinstance(item, dict)
        ]
        names = [name for name in names if name]
        if names:
            return "、".join(names)
    return None


def _rating_change(value: object) -> str | None:
    if number(value) is not None:
        return None
    return text(value) or None


def _forecast_summary(row: JsonRecord) -> dict[str, object]:
    return {
        "institution_count": first_number(row, "RATING_ORG_NUM"),
        "target_price_min": first_number(row, "DEC_AIMPRICEMIN"),
        "target_price_max": first_number(row, "DEC_AIMPRICEMAX"),
        "estimates": _forecast_estimates(row, include_pe=False),
    }


def _forecast_item(row: JsonRecord, *, include_pe: bool) -> dict[str, object]:
    return {
        "institution": _safe_text(first_text(row, "ORG_NAME_ABBR")) or None,
        "published_at": normalize_datetime(row.get("PUBLISH_DATE")),
        "estimates": _forecast_estimates(row, include_pe=include_pe),
    }


def _forecast_estimates(
    row: JsonRecord, *, include_pe: bool
) -> list[dict[str, object]]:
    estimates: list[dict[str, object]] = []
    for index in range(1, 5):
        year = first_number(row, f"YEAR{index}")
        eps = first_number(row, f"EPS{index}")
        if year is None or eps is None:
            continue
        mark = first_text(row, f"YEAR_MARK{index}")
        estimate: dict[str, object] = {
            "year": year,
            "kind": {"A": "actual", "E": "estimate"}.get(mark, "unknown"),
            "eps": eps,
        }
        if include_pe:
            pe = first_number(row, f"PE{index}")
            if pe is not None:
                estimate["pe"] = pe
        estimates.append(estimate)
    return estimates


def _news_items(
    rows: Iterable[JsonRecord], *, content_kind: str, official: bool
) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for row in rows:
        item = _news_item(row, content_kind=content_kind, official=official)
        if item is not None:
            items.append(item)
    return items


def _news_item(
    row: JsonRecord, *, content_kind: str, official: bool
) -> dict[str, object] | None:
    title = _safe_text(
        first_text(row, "title", "TITLE", "news_title", "notice_title", "name")
    )
    if not title:
        return None
    summary, truncated = clean_html(
        first_value(
            row,
            "summary",
            "digest",
            "content",
            "description",
            "notice_content",
            "intro",
            "wapsummary",
        )
        or "",
        1_000,
    )
    return {
        "title": title,
        "published_at": normalize_datetime(
            first_value(
                row,
                "publish_time",
                "publishDate",
                "notice_date",
                "eiTime",
                "showtime",
                "date",
                "time",
                "ctime",
                "intime",
            )
        ),
        "summary": summary or None,
        "summary_truncated": truncated,
        "source": _safe_text(
            first_text(row, "source", "media", "source_name", "org", "media_name")
        )
        or None,
        "announcement_type": _safe_text(
            first_text(row, "columns", "notice_type", "type")
        )
        or None,
        "content_kind": content_kind,
        "official": official,
    }


def _search_rows(root: JsonRecord) -> list[JsonRecord]:
    result = (
        as_record(root.get("result"))
        or as_record(as_record(root.get("data")) or {})
        or {}
    )
    values = result.get("cmsArticleWebOld")
    if isinstance(values, list):
        return [item for item in values if isinstance(item, dict)]
    nested_result = as_record(as_record(root.get("data")) or {})
    if nested_result:
        nested = as_record(nested_result.get("result")) or {}
        rows = nested.get("cmsArticleWebOld")
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
    return find_rows(root) or []


def _items(root: JsonRecord) -> list[JsonRecord]:
    values = root.get("items")
    if isinstance(values, list):
        return [item for item in values if isinstance(item, dict)]
    return find_rows(root) or []


def _paged_symbol_result_data(
    root: JsonRecord,
    symbol: str,
    page: int,
    limit: int,
    items: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "page": page,
        "limit": limit,
        "total": _integer_or_zero(root.get("total")),
        "items": list(items),
    }


def _paged_symbol_result(
    root: JsonRecord,
    symbol: str,
    page: int,
    limit: int,
    items: Sequence[Mapping[str, object]],
) -> NormalizedData:
    return NormalizedData(
        {
            "symbol": symbol,
            "page": page,
            "limit": limit,
            "total": _integer_or_zero(root.get("total")),
            "items": items,
        }
    )


def _integer_or_zero(value: object) -> int:
    return value if type(value) is int and value >= 0 else 0


__all__ = [
    "normalize_announcements",
    "normalize_financials",
    "normalize_forecast",
    "normalize_industry_comparison",
    "normalize_market_reports",
    "normalize_news_feed",
    "normalize_news_search",
    "normalize_operating_indicators",
    "normalize_ratings",
    "normalize_shareholders",
    "normalize_stock_news",
    "normalize_stock_reports",
    "normalize_valuation",
]
