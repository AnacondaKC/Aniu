"""Quote, chart, ranking, and money-flow standardizers."""

from __future__ import annotations

from typing import Literal, cast

from backend.stock_api.public.contracts import (
    ConnectMoneyFlowRequest,
    IndexKlineRequest,
    IndexQuoteRequest,
    IntradayRequest,
    KlineRequest,
    QuoteSnapshotRequest,
    SectorMoneyFlowRequest,
    SectorRankingRequest,
    StockMoneyFlowHistoryRequest,
    StockMoneyFlowIntradayRequest,
    StockRankingRequest,
)
from backend.stock_api.public.errors import NoStockData, UpstreamUnavailable
from backend.stock_api.public.normalizers.common import (
    JsonRecord,
    NormalizedData,
    as_record,
    canonical_symbol,
    find_rows,
    first_number,
    first_text,
    normalize_datetime,
    number,
    require_items,
    sampled,
    text,
)

Provider = Literal["eastmoney", "tencent", "sina"]


def normalize_breadth(provider: Literal["eastmoney"], raw: object) -> NormalizedData:
    if provider != "eastmoney":
        raise UpstreamUnavailable("市场宽度数据源不受支持。", retryable=False)
    rows = _quote_rows(provider, raw)
    expected_codes = {"000001", "399001"}
    seen: set[str] = set()
    rising = 0
    falling = 0
    flat = 0
    for row in rows:
        code = str(row.get("f12") or "").strip()
        if code not in expected_codes or code in seen:
            continue
        values = tuple(row.get(field) for field in ("f104", "f105", "f106"))
        if not all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value >= 0
            and float(value).is_integer()
            for value in values
        ):
            continue
        rising_value, falling_value, flat_value = values
        assert isinstance(rising_value, (int, float))
        assert isinstance(falling_value, (int, float))
        assert isinstance(flat_value, (int, float))
        seen.add(code)
        rising += int(rising_value)
        falling += int(falling_value)
        flat += int(flat_value)
    if seen != expected_codes:
        raise UpstreamUnavailable(
            "东方财富涨跌家数响应缺少沪深市场汇总。", retryable=True
        )
    return NormalizedData(
        {"rising": rising, "falling": falling, "flat": flat},
        degraded=False,
    )


def normalize_quotes(
    provider: Provider,
    raw: object,
    request: QuoteSnapshotRequest | IndexQuoteRequest,
) -> NormalizedData:
    rows = _quote_rows(provider, raw)
    if not rows:
        raise NoStockData("行情没有返回有效记录。")
    quotes: list[dict[str, object]] = []
    returned: set[str] = set()
    invalid_matches = 0
    missing_fields: set[str] = set()
    for row in rows:
        symbol = _row_symbol(provider, row)
        if symbol not in request.symbols or symbol in returned:
            continue
        price = first_number(row, "f43", "f2", "price", "trade")
        previous_close = first_number(row, "f18", "f60", "previous_close", "settlement")
        if price is None:
            invalid_matches += 1
            continue
        change = first_number(row, "f169", "f4", "change", "pricechange", "zd")
        change_percent = first_number(
            row, "f170", "f3", "change_percent", "changepercent", "zdf", "percent"
        )
        if change is None and previous_close is not None:
            change = price - previous_close
        if (
            change_percent is None
            and previous_close is not None
            and previous_close != 0
        ):
            change_percent = (price - previous_close) / previous_close * 100
        item: dict[str, object] = {
            "symbol": symbol,
            "name": first_text(row, "f58", "f14", "name"),
            "price": price,
            "previous_close": previous_close,
            "change": change,
            "change_percent": change_percent,
            "market_time": _quote_time(provider, row),
        }
        if item["market_time"] is None:
            missing_fields.add("market_time")
        if request.detail == "full":
            raw_volume = first_number(
                row, "f47", "f5", "volume_lots", "volume", "volume_shares"
            )
            amount = first_number(row, "f48", "f6", "amount_cny", "amount")
            if provider == "tencent" and amount is None:
                amount_value = first_number(row, "amount_ten_thousand_cny")
                amount = None if amount_value is None else amount_value * 10_000
            full_values = {
                "open": first_number(row, "f17", "f46", "open"),
                "high": first_number(row, "f15", "f44", "high"),
                "low": first_number(row, "f16", "f45", "low"),
                "previous_close": previous_close,
                "volume_shares": _shares(provider, raw_volume),
                "amount": amount,
                "turnover_rate": first_number(
                    row, "f8", "f168", "turnover_rate", "turnoverratio"
                ),
            }
            item.update(
                {
                    "open": full_values["open"],
                    "high": full_values["high"],
                    "low": full_values["low"],
                    "volume_shares": full_values["volume_shares"],
                    "amount": full_values["amount"],
                    "amount_currency": "CNY",
                    "turnover_rate": full_values["turnover_rate"],
                }
            )
            missing_fields.update(
                field for field, value in full_values.items() if value is None
            )
        quotes.append(item)
        returned.add(symbol)
    if not quotes:
        if invalid_matches:
            raise UpstreamUnavailable("行情响应中的价格字段无效。")
        raise NoStockData("行情没有返回请求证券。")
    unavailable = [symbol for symbol in request.symbols if symbol not in returned]
    warnings: list[str] = []
    if unavailable:
        warnings.append(f"未返回有效数据：{','.join(unavailable)}。")
    if request.detail == "full" and missing_fields:
        warnings.append(f"行情缺少字段：{'、'.join(sorted(missing_fields))}。")
    return NormalizedData(
        {
            "detail": request.detail,
            "quotes": quotes,
            "unavailable_symbols": unavailable,
        },
        degraded=bool(unavailable or missing_fields),
        warnings=tuple(warnings),
    )


def normalize_index_quotes(
    provider: Provider,
    raw: object,
    request: IndexQuoteRequest,
) -> NormalizedData:
    normalized = normalize_quotes(provider, raw, request)
    if provider == "sina":
        quotes = normalized.data.get("quotes")
        if isinstance(quotes, list):
            for quote in quotes:
                if not isinstance(quote, dict):
                    continue
                volume = quote.get("volume_shares")
                if isinstance(volume, (int, float)):
                    quote["volume_shares"] = volume * 100
    return normalized


def normalize_kline(
    provider: Provider, raw: object, request: KlineRequest | IndexKlineRequest
) -> NormalizedData:
    eastmoney_trends = _is_eastmoney_trends(provider, raw)
    rows = _kline_rows(provider, raw, request)
    bars_by_time: dict[str, dict[str, object]] = {}
    for row in rows:
        parsed = _kline_bar(provider, row, volume_in_shares=eastmoney_trends)
        if parsed is not None:
            bars_by_time[str(parsed["time"])] = parsed
    bars = list(bars_by_time.values())[-request.limit :]
    require_items(bars, "K 线")
    return NormalizedData(
        {
            "symbol": request.symbol,
            "period": request.period,
            "adjust": request.adjust,
            "bars": bars,
        },
        degraded=provider in {"tencent", "sina"},
        warnings=(
            ("腾讯 K 线成交额不稳定，amount 已置为 null。",)
            if provider == "tencent"
            else ("新浪 K 线不支持复权和日期范围，统计口径可能不同。",)
            if provider == "sina"
            else ()
        ),
    )


def normalize_intraday(
    provider: Literal["eastmoney", "tencent"], raw: object, request: IntradayRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    previous_close: object | None = number(root.get("previous_close"))
    rows: list[object] = []
    if provider == "eastmoney":
        data = as_record(root.get("data")) or {}
        trends = data.get("trends")
        rows = trends if isinstance(trends, list) else []
        previous_close = number(data.get("preClose"))
    else:
        data = as_record(root.get("data")) or {}
        security = next((item for item in data.values() if isinstance(item, dict)), {})
        nested = as_record(security) or {}
        nested_data = as_record(nested.get("data")) or {}
        trend_rows = nested_data.get("data")
        rows = trend_rows if isinstance(trend_rows, list) else []
    points: list[dict[str, object]] = []
    for row in rows:
        fields = _split_line(row, r"\s+" if provider == "tencent" else ",")
        if len(fields) < 2:
            continue
        index = 2 if provider == "eastmoney" else 1
        price = number(fields[index]) if len(fields) > index else None
        if not text(fields[0]) or price is None:
            continue
        volume_index = 5 if provider == "eastmoney" else 2
        amount_index = 6 if provider == "eastmoney" else 3
        volume = _shares(
            provider,
            number(fields[volume_index]) if len(fields) > volume_index else None,
        )
        amount = number(fields[amount_index]) if len(fields) > amount_index else None
        average = (
            amount / volume
            if (
                provider == "tencent"
                and amount is not None
                and volume is not None
                and volume != 0
            )
            else number(fields[7])
            if provider == "eastmoney" and len(fields) > 7
            else None
        )
        points.append(
            {
                "time": text(fields[0]),
                "price": price,
                "average_price": average,
                "cumulative_volume_shares": volume,
                "cumulative_amount": amount,
            }
        )
    require_items(points, "分时走势")
    values, was_sampled = sampled(points, request.limit)
    warnings: list[str] = []
    if previous_close is None:
        warnings.append("分时走势缺少昨收价。")
    if provider == "tencent" and any(item["average_price"] is None for item in points):
        warnings.append("腾讯分时无法计算部分成交均价。")
    return NormalizedData(
        {
            "symbol": request.symbol,
            "days": request.days,
            "previous_close": previous_close,
            "sampled": was_sampled,
            "points": values,
        },
        degraded=bool(warnings),
        warnings=tuple(warnings),
    )


def normalize_ranking(
    provider: Provider,
    raw: object,
    request: StockRankingRequest | SectorRankingRequest,
    *,
    sectors: bool,
    paginate_locally: bool = False,
) -> NormalizedData:
    rows = _ranking_rows(provider, raw)
    items = [item for row in rows if (item := _ranking_item(provider, row, sectors))]
    output_sort = "volume_shares" if request.sort == "volume" else request.sort
    sort_values = [item.get(output_sort) for item in items]
    missing_sort_values = any(
        not isinstance(value, (int, float)) for value in sort_values
    )
    if items and not any(isinstance(value, (int, float)) for value in sort_values):
        raise UpstreamUnavailable("排行响应中的排序字段全部无效。")
    if paginate_locally:
        valid = [
            item for item in items if isinstance(item.get(output_sort), (int, float))
        ]
        missing = [
            item
            for item in items
            if not isinstance(item.get(output_sort), (int, float))
        ]
        valid.sort(
            key=lambda item: float(cast(float | int, item[output_sort])),
            reverse=request.order == "desc",
        )
        items = valid + missing
        start = (request.page - 1) * request.limit
        items = items[start : start + request.limit]
    else:
        items = items[: request.limit]
    require_items(items, "板块排行" if sectors else "个股排行")
    warnings: list[str] = []
    if provider != "eastmoney":
        warnings.append("该排行源的市场范围和统计口径可能与东方财富不同。")
    if missing_sort_values:
        warnings.append("部分排行记录缺少请求排序字段。")
    return NormalizedData(
        {"page": request.page, "limit": request.limit, "items": items},
        degraded=provider != "eastmoney" or missing_sort_values,
        warnings=tuple(warnings),
    )


def normalize_stock_money_flow(
    provider: Literal["eastmoney", "sina"],
    raw: object,
    request: StockMoneyFlowHistoryRequest | StockMoneyFlowIntradayRequest,
) -> NormalizedData:
    root = as_record(raw) or {}
    data = as_record(root.get("data"))
    kline_rows: list[object] | None = None
    if provider == "eastmoney":
        if data is None:
            raise UpstreamUnavailable("资金流响应缺少 data。")
        candidate_rows = data.get("klines")
        if not isinstance(candidate_rows, list):
            raise UpstreamUnavailable("资金流响应缺少 klines。")
        kline_rows = candidate_rows
    elif data is not None:
        candidate_rows = data.get("klines")
        if isinstance(candidate_rows, list):
            kline_rows = candidate_rows
    items: list[dict[str, object]]
    was_sampled = False
    if kline_rows is not None:
        items = [item for line in kline_rows if (item := _money_line(line))]
        if isinstance(request, StockMoneyFlowHistoryRequest):
            start = (request.page - 1) * request.limit
            items = items[start : start + request.limit]
        else:
            values, was_sampled = sampled(items, request.limit)
            items = values
    else:
        rows = find_rows(raw) or []
        sina_items = [_sina_money_item(row) for row in rows]
        items = [item for item in sina_items if item is not None]
        if isinstance(request, StockMoneyFlowHistoryRequest):
            items = items[: request.limit]
    require_items(items, "资金流")
    data_out: dict[str, object] = {
        "symbol": request.symbol,
        "items": items,
    }
    if isinstance(request, StockMoneyFlowHistoryRequest):
        data_out.update({"page": request.page, "limit": request.limit})
    else:
        data_out.update({"sampled": was_sampled})
    return NormalizedData(
        data_out,
        degraded=provider == "sina",
        warnings=(
            ("新浪资金流分类口径可能与东方财富不同。",) if provider == "sina" else ()
        ),
    )


def normalize_sector_money_flow(
    raw: object, request: SectorMoneyFlowRequest
) -> NormalizedData:
    ranking = normalize_ranking(
        "eastmoney",
        raw,
        SectorRankingRequest(
            sector_type=request.sector_type,
            sort="net_inflow",
            order="desc",
            page=request.page,
            limit=request.limit,
        ),
        sectors=True,
    )
    return NormalizedData(
        {
            "sector_type": request.sector_type,
            "page": request.page,
            "limit": request.limit,
            "items": ranking.data["items"],
        }
    )


def normalize_connect_flow(
    raw: object, request: ConnectMoneyFlowRequest
) -> NormalizedData:
    root = as_record(raw) or {}
    data = as_record(root.get("data"))
    if data is None:
        raise UpstreamUnavailable("沪深港通资金流响应缺少 data。")
    northbound = _connect_rows(data.get("s2n"), data.get("s2nDate"))
    southbound = _connect_rows(data.get("n2s"), data.get("n2sDate"))
    expected = {
        "northbound": request.direction in {"all", "northbound"},
        "southbound": request.direction in {"all", "southbound"},
    }
    available = {
        "northbound": bool(northbound),
        "southbound": bool(southbound),
    }
    if not any(expected[key] and available[key] for key in expected):
        raise NoStockData("请求方向没有可用的沪深港通资金流。")
    unavailable = [
        key for key, wanted in expected.items() if wanted and not available[key]
    ]
    payload: dict[str, object] = {
        "direction": request.direction,
        "amount_currency": "CNY",
        "unavailable_directions": unavailable,
    }
    if expected["northbound"]:
        payload["northbound"] = (
            northbound[-request.limit :] if available["northbound"] else []
        )
    if expected["southbound"]:
        payload["southbound"] = (
            southbound[-request.limit :] if available["southbound"] else []
        )
    return NormalizedData(
        payload,
        degraded=bool(unavailable),
        warnings=(
            (f"沪深港通分钟净流入暂缺方向：{'、'.join(unavailable)}。",)
            if unavailable
            else ()
        ),
    )


def _quote_rows(provider: Provider, raw: object) -> list[JsonRecord]:
    root = as_record(raw) or {}
    if provider == "eastmoney":
        data = as_record(root.get("data")) or {}
        diff = data.get("diff")
        if isinstance(diff, list):
            return [item for item in diff if isinstance(item, dict)]
        return [data] if data else []
    rows = root.get("quotes")
    return (
        [item for item in rows if isinstance(item, dict)]
        if isinstance(rows, list)
        else []
    )


def _row_symbol(provider: Provider, row: JsonRecord) -> str | None:
    if provider == "eastmoney":
        return canonical_symbol(
            row.get("f57") or row.get("f12") or row.get("code"),
            market=row.get("f107") or row.get("f13"),
        )
    return canonical_symbol(row.get("symbol") or row.get("code"))


def _quote_time(provider: Provider, row: JsonRecord) -> str | None:
    if provider == "eastmoney":
        timestamp = number(row.get("f86"))
        if timestamp is not None:
            return normalize_datetime(timestamp)
        return normalize_datetime(row.get("f124"))
    if provider == "tencent":
        return normalize_datetime(row.get("market_time"))
    values = " ".join(filter(None, (first_text(row, "date"), first_text(row, "time"))))
    return normalize_datetime(values)


def _kline_rows(
    provider: Provider, raw: object, request: KlineRequest | IndexKlineRequest
) -> list[object]:
    root = as_record(raw) or {}
    if provider == "eastmoney":
        if "data" not in root:
            raise UpstreamUnavailable("东方财富 K 线响应缺少 data。")
        data = as_record(root.get("data"))
        if data is None:
            raise UpstreamUnavailable("东方财富 K 线响应中的 data 无效。")
        trends = data.get("trends")
        if isinstance(trends, list):
            return trends
        if "klines" not in data:
            raise UpstreamUnavailable("东方财富 K 线响应缺少 klines。")
        values = data["klines"]
        if not isinstance(values, list):
            raise UpstreamUnavailable("东方财富 K 线响应中的 klines 无效。")
        return values
    if provider == "sina":
        return raw if isinstance(raw, list) else []
    data = as_record(root.get("data")) or {}
    security = next((item for item in data.values() if isinstance(item, dict)), {})
    record = as_record(security) or {}
    preferred: tuple[str, ...]
    if request.period.endswith("m"):
        preferred = (f"m{request.period[:-1]}", request.period)
    else:
        prefix = "" if request.adjust == "none" else request.adjust
        preferred = (f"{prefix}{request.period}", request.period)
    for key in preferred:
        rows = record.get(key)
        if isinstance(rows, list):
            return rows
    for value in record.values():
        if isinstance(value, list):
            return value
    return []


def _is_eastmoney_trends(provider: Provider, raw: object) -> bool:
    if provider != "eastmoney":
        return False
    root = as_record(raw) or {}
    data = as_record(root.get("data")) or {}
    return isinstance(data.get("trends"), list)


def _kline_bar(
    provider: Provider, value: object, *, volume_in_shares: bool = False
) -> dict[str, object] | None:
    if provider == "sina":
        row = as_record(value)
        if row is None:
            return None
        values = (
            first_text(row, "day", "date"),
            first_number(row, "open"),
            first_number(row, "high"),
            first_number(row, "low"),
            first_number(row, "close"),
            first_number(row, "volume"),
            first_number(row, "amount"),
        )
    else:
        fields = _split_line(value, ",")
        if len(fields) < 6:
            return None
        values = (
            text(fields[0]),
            number(fields[1]),
            number(fields[3]),
            number(fields[4]),
            number(fields[2]),
            number(fields[5]),
            number(fields[6]) if len(fields) > 6 else None,
        )
    time, opening, high, low, close, volume, amount = values
    if not time or any(value is None for value in (opening, high, low, close)):
        return None
    return {
        "time": time,
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume_shares": volume if volume_in_shares else _shares(provider, volume),
        "amount": None if provider == "tencent" else amount,
    }


def _ranking_rows(provider: Provider, raw: object) -> list[JsonRecord]:
    root = as_record(raw) or {}
    if provider == "eastmoney":
        data = as_record(root.get("data")) or {}
        rows = data.get("diff")
        return (
            [item for item in rows if isinstance(item, dict)]
            if isinstance(rows, list)
            else []
        )
    if provider == "sina":
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        return _sina_industry_rows(raw)
    return find_rows(raw) or []


def _sina_industry_rows(raw: object) -> list[JsonRecord]:
    root = as_record(raw)
    if root is None:
        return []
    rows: list[JsonRecord] = []
    for value in root.values():
        if not isinstance(value, str):
            continue
        fields = [field.strip() for field in value.split(",")]
        if len(fields) < 8 or not fields[0] or not fields[1]:
            continue
        rows.append(
            {
                "id": fields[0],
                "name": fields[1],
                "price": fields[3],
                "change": fields[4],
                "change_percent": fields[5],
                "volume": fields[6],
                "amount": fields[7],
                "leader_symbol": fields[8] if len(fields) > 8 else "",
                "leader_change_percent": fields[9] if len(fields) > 9 else "",
                "leader_price": fields[10] if len(fields) > 10 else "",
                "leader_change": fields[11] if len(fields) > 11 else "",
                "leader_name": fields[12] if len(fields) > 12 else "",
            }
        )
    return rows


def _ranking_item(
    provider: Provider, row: JsonRecord, sectors: bool
) -> dict[str, object] | None:
    code = first_text(row, "f12", "code", "symbol", "sector_code", "node", "id")
    name = first_text(row, "f14", "name", "sector_name", "cname", "label")
    if not code and not name:
        return None
    if sectors:
        item: dict[str, object] = {"id": code or None, "name": name}
    else:
        symbol = canonical_symbol(code, market=row.get("f13") or row.get("market"))
        if symbol is None:
            return None
        item = {"symbol": symbol, "code": symbol[:6], "name": name}
    if provider == "tencent":
        price = first_number(row, "zxj")
        change = first_number(row, "zd")
        change_percent = first_number(row, "zdf")
        volume = _shares(provider, first_number(row, "volume"))
        amount = first_number(row, "turnover")
        if amount is not None:
            amount *= 10_000
        turnover_rate = first_number(row, "hsl")
        net_inflow = None
    else:
        price = first_number(row, "f2", "price", "trade", "now")
        change = first_number(row, "f4", "change", "pricechange", "zd")
        change_percent = first_number(
            row, "f3", "change_percent", "changepercent", "zdf", "percent"
        )
        volume = _shares(provider, first_number(row, "f5", "volume"))
        amount = first_number(row, "f6", "amount")
        turnover_rate = first_number(row, "f8", "turnover_rate", "turnoverratio", "hsl")
        net_inflow = first_number(row, "f62", "net_inflow", "netamount")
    item.update(
        {
            "price": price,
            "change": change,
            "change_percent": change_percent,
            "volume_shares": volume,
            "amount": amount,
            "turnover_rate": turnover_rate,
            "net_inflow": net_inflow,
        }
    )
    return item


def _money_line(value: object) -> dict[str, object] | None:
    fields = _split_line(value, ",")
    if not fields or not text(fields[0]):
        return None
    return {
        "time": text(fields[0]),
        "main_net_inflow": number(fields[1]) if len(fields) > 1 else None,
        "small_net_inflow": number(fields[2]) if len(fields) > 2 else None,
        "medium_net_inflow": number(fields[3]) if len(fields) > 3 else None,
        "large_net_inflow": number(fields[4]) if len(fields) > 4 else None,
        "super_large_net_inflow": number(fields[5]) if len(fields) > 5 else None,
        "main_net_ratio": number(fields[6]) if len(fields) > 6 else None,
        "small_net_ratio": number(fields[7]) if len(fields) > 7 else None,
        "medium_net_ratio": number(fields[8]) if len(fields) > 8 else None,
        "large_net_ratio": number(fields[9]) if len(fields) > 9 else None,
        "super_large_net_ratio": number(fields[10]) if len(fields) > 10 else None,
        "close": number(fields[11]) if len(fields) > 11 else None,
        "change_percent": number(fields[12]) if len(fields) > 12 else None,
    }


def _sina_money_item(row: JsonRecord) -> dict[str, object] | None:
    time = first_text(row, "date", "day", "opendate", "ticktime")
    if not time:
        return None
    return {
        "time": time,
        "main_net_inflow": first_number(row, "main_net_inflow", "netamount", "r0_net"),
        "large_net_inflow": first_number(row, "large_net_inflow", "r1_net"),
        "medium_net_inflow": first_number(row, "medium_net_inflow", "r2_net"),
        "small_net_inflow": first_number(row, "small_net_inflow", "r3_net"),
        "close": first_number(row, "close", "trade"),
        "change_percent": first_number(row, "change_percent", "changeratio"),
    }


def _connect_rows(value: object, source_date: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    rows: list[dict[str, object]] = []
    for line in value:
        fields = _split_line(line, ",")
        if len(fields) < 6 or ":" not in text(fields[0]):
            continue
        shanghai = number(fields[1])
        shenzhen = number(fields[3])
        total = number(fields[5])
        if shanghai is None and shenzhen is None and total is None:
            continue
        rows.append(
            {
                "date": text(source_date) or None,
                "time": text(fields[0]),
                "shanghai_leg_net_inflow": None
                if shanghai is None
                else shanghai * 10_000,
                "shenzhen_leg_net_inflow": None
                if shenzhen is None
                else shenzhen * 10_000,
                "total_net_inflow": None if total is None else total * 10_000,
            }
        )
    return rows


def _split_line(value: object, separator: str) -> list[str]:
    if isinstance(value, str):
        return (
            value.strip().split(separator)
            if separator == ","
            else __import__("re").split(separator, value.strip())
        )
    if isinstance(value, list):
        return [text(item) for item in value]
    return []


def _shares(provider: Provider, value: float | int | None) -> float | int | None:
    if value is None:
        return None
    return value if provider == "sina" else value * 100


__all__ = [
    "normalize_connect_flow",
    "normalize_intraday",
    "normalize_index_quotes",
    "normalize_kline",
    "normalize_quotes",
    "normalize_ranking",
    "normalize_sector_money_flow",
    "normalize_stock_money_flow",
]
