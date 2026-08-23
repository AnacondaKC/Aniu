from backend.business.settings import AppSettings
from backend.business.settings.dto import to_settings_dto, to_stock_api_settings_dto


def test_settings_dtos_expose_mx_configuration_and_data_catalog() -> None:
    settings = AppSettings(mx_api_key="mx-secret-7890")
    app_dto = to_settings_dto(settings)
    stock_api_dto = to_stock_api_settings_dto(settings)

    assert app_dto.mx.api_key_configured is True
    assert app_dto.mx.api_key_last_four == "7890"
    assert [item.id for item in stock_api_dto.mx.interfaces] == [
        "news",
        "data",
        "screening",
        "portfolio",
    ]
    assert stock_api_dto.mx.interfaces[-1].name == "模拟交易"
    assert stock_api_dto.mx.interfaces[-1].access_modes == ("read", "write")
    assert all(item.features and item.examples for item in stock_api_dto.mx.interfaces)
    assert stock_api_dto.public_stock.name == "公开数据"
    assert [item.tool_name for item in stock_api_dto.public_stock.tools] == [
        "stock_quote",
        "query_kline",
        "stock_intraday",
        "stock_ranking",
        "stock_money_flow",
        "stock_fundamentals",
        "stock_research",
        "stock_news",
        "market_snapshot",
        "portfolio_stock_snapshot",
        "stock_analysis",
        "industry_snapshot",
    ]
    assert stock_api_dto.public_stock.providers == (
        "tencent",
        "sina",
        "eastmoney",
    )
    assert stock_api_dto.public_stock.features == (
        "实时行情",
        "K 线与分时",
        "资金流向",
        "基本数据",
        "研报预测",
        "资讯公告",
        "聚合研判",
    )
    names_by_tool_name = {
        item.tool_name: item.name for item in stock_api_dto.public_stock.tools
    }
    assert names_by_tool_name["stock_fundamentals"] == "基本数据"
    assert names_by_tool_name["stock_research"] == "研报预测"
    assert names_by_tool_name["stock_news"] == "资讯公告"
    assert names_by_tool_name["market_snapshot"] == "行情查询"
    assert names_by_tool_name["portfolio_stock_snapshot"] == "持仓查询"
    assert names_by_tool_name["stock_analysis"] == "个股查询"
    assert names_by_tool_name["industry_snapshot"] == "热度板块"
    assert stock_api_dto.public_stock.tools[0].actions == ("行情快照",)
    assert stock_api_dto.public_stock.tools[0].providers == (
        "tencent",
        "sina",
        "eastmoney",
    )
    assert stock_api_dto.public_stock.tools[1].providers == (
        "tencent",
        "sina",
        "eastmoney",
    )
    assert stock_api_dto.public_stock.tools[2].providers == (
        "tencent",
        "eastmoney",
    )
    assert stock_api_dto.public_stock.tools[-1].providers == ("eastmoney",)
    assert stock_api_dto.public_stock.tools[-2].providers == (
        "tencent",
        "sina",
        "eastmoney",
    )
    assert all(
        item.summary and item.actions for item in stock_api_dto.public_stock.tools
    )
    assert not hasattr(stock_api_dto, "revision")
    assert not hasattr(stock_api_dto, "market_data")
    assert not hasattr(stock_api_dto, "data_interfaces")
