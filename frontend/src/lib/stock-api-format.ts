import type { StockApiLogToolSource, StockApiProvider } from "@/lib/api-types";

const stockApiProviderLabels: Record<StockApiProvider, string> = {
  mx: "妙想接口",
  eastmoney: "东方财富",
  tencent: "腾讯财经",
  sina: "新浪财经",
};

const stockApiToolSourceLabels: Record<StockApiLogToolSource, string> = {
  public: "公开数据",
  aggregate: "聚合研判",
  mx: "妙想接口",
};

const parameterKeyLabels: Record<string, string> = {
  path: "路径参数",
  query: "查询参数",
  body: "请求体",
  query_string: "查询内容",
  queryString: "查询内容",
  keyword: "关键词",
  instrument: "查询标的",
  instrument_type: "标的类型",
  resolved_instrument: "规范标的",
  data_source: "数据来源",
  symbol: "股票代码",
  symbols: "股票代码列表",
  stock_code: "股票代码",
  stock_codes: "股票代码列表",
  code: "代码",
  codes: "代码列表",
  index_code: "指数代码",
  index_codes: "指数代码列表",
  moneyUnit: "金额单位",
  money_unit: "金额单位",
  market: "市场",
  market_code: "市场代码",
  start_date: "开始日期",
  begin_date: "开始日期",
  end_date: "结束日期",
  date: "日期",
  dates: "日期列表",
  period: "周期",
  frequency: "频率",
  interval: "间隔",
  page: "页码",
  page_size: "每页数量",
  limit: "数量上限",
  offset: "偏移量",
  type: "类型",
  action: "动作",
  detail: "详情",
  adjust: "复权",
  days: "天数",
  full: "返回全量",
  content: "内容",
  report_id: "研报编号",
  feed: "资讯流",
  sector_type: "板块类型",
  mode: "模式",
  order: "排序方向",
  category: "分类",
  group: "分组",
  group_id: "分组标识",
  sector: "板块",
  concept: "概念",
  order_id: "委托编号",
  order_ids: "委托编号列表",
  account: "账户",
  account_id: "账户标识",
  field: "字段",
  fields: "字段列表",
  sort: "排序",
  direction: "方向",
  value: "值",
  values: "值列表",
  condition: "条件",
  conditions: "条件列表",
};

const parameterValueLabels: Record<string, Record<string, string>> = {
  instrument_type: {
    stock: "个股",
    index: "指数",
  },
  data_source: {
    public: "公开数据",
    mx: "妙想接口",
  },
};

function formatParameterKey(key: string) {
  return parameterKeyLabels[key] ?? "参数";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatParameterValue(value: unknown, key?: string): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) {
    return value.map((item) => formatParameterValue(item, key)).join("、");
  }
  if (isRecord(value)) {
    return Object.entries(value)
      .map(
        ([itemKey, item]) =>
          `${formatParameterKey(itemKey)}：${formatParameterValue(item, itemKey)}`,
      )
      .join("；");
  }
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "string") return (key && parameterValueLabels[key]?.[value]) ?? value;
  if (typeof value === "number") return value.toLocaleString("zh-CN");
  return "—";
}

export function formatStockApiProvider(provider: StockApiProvider) {
  return stockApiProviderLabels[provider];
}

export function formatStockApiToolSource(source: StockApiLogToolSource) {
  return stockApiToolSourceLabels[source];
}

export function formatStockApiParameters(value: unknown, maximumLength = 180) {
  if (value === null || value === undefined) return "—";
  const text = isRecord(value)
    ? Object.entries(value)
        .map(([key, item]) => `${formatParameterKey(key)}：${formatParameterValue(item, key)}`)
        .join("；")
    : formatParameterValue(value);
  return text.length > maximumLength ? `${text.slice(0, maximumLength - 1)}…` : text;
}
