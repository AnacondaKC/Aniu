import { useState } from "react";

import type { MarketOverview as MarketOverviewData } from "@/lib/api-types";
import { getErrorMessage } from "@/lib/format";

import styles from "./market-overview.module.css";

const MARKET_INDICES = [
  { id: "sse", name: "上证指数" },
  { id: "szse", name: "深证成指" },
  { id: "chinext", name: "创业板指" },
  { id: "star50", name: "科创综指" },
] as const;

const MARKET_RANKINGS = [
  { id: "gainers", name: "个股涨幅" },
  { id: "losers", name: "个股跌幅" },
  { id: "netInflow", name: "资金流入" },
  { id: "netOutflow", name: "资金流出" },
] as const;

type MarketRankingId = (typeof MARKET_RANKINGS)[number]["id"];

const MARKET_HOTSPOTS = [
  { id: "industry", name: "行业" },
  { id: "concept", name: "概念" },
] as const;

type MarketIndexQuote = {
  id: string;
  name: string;
  symbol: string;
  price: number;
  previous_close: number | null;
  change: number | null;
  change_percent: number | null;
  market_time: string | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  amount?: number | null;
};

type MarketTrend = {
  id: string;
  previous_close?: number | null;
  points: {
    time: string;
    price: number;
    average_price?: number | null;
    cumulative_amount?: number | null;
  }[];
};

type MarketIndexTurnoverPoint = {
  time: string;
  amount: number;
  ratio: number;
  direction: "rise" | "fall" | "flat";
};

type MarketTrendClock = {
  key: string;
  date: string;
  hour: number;
  minute: number;
  totalMinutes: number;
};

type MarketMover = {
  symbol?: string;
  code?: string;
  name: string;
  price?: number | null;
  change_percent?: number | null;
  net_inflow?: number | null;
};

type MarketNewsItem = {
  id?: string;
  title: string;
  time?: string;
  published_at?: string | null;
  summary?: string;
};

type MarketError = string | { resource: string; item_id: string | number | null; message: string };

type MarketSnapshot = {
  generated_at: string;
  indices: MarketIndexQuote[];
  trends: MarketTrend[];
  turnover: { today_amount?: number | null; previous_amount?: number | null };
  breadth: { rising: number; falling: number; flat: number } | null;
  rankings: Record<string, MarketMover[]>;
  hotspots: Record<string, MarketMover[]>;
  headlines: MarketNewsItem[];
  flash_news: MarketNewsItem[];
  errors: MarketError[];
};

type MarketOverviewProps = {
  data: MarketOverviewData | undefined;
  isLoading: boolean;
  error: unknown;
  onRefresh: () => void;
};

type ResourceState<T> =
  { kind: "loading" } | { kind: "ready"; snapshot: T } | { kind: "error"; message: string };

const PRICE_FORMATTER = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const CHANGE_FORMATTER = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const FLOW_FORMATTER = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const AMOUNT_FORMATTER = new Intl.NumberFormat("zh-CN", {
  maximumFractionDigits: 0,
});
const CHART_AMOUNT_FORMATTER = new Intl.NumberFormat("zh-CN", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const INVESTOR_QUOTES = [
  { author: "沃伦·巴菲特", quote: "在别人贪婪时恐惧，在别人恐惧时贪婪。" },
  { author: "彼得·林奇", quote: "让趋势成为你的朋友。" },
  { author: "乔治·索罗斯", quote: "身在市场，你就得准备忍受痛苦。" },
  { author: "是川银藏", quote: "买卖只吃八分饱。" },
] as const;

export function MarketOverview({ data, isLoading, error, onRefresh }: MarketOverviewProps) {
  const snapshot = data as MarketSnapshot | undefined;
  const indexState = resourceState(
    isLoading,
    snapshot?.indices,
    resourceError(snapshot, "指数"),
    error,
  );
  const trendsState = resourceState(
    isLoading,
    snapshot?.trends,
    resourceError(snapshot, "分时"),
    error,
  );
  const turnoverState = resourceState(
    isLoading,
    snapshot?.turnover,
    resourceError(snapshot, "成交"),
    error,
  );
  const breadthState = resourceState(
    isLoading,
    snapshot?.breadth ?? undefined,
    resourceError(snapshot, "涨跌") ??
      (snapshot?.breadth === null ? "涨跌家数暂不可用。" : undefined),
    error,
  );
  const moversState = resourceState(
    isLoading,
    snapshot?.rankings,
    resourceError(snapshot, "排行"),
    error,
  );
  const hotspotsState = resourceState(
    isLoading,
    snapshot?.hotspots,
    resourceError(snapshot, "热点"),
    error,
  );
  const headlinesState = resourceState(
    isLoading,
    snapshot?.headlines,
    resourceError(snapshot, "头条"),
    error,
  );
  const flashNewsState = resourceState(
    isLoading,
    snapshot?.flash_news,
    resourceError(snapshot, "快讯"),
    error,
  );

  return (
    <main className={styles.root} aria-label="A 股行情">
      <div className={styles.dashboard}>
        <section className={styles.card} aria-label="市场概览">
          {indexState.kind === "loading" ? <IndexSkeleton /> : null}
          {indexState.kind === "error" ? (
            <ErrorState
              title="暂时无法获取指数行情"
              message={indexState.message}
              onRefresh={onRefresh}
            />
          ) : null}
          {indexState.kind === "ready" ? (
            <IndexGrid snapshot={indexState.snapshot} trendsState={trendsState} />
          ) : null}
          <LiveMarketPulse
            turnoverState={turnoverState}
            breadthState={breadthState}
            onRefresh={onRefresh}
          />
        </section>

        <MarketRankingsCard state={moversState} onRefresh={onRefresh} />
        <MarketHotspotsCard state={hotspotsState} onRefresh={onRefresh} />

        <div className={styles.newsGrid}>
          <NewsCard
            title="头条要闻"
            description="东方财富精选财经要闻"
            tone="headlines"
            state={headlinesState}
            onRefresh={onRefresh}
          />
          <NewsCard
            title="24小时快讯"
            description="东方财富 7x24 实时快讯"
            tone="flash"
            state={flashNewsState}
            onRefresh={onRefresh}
          />
        </div>

        <InvestorQuoteFooter />
      </div>
    </main>
  );
}

function resourceState<T>(
  isLoading: boolean,
  value: T | undefined,
  message: string | undefined,
  queryError: unknown,
): ResourceState<T> {
  if (isLoading && value === undefined) return { kind: "loading" };
  if (value !== undefined) return { kind: "ready", snapshot: value };
  if (message !== undefined || queryError !== undefined) {
    return {
      kind: "error",
      message: message ?? getErrorMessage(queryError),
    };
  }
  return { kind: "loading" };
}

function resourceError(snapshot: MarketSnapshot | undefined, keyword: string) {
  const resourceAliases: Record<string, string[]> = {
    指数: ["index", "indices"],
    分时: ["trend", "trends"],
    成交: ["turnover"],
    涨跌: ["breadth"],
    排行: ["ranking", "rankings"],
    热点: ["hotspot", "hotspots"],
    头条: ["headline", "headlines"],
    快讯: ["flash", "flash_news"],
  };
  const aliases = resourceAliases[keyword] ?? [];
  const error = snapshot?.errors.find((candidate) => {
    if (typeof candidate === "string") return candidate.includes(keyword);
    const resource = candidate.resource.toLowerCase();
    return aliases.some((alias) => resource.includes(alias));
  });
  return typeof error === "string" ? error : error?.message;
}

function InvestorQuoteFooter() {
  const [quote] = useState(
    () => INVESTOR_QUOTES[Math.floor(Math.random() * INVESTOR_QUOTES.length)] ?? INVESTOR_QUOTES[0],
  );
  return (
    <blockquote className={styles.investorQuote} aria-label="投资名言">
      <p>{quote.quote}</p>
      <cite>—— {quote.author}</cite>
    </blockquote>
  );
}

export function MarketUpdateTime({ data }: { data: MarketOverviewData | undefined }) {
  const snapshot = data as MarketSnapshot | undefined;
  if (snapshot === undefined) return null;
  return (
    <span className={styles.marketUpdateTime}>
      {formatUpdateTime(latestMarketTime(snapshot.indices, snapshot.generated_at))}
    </span>
  );
}

function formatUpdateTime(value: string): string {
  const date = new Date(value);
  if (!Number.isNaN(date.getTime())) {
    const parts = new Intl.DateTimeFormat("zh-CN", {
      month: "numeric",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).formatToParts(date);
    const get = (type: Intl.DateTimeFormatPartTypes) =>
      parts.find((part) => part.type === type)?.value ?? "--";
    return `最近刷新：${get("month")}月${get("day")}日 ${get("hour")}时${get("minute")}分${get("second")}秒`;
  }
  return `数据更新时间：${value}`;
}

function LiveMarketPulse({
  turnoverState,
  breadthState,
  onRefresh,
}: {
  turnoverState: ResourceState<MarketSnapshot["turnover"]>;
  breadthState: ResourceState<MarketSnapshot["breadth"]>;
  onRefresh: () => void;
}) {
  return (
    <div className={styles.marketPulse}>
      <div className={styles.marketPulseMetric} aria-label="沪深股票涨跌分布">
        {breadthState.kind === "loading" ? (
          <span className={`${styles.skeleton} ${styles.pulseSkeleton}`} />
        ) : null}
        {breadthState.kind === "error" ? (
          <>
            <span className={styles.pulseUnavailable} role="status">
              {breadthState.message}
            </span>
            <button type="button" className={styles.textButton} onClick={onRefresh}>
              重新加载
            </button>
          </>
        ) : null}
        {breadthState.kind === "ready" && breadthState.snapshot ? (
          <MarketBreadthMetrics breadth={breadthState.snapshot} compact />
        ) : null}
      </div>
      <div className={styles.marketTurnoverRow} aria-label="沪深两市成交额">
        {turnoverState.kind === "loading" ? (
          <span className={`${styles.skeleton} ${styles.turnoverSkeleton}`} />
        ) : null}
        {turnoverState.kind === "error" ? (
          <button type="button" className={styles.textButton} onClick={onRefresh}>
            成交额暂不可用，重新加载
          </button>
        ) : null}
        {turnoverState.kind === "ready" ? (
          <TurnoverMetric snapshot={turnoverState.snapshot} />
        ) : null}
      </div>
    </div>
  );
}

function TurnoverMetric({ snapshot }: { snapshot: MarketSnapshot["turnover"] }) {
  const todayAmount = snapshot.today_amount;
  const previousAmount = snapshot.previous_amount;
  if (typeof todayAmount !== "number") {
    return <span className={styles.textButton}>成交额暂不可用</span>;
  }
  const delta = typeof previousAmount === "number" ? todayAmount - previousAmount : undefined;
  const tone = toneFor(delta ?? 0);
  const comparison =
    delta === undefined
      ? "较上一日暂无数据"
      : delta > 0
        ? "较上一日增量 +" + formatAmountInHundredMillions(delta)
        : delta < 0
          ? "较上一日缩量 -" + formatAmountInHundredMillions(delta)
          : "较上一日持平 0亿";
  return (
    <div className={styles.turnoverMetric}>
      <span className={styles.turnoverTotal}>
        两市成交总额 <strong>{formatAmountInHundredMillions(todayAmount)}</strong>
      </span>
      <span className={styles.turnoverDelta + " " + styles[tone]}>{comparison}</span>
    </div>
  );
}

function MarketRankingsCard({
  state,
  onRefresh,
}: {
  state: ResourceState<MarketSnapshot["rankings"]>;
  onRefresh: () => void;
}) {
  return (
    <section className={styles.rankingSection} aria-label="实时排行">
      {state.kind === "error" ? (
        <div className={styles.insightError} role="alert">
          <span>{state.message}</span>
          <button type="button" className={styles.textButton} onClick={onRefresh}>
            重新加载实时排行
          </button>
        </div>
      ) : null}
      {state.kind !== "error" ? (
        <div className={styles.rankingCardGrid}>
          {MARKET_RANKINGS.map((ranking) => (
            <article
              className={`${styles.rankingMetricCard} ${styles[rankingCardTone(ranking.id)]}`}
              aria-label={`${ranking.name}排行卡片`}
              key={ranking.id}
            >
              <h4>{ranking.name}</h4>
              {state.kind === "loading" ? (
                <InsightSkeleton rows={5} />
              ) : (
                <MoverList rows={rankingRows(state.snapshot, ranking.id)} ranking={ranking.id} />
              )}
            </article>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function rankingRows(
  rankings: MarketSnapshot["rankings"],
  ranking: MarketRankingId,
): MarketMover[] {
  const key =
    ranking === "netInflow" ? "net_inflow" : ranking === "netOutflow" ? "net_outflow" : ranking;
  return rankings[key] ?? [];
}

function rankingCardTone(
  ranking: MarketRankingId,
): "rankingMetricCardRise" | "rankingMetricCardFall" {
  return ranking === "gainers" || ranking === "netInflow"
    ? "rankingMetricCardRise"
    : "rankingMetricCardFall";
}

function MoverList({ rows, ranking }: { rows: MarketMover[]; ranking: MarketRankingId }) {
  return (
    <div className={styles.marketRankList} aria-label={`${rankingLabel(ranking)}排行`}>
      {rows.map((stock, index) => (
        <div className={styles.marketRankRow} key={stock.code ?? stock.symbol ?? stock.name}>
          <span className={styles.marketRankIndex}>{index + 1}</span>
          <span className={styles.stockFlowIdentity}>
            <span className={styles.stockFlowName}>{stock.name}</span>
            <small className={styles.stockFlowCode}>{stock.code ?? stock.symbol ?? "--"}</small>
          </span>
          <span className={styles.stockFlowMetrics}>
            <strong className={`${styles.insightValue} ${styles[rankingTone(stock, ranking)]}`}>
              {rankingValue(stock, ranking)}
            </strong>
            <small
              className={`${styles.stockFlowChange} ${styles[toneFor(stock.change_percent ?? 0)]}`}
            >
              {ranking === "gainers" || ranking === "losers"
                ? formatPrice(stock.price)
                : `${signed(stock.change_percent ?? 0)}%`}
            </small>
          </span>
        </div>
      ))}
      {rows.length === 0 ? <p className={styles.emptyRows}>暂无数据</p> : null}
    </div>
  );
}

function MarketHotspotsCard({
  state,
  onRefresh,
}: {
  state: ResourceState<MarketSnapshot["hotspots"]>;
  onRefresh: () => void;
}) {
  return (
    <section className={`${styles.card} ${styles.hotspotSection}`} aria-label="板块热点">
      {state.kind === "error" ? (
        <div className={styles.insightError} role="alert">
          <span>{state.message}</span>
          <button type="button" className={styles.textButton} onClick={onRefresh}>
            重新加载板块热点
          </button>
        </div>
      ) : null}
      {state.kind !== "error" ? (
        <div className={styles.hotspotCardGrid}>
          {MARKET_HOTSPOTS.map((group) => (
            <article
              className={`${styles.hotspotMetricCard} ${group.id === "industry" ? styles.hotspotMetricCardIndustry : styles.hotspotMetricCardConcept}`}
              aria-label={`${group.name}涨幅卡片`}
              key={group.id}
            >
              <h4>{group.name}涨幅</h4>
              {state.kind === "loading" ? (
                <InsightSkeleton rows={5} />
              ) : (
                <HotspotList
                  rows={state.snapshot[group.id] ?? []}
                  groupName={`${group.name}涨幅`}
                />
              )}
            </article>
          ))}
        </div>
      ) : null}
    </section>
  );
}

function HotspotList({ rows, groupName }: { rows: MarketMover[]; groupName: string }) {
  return (
    <div className={styles.marketRankList} aria-label={`${groupName}排行`}>
      {rows.map((hotspot, index) => (
        <div className={styles.marketRankRow} key={hotspot.code ?? hotspot.symbol ?? hotspot.name}>
          <span className={styles.marketRankIndex}>{index + 1}</span>
          <span className={styles.stockFlowIdentity}>
            <span className={styles.stockFlowName}>{hotspot.name}</span>
            {(hotspot.code ?? hotspot.symbol) ? (
              <small className={styles.stockFlowCode}>{hotspot.code ?? hotspot.symbol}</small>
            ) : null}
          </span>
          <strong
            className={`${styles.insightValue} ${styles[toneFor(hotspot.change_percent ?? 0)]}`}
          >
            {signed(hotspot.change_percent ?? 0)}%
          </strong>
        </div>
      ))}
      {rows.length === 0 ? <p className={styles.emptyRows}>暂无数据</p> : null}
    </div>
  );
}

function MarketBreadthMetrics({
  breadth,
  compact = false,
}: {
  breadth: NonNullable<MarketSnapshot["breadth"]>;
  compact?: boolean;
}) {
  const total = breadth.rising + breadth.falling + breadth.flat;
  const percentage = (value: number) => `${total === 0 ? 0 : (value / total) * 100}%`;
  const ariaLabel = compact
    ? `沪深A股涨跌：上涨 ${breadth.rising}，下跌 ${breadth.falling}`
    : `沪深A股涨跌：上涨 ${breadth.rising}，下跌 ${breadth.falling}，平盘 ${breadth.flat}`;
  return (
    <div
      className={
        compact ? `${styles.marketBreadth} ${styles.marketBreadthCompact}` : styles.marketBreadth
      }
      aria-label={ariaLabel}
    >
      <div className={styles.marketBreadthLabels}>
        <span className={styles.marketBreadthFall}>
          跌 <strong>{breadth.falling}</strong>
        </span>
        {!compact ? (
          <span className={styles.marketBreadthFlat}>
            平 <strong>{breadth.flat}</strong>
          </span>
        ) : null}
        <span className={styles.marketBreadthRise}>
          涨 <strong>{breadth.rising}</strong>
        </span>
      </div>
      <div className={styles.marketBreadthTrack} aria-hidden="true">
        <span
          className={styles.marketBreadthFallBar}
          style={{ width: percentage(breadth.falling) }}
        />
        <span className={styles.marketBreadthFlatBar} style={{ width: percentage(breadth.flat) }} />
        <span
          className={styles.marketBreadthRiseBar}
          style={{ width: percentage(breadth.rising) }}
        />
      </div>
    </div>
  );
}

function IndexGrid({
  snapshot,
  trendsState,
}: {
  snapshot: MarketIndexQuote[];
  trendsState: ResourceState<MarketTrend[]>;
}) {
  const quoteById = new Map(snapshot.map((quote) => [quote.id, quote]));
  const trendById = new Map(
    trendsState.kind === "ready" ? trendsState.snapshot.map((trend) => [trend.id, trend]) : [],
  );
  return (
    <>
      <div className={styles.quoteGrid}>
        {MARKET_INDICES.map((index) => (
          <IndexCard
            key={index.id}
            index={index}
            quote={quoteById.get(index.id)}
            trend={trendById.get(index.id)}
            trendLoading={trendsState.kind === "loading"}
          />
        ))}
      </div>
      {snapshot.length < MARKET_INDICES.length ? (
        <p className={styles.partialNotice}>
          暂不可用：
          {MARKET_INDICES.filter((index) => !quoteById.has(index.id))
            .map((index) => index.name)
            .join("、")}
        </p>
      ) : null}
    </>
  );
}

function IndexCard({
  index,
  quote,
  trend,
  trendLoading,
}: {
  index: (typeof MARKET_INDICES)[number];
  quote: MarketIndexQuote | undefined;
  trend: MarketTrend | undefined;
  trendLoading: boolean;
}) {
  if (quote === undefined) {
    return (
      <article
        className={`${styles.quoteCard} ${styles.quoteUnavailable}`}
        aria-label={`${index.name}暂不可用`}
      >
        <p className={styles.quoteName}>{index.name}</p>
        <p className={styles.unavailableValue}>--</p>
        <p className={styles.quoteTime}>当前指数暂不可用</p>
      </article>
    );
  }
  const tone = toneFor(quote.change ?? 0);
  const cardTone =
    quote.change && quote.change > 0
      ? "quoteCardRise"
      : quote.change && quote.change < 0
        ? "quoteCardFall"
        : "quoteCardFlat";
  return (
    <article className={`${styles.quoteCard} ${styles[cardTone]}`} aria-label={index.name}>
      <p className={styles.quoteName}>{index.name}</p>
      <p className={`${styles.quotePrice} ${styles[tone]}`}>{formatPrice(quote.price)}</p>
      <p className={`${styles.quoteChange} ${styles[tone]}`}>
        <span>{signed(quote.change ?? 0)}</span>
        <span>{signed(quote.change_percent ?? 0)}%</span>
      </p>
      <IndexSparkline indexName={index.name} trend={trend} tone={tone} loading={trendLoading} />
      <div className={styles.quoteStats}>
        <span>
          高 <strong>{formatPrice(quote.high)}</strong>
        </span>
        <span>
          低 <strong>{formatPrice(quote.low)}</strong>
        </span>
      </div>
      <IndexTurnoverChart indexName={index.name} trend={trend} loading={trendLoading} />
    </article>
  );
}

function IndexTurnoverChart({
  indexName,
  trend,
  loading,
}: {
  indexName: string;
  trend: MarketTrend | undefined;
  loading: boolean;
}) {
  const title = indexName + "分时成交额";
  if (loading) {
    return (
      <div className={styles.indexTurnoverChart} aria-label={title + "加载中"} aria-busy="true">
        <div className={styles.indexTurnoverHeading}>
          <span>成交额</span>
          <small>分时</small>
        </div>
        <span className={`${styles.skeleton} ${styles.indexTurnoverSkeleton}`} />
      </div>
    );
  }
  const points = trend === undefined ? [] : marketIndexTurnoverBuckets(trend);
  if (points.length === 0) {
    return (
      <div className={styles.indexTurnoverUnavailable} aria-label={title + "暂不可用"}>
        成交额暂无数据
      </div>
    );
  }
  const maximum = Math.max(...points.map((point) => point.amount));
  return (
    <div className={styles.indexTurnoverChart} aria-label={title}>
      <div className={styles.indexTurnoverHeading}>
        <span>成交额</span>
        <small>{points.at(-1)?.time}</small>
      </div>
      <div
        className={styles.indexTurnoverBars}
        role="img"
        aria-label={title + "变化，共 " + points.length + " 个时段"}
      >
        {points.map((point) => (
          <span
            className={styles.indexTurnoverBar + " " + indexTurnoverBarClass(point.direction)}
            data-slot="index-turnover-bar"
            style={{
              height: (maximum === 0 ? 0 : (point.amount / maximum) * 100) + "%",
              left: Math.min(point.ratio, 47 / 48) * 100 + "%",
            }}
            title={point.time + " " + formatChartAmount(point.amount)}
            aria-hidden="true"
            key={point.time}
          />
        ))}
      </div>
      <div className={styles.indexTurnoverAxis} aria-hidden="true">
        <span>09:30</span>
        <span>11:30</span>
        <span>13:00</span>
        <span>15:00</span>
      </div>
    </div>
  );
}

function indexTurnoverBarClass(direction: MarketIndexTurnoverPoint["direction"]) {
  if (direction === "rise") return styles.indexTurnoverBarRise;
  if (direction === "fall") return styles.indexTurnoverBarFall;
  return styles.indexTurnoverBarFlat;
}

function IndexSparkline({
  indexName,
  trend,
  tone,
  loading,
}: {
  indexName: string;
  trend: MarketTrend | undefined;
  tone: "insightRise" | "insightFall" | "insightFlat";
  loading: boolean;
}) {
  if (trend === undefined) {
    return (
      <div
        className={styles.sparklinePlaceholder}
        aria-label={`${indexName}分时${loading ? "加载中" : "暂不可用"}`}
      >
        {loading ? <span className={styles.skeleton} /> : null}
      </div>
    );
  }
  const width = 120;
  const height = 38;
  const plottedPoints = trend.points.flatMap((point) => {
    const timeRatio = marketTrendTimeRatio(point.time);
    return timeRatio === undefined ? [] : [{ point, x: timeRatio * width }];
  });
  if (plottedPoints.length === 0) {
    return <div className={styles.sparklinePlaceholder} aria-label={`${indexName}分时暂不可用`} />;
  }
  const previousClose = trend.previous_close ?? plottedPoints[0]?.point.price ?? 0;
  const values = [...plottedPoints.map(({ point }) => point.price), previousClose];
  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const range = maximum - minimum || Math.max(Math.abs(maximum) * 0.002, 1);
  const padding = range * 0.12;
  const chartMin = minimum - padding;
  const chartRange = range + padding * 2;
  const yFor = (value: number) => height - ((value - chartMin) / chartRange) * height;
  const points = plottedPoints
    .map(({ point, x }) => `${x.toFixed(2)},${yFor(point.price).toFixed(2)}`)
    .join(" ");
  return (
    <svg
      className={`${styles.sparkline} ${styles[tone]}`}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={`${indexName}当日分时走势`}
    >
      <line
        className={styles.sparklineBaseline}
        x1="0"
        x2={width}
        y1={yFor(previousClose)}
        y2={yFor(previousClose)}
      />
      <polyline className={styles.sparklineLine} points={points} />
    </svg>
  );
}

function IndexSkeleton() {
  return (
    <div className={styles.quoteGrid} aria-label="指数行情加载中" aria-busy="true">
      {MARKET_INDICES.map((index) => (
        <div key={index.id} className={styles.quoteCard}>
          <span className={`${styles.skeleton} ${styles.skeletonName}`} />
          <span className={`${styles.skeleton} ${styles.skeletonPrice}`} />
          <span className={`${styles.skeleton} ${styles.skeletonChange}`} />
          <span className={`${styles.skeleton} ${styles.skeletonTime}`} />
        </div>
      ))}
    </div>
  );
}

function InsightSkeleton({ rows }: { rows: number }) {
  return (
    <div className={styles.insightSkeleton} aria-label="行情数据加载中" aria-busy="true">
      {Array.from({ length: rows }, (_, index) => (
        <span className={styles.pendingRow} key={index} aria-hidden="true">
          <i className={styles.skeleton} />
          <i className={`${styles.skeleton} ${styles.pendingValue}`} />
        </span>
      ))}
    </div>
  );
}

function NewsCard({
  title,
  description,
  tone,
  state,
  onRefresh,
}: {
  title: string;
  description: string;
  tone: "headlines" | "flash";
  state: ResourceState<MarketNewsItem[]>;
  onRefresh: () => void;
}) {
  return (
    <section
      className={`${styles.newsCard} ${tone === "headlines" ? styles.newsCardHeadlines : styles.newsCardFlash}`}
    >
      <CardHeading title={title} description={description} />
      {state.kind === "loading" ? <InsightSkeleton rows={6} /> : null}
      {state.kind === "error" ? (
        <div className={styles.insightError}>
          <span>{state.message}</span>
          <button type="button" className={styles.textButton} onClick={onRefresh}>
            重新加载
          </button>
        </div>
      ) : null}
      {state.kind === "ready" ? <NewsList items={state.snapshot} /> : null}
    </section>
  );
}

function NewsList({ items }: { items: MarketNewsItem[] }) {
  return (
    <div className={styles.newsList}>
      {items.slice(0, 10).map((item, index) => (
        <div
          className={styles.newsRow}
          key={`${item.id ?? item.title}-${item.time ?? item.published_at ?? ""}-${index}`}
        >
          <span className={styles.newsTime}>{newsTime(item.time ?? item.published_at)}</span>
          <span className={styles.newsTitle} title={item.summary}>
            {item.title}
          </span>
        </div>
      ))}
      {items.length === 0 ? <p className={styles.emptyRows}>暂无资讯</p> : null}
    </div>
  );
}

function CardHeading({ title, description }: { title: string; description: string }) {
  return (
    <header className={styles.cardHeader}>
      <div>
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </header>
  );
}

function ErrorState({
  title,
  message,
  onRefresh,
}: {
  title: string;
  message: string;
  onRefresh: () => void;
}) {
  return (
    <div className={styles.errorState} role="alert">
      <strong>{title}</strong>
      <span>{message}</span>
      <button type="button" className={styles.retryButton} onClick={onRefresh}>
        重新加载
      </button>
    </div>
  );
}

function rankingLabel(ranking: MarketRankingId) {
  return MARKET_RANKINGS.find((candidate) => candidate.id === ranking)?.name ?? "个股";
}

function rankingTone(
  stock: MarketMover,
  ranking: MarketRankingId,
): "insightRise" | "insightFall" | "insightFlat" {
  return ranking === "netInflow" || ranking === "netOutflow"
    ? toneFor(stock.net_inflow ?? 0)
    : toneFor(stock.change_percent ?? 0);
}

function rankingValue(stock: MarketMover, ranking: MarketRankingId) {
  switch (ranking) {
    case "gainers":
    case "losers":
      return `${signed(stock.change_percent ?? 0)}%`;
    case "netInflow":
    case "netOutflow":
      return flowInHundredMillions(stock.net_inflow ?? 0);
  }
}

function flowInHundredMillions(value: number) {
  const formatted = FLOW_FORMATTER.format(Math.abs(value) / 100_000_000);
  if (value > 0) return `+${formatted} 亿`;
  if (value < 0) return `-${formatted} 亿`;
  return `${formatted} 亿`;
}

function toneFor(value: number): "insightRise" | "insightFall" | "insightFlat" {
  if (value > 0) return "insightRise";
  if (value < 0) return "insightFall";
  return "insightFlat";
}

function signed(value: number) {
  const formatted = CHANGE_FORMATTER.format(Math.abs(value));
  if (value > 0) return `+${formatted}`;
  if (value < 0) return `-${formatted}`;
  return formatted;
}

function formatPrice(value: number | null | undefined) {
  return value === null || value === undefined || Number.isNaN(value)
    ? "--"
    : PRICE_FORMATTER.format(value);
}

function formatAmountInHundredMillions(value: number) {
  return `${AMOUNT_FORMATTER.format(Math.abs(value) / 100_000_000)}亿`;
}

function formatChartAmount(value: number) {
  return `${CHART_AMOUNT_FORMATTER.format(Math.abs(value) / 100_000_000)}亿`;
}

function marketIndexTurnoverBuckets(trend: MarketTrend): MarketIndexTurnoverPoint[] {
  const amounts = [...trendCumulativeAmounts(trend).values()].sort(
    (left, right) => left.clock.totalMinutes - right.clock.totalMinutes,
  );
  const latestDate = amounts.at(-1)?.clock.date;
  if (latestDate === undefined) return [];

  const buckets = new Map<
    string,
    MarketIndexTurnoverPoint & { openPrice: number | null; closePrice: number | null }
  >();
  let previous: (typeof amounts)[number] | undefined;
  for (const current of amounts.filter((item) => item.clock.date === latestDate)) {
    const isNewSession =
      previous === undefined ||
      previous.clock.date !== current.clock.date ||
      current.clock.totalMinutes < previous.clock.totalMinutes;
    const amount =
      previous === undefined || isNewSession ? current.amount : current.amount - previous.amount;
    if (Number.isFinite(amount) && amount >= 0) {
      const minute = Math.floor(current.clock.minute / 5) * 5;
      const time =
        String(current.clock.hour).padStart(2, "0") + ":" + String(minute).padStart(2, "0");
      const ratio = marketTrendTimeRatio(time);
      if (ratio !== undefined) {
        const previousPrice = previous?.price ?? trend.previous_close ?? null;
        const existing = buckets.get(time);
        const openPrice = existing?.openPrice ?? previousPrice;
        const closePrice = current.price;
        const direction =
          openPrice === null || closePrice === null
            ? "flat"
            : closePrice > openPrice
              ? "rise"
              : closePrice < openPrice
                ? "fall"
                : "flat";
        buckets.set(time, {
          time,
          amount: (existing?.amount ?? 0) + amount,
          ratio,
          direction,
          openPrice,
          closePrice,
        });
      }
    }
    previous = current;
  }
  return [...buckets.values()].map((point) => {
    const { openPrice, closePrice, ...result } = point;
    void openPrice;
    void closePrice;
    return result;
  });
}

function trendCumulativeAmounts(
  trend: MarketTrend,
): Map<string, { clock: MarketTrendClock; amount: number; price: number | null }> {
  const amounts = new Map<
    string,
    { clock: MarketTrendClock; amount: number; price: number | null }
  >();
  for (const point of trend.points) {
    const clock = parseMarketTrendClock(point.time);
    const amount = point.cumulative_amount;
    if (
      clock !== undefined &&
      marketTrendTimeRatio(point.time) !== undefined &&
      typeof amount === "number" &&
      Number.isFinite(amount) &&
      amount >= 0
    ) {
      amounts.set(clock.key, {
        clock,
        amount,
        price: typeof point.price === "number" && Number.isFinite(point.price) ? point.price : null,
      });
    }
  }
  return amounts;
}

function parseMarketTrendClock(value: string): MarketTrendClock | undefined {
  const normalized = value.trim();
  const dateMatch = /^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/u.exec(normalized);
  const timeMatch = /(?:^|[T\s])(\d{1,2}):(\d{2})(?::\d{2})?/u.exec(normalized);
  if (timeMatch === null) return undefined;
  const hour = Number(timeMatch[1]);
  const minute = Number(timeMatch[2]);
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour > 23 || minute > 59) {
    return undefined;
  }
  const date =
    dateMatch === null
      ? ""
      : `${dateMatch[1] ?? ""}-${(dateMatch[2] ?? "").padStart(2, "0")}-${(dateMatch[3] ?? "").padStart(2, "0")}`;
  return {
    key: `${date}|${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`,
    date,
    hour,
    minute,
    totalMinutes: hour * 60 + minute,
  };
}

function latestMarketTime(indices: MarketIndexQuote[], fallback: string) {
  return (
    indices
      .map((index) => index.market_time)
      .filter(Boolean)
      .sort()
      .at(-1) ?? fallback
  );
}

function newsTime(value: string | null | undefined) {
  if (!value) return "--";
  const match = /(?:T|\s)(\d{2}):(\d{2})/.exec(value);
  return match ? `${match[1]}:${match[2]}` : value;
}

function marketTrendTimeRatio(time: string): number | undefined {
  const match = /(?:^|\s|T)(\d{1,2}):(\d{2})(?::\d{2})?(?:\s|$|Z)/u.exec(time.trim());
  if (match === null) return undefined;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes) || hours > 23 || minutes > 59)
    return undefined;
  const minuteOfDay = hours * 60 + minutes;
  const morningOpen = 9 * 60 + 30;
  const morningClose = 11 * 60 + 30;
  const afternoonOpen = 13 * 60;
  const afternoonClose = 15 * 60;
  if (minuteOfDay >= morningOpen && minuteOfDay <= morningClose) {
    return (minuteOfDay - morningOpen) / (4 * 60);
  }
  if (minuteOfDay >= afternoonOpen && minuteOfDay <= afternoonClose) {
    return (morningClose - morningOpen + minuteOfDay - afternoonOpen) / (4 * 60);
  }
  return undefined;
}
