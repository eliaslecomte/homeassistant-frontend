import type { HomeAssistant } from "../types";
import { fetchStatistics } from "./recorder";
import type { Statistics } from "./recorder";
import { ERR_UNKNOWN_COMMAND } from "./websocket_api";

type StatisticsArguments =
  Parameters<typeof fetchStatistics> extends [
    HomeAssistant["callWS"],
    ...infer Arguments,
  ]
    ? Arguments
    : never;

type ConditionalStatistics =
  | { cache_token: string; not_modified: true }
  | {
      cache_token: string | null;
      not_modified: false;
      statistics: Statistics;
    };

interface CachedStatistics {
  token: string;
  statistics: Statistics;
  size: number;
}

const MAX_ENTRIES = 24;
const MAX_DATA_POINTS = 100_000;

class StatisticsCache {
  private _entries = new Map<string, CachedStatistics>();

  private _pending = new Map<string, Promise<Statistics>>();

  private _size = 0;

  private _supported = true;

  constructor(private _callWS: HomeAssistant["callWS"]) {}

  async fetch(...args: StatisticsArguments): Promise<Statistics> {
    const [start, end, ids, period = "hour", units, types] = args;
    const message = {
      start_time: start.toISOString(),
      end_time: end?.toISOString(),
      statistic_ids: ids && [...new Set(ids)].sort(),
      period,
      units: units && Object.fromEntries(Object.entries(units).sort()),
      types: types && [...types].sort(),
    };
    const key = JSON.stringify(message);
    let pending = this._pending.get(key);
    if (!pending) {
      pending = this._fetch(key, message, args);
      this._pending.set(key, pending);
    }
    try {
      // Energy's power backfill and other consumers can modify the response.
      return structuredClone(await pending);
    } finally {
      if (this._pending.get(key) === pending) {
        this._pending.delete(key);
      }
    }
  }

  private async _fetch(
    key: string,
    message: Record<string, unknown>,
    args: StatisticsArguments
  ): Promise<Statistics> {
    if (!this._supported) {
      return fetchStatistics(this._callWS, ...args);
    }
    const cached = this._entries.get(key);
    let response: ConditionalStatistics;
    try {
      response = await this._callWS<ConditionalStatistics>({
        ...message,
        type: "recorder/statistics_during_period_cached",
        cache_token: cached?.token,
      });
    } catch (err: unknown) {
      if (
        typeof err !== "object" ||
        err === null ||
        !("code" in err) ||
        err.code !== ERR_UNKNOWN_COMMAND
      ) {
        throw err;
      }
      this._supported = false;
      this._entries.clear();
      this._size = 0;
      return fetchStatistics(this._callWS, ...args);
    }
    if (response.not_modified) {
      if (!cached) {
        throw new Error(
          "Statistics validation returned no data for an uncached request"
        );
      }
      this._store(key, cached);
      return cached.statistics;
    }
    const size = Object.values(response.statistics).reduce(
      (total, values) => total + values.length,
      0
    );
    this._store(
      key,
      response.cache_token
        ? { token: response.cache_token, statistics: response.statistics, size }
        : undefined
    );
    return response.statistics;
  }

  private _store(key: string, entry?: CachedStatistics): void {
    // The entry captured before the request may have been evicted meanwhile.
    const existing = this._entries.get(key);
    if (existing) {
      this._size -= existing.size;
      this._entries.delete(key);
    }
    if (!entry || !this._supported || entry.size > MAX_DATA_POINTS) {
      return;
    }
    this._entries.set(key, entry);
    this._size += entry.size;
    while (this._entries.size > MAX_ENTRIES || this._size > MAX_DATA_POINTS) {
      const oldest = this._entries.keys().next().value!;
      this._size -= this._entries.get(oldest)!.size;
      this._entries.delete(oldest);
    }
  }
}

const caches = new WeakMap<HomeAssistant["callWS"], StatisticsCache>();

export const fetchStatisticsWithCache = (
  callWS: HomeAssistant["callWS"],
  ...args: StatisticsArguments
): Promise<Statistics> => {
  let cache = caches.get(callWS);
  if (!cache) {
    cache = new StatisticsCache(callWS);
    caches.set(callWS, cache);
  }
  return cache.fetch(...args);
};
