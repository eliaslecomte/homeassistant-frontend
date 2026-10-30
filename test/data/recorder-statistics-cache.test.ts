import { assert, describe, expect, it, vi } from "vitest";
import type { HomeAssistant } from "../../src/types";
import type { Statistics } from "../../src/data/recorder";
import { fetchStatisticsWithCache } from "../../src/data/recorder-statistics-cache";

const start = new Date("2024-01-01T00:00:00Z");
const end = new Date("2024-01-01T23:59:59Z");
const stats: Statistics = {
  "sensor.energy": [{ start: start.getTime(), end: end.getTime(), change: 4 }],
};

const request = (
  callWS: (message: Parameters<HomeAssistant["callWS"]>[0]) => Promise<unknown>,
  date = start
) =>
  fetchStatisticsWithCache(
    callWS as HomeAssistant["callWS"],
    date,
    end,
    ["sensor.energy"],
    "hour",
    {
      energy: "kWh",
    },
    ["change"]
  );

describe("conditional statistics caching", () => {
  it("validates revisits and returns independent values to mutating consumers", async () => {
    const callWS = vi
      .fn<HomeAssistant["callWS"]>()
      .mockResolvedValueOnce({
        cache_token: "first",
        not_modified: false,
        statistics: stats,
      })
      .mockResolvedValueOnce({ cache_token: "first", not_modified: true });
    const first = await request(callWS);
    first["sensor.energy"][0].change = 999;
    assert.deepEqual(await request(callWS), stats);
    assert.equal(callWS.mock.calls[1][0].cache_token, "first");
  });

  it("replaces a cached response after source corrections or a reconnect", async () => {
    const corrected = {
      "sensor.energy": [{ ...stats["sensor.energy"][0], change: 7 }],
    };
    const callWS = vi
      .fn<HomeAssistant["callWS"]>()
      .mockResolvedValueOnce({
        cache_token: "old",
        not_modified: false,
        statistics: stats,
      })
      .mockResolvedValueOnce({
        cache_token: "new",
        not_modified: false,
        statistics: corrected,
      })
      .mockResolvedValueOnce({ cache_token: "new", not_modified: true });
    await request(callWS);
    assert.deepEqual(await request(callWS), corrected);
    assert.deepEqual(await request(callWS), corrected);
    assert.equal(callWS.mock.calls[2][0].cache_token, "new");
  });

  it("deduplicates in-flight requests without sharing mutable results", async () => {
    let resolve!: (value: unknown) => void;
    const callWS = vi.fn<HomeAssistant["callWS"]>().mockImplementation(
      () =>
        new Promise((done) => {
          resolve = done;
        })
    );
    const first = request(callWS);
    const second = request(callWS);
    assert.equal(callWS.mock.calls.length, 1);
    resolve({ cache_token: "first", not_modified: false, statistics: stats });
    const [a, b] = await Promise.all([first, second]);
    a["sensor.energy"].push({ start: 0, end: 1 });
    assert.deepEqual(b, stats);
  });

  it("does not cache responses without a snapshot token", async () => {
    const callWS = vi.fn<HomeAssistant["callWS"]>().mockResolvedValue({
      cache_token: null,
      not_modified: false,
      statistics: stats,
    });
    await request(callWS);
    await request(callWS);
    assert.isUndefined(callWS.mock.calls[1][0].cache_token);
  });

  it("falls back on older servers and never hides other errors", async () => {
    const callWS = vi
      .fn<HomeAssistant["callWS"]>()
      .mockRejectedValueOnce({ code: "unknown_command" })
      .mockResolvedValue(stats);
    assert.deepEqual(await request(callWS), stats);
    assert.deepEqual(await request(callWS), stats);
    assert.equal(
      callWS.mock.calls[2][0].type,
      "recorder/statistics_during_period"
    );
    const unavailable = vi
      .fn<HomeAssistant["callWS"]>()
      .mockRejectedValue({ code: "unknown_error" });
    await expect(request(unavailable)).rejects.toEqual({
      code: "unknown_error",
    });
    await expect(request(unavailable)).rejects.toEqual({
      code: "unknown_error",
    });
    assert.equal(unavailable.mock.calls.length, 2);
  });

  it("bounds entries and evicts the least recently used period", async () => {
    const callWS = vi.fn<HomeAssistant["callWS"]>().mockResolvedValue({
      cache_token: "first",
      not_modified: false,
      statistics: stats,
    });
    await Array.from({ length: 25 }).reduce(async (previous, _, day) => {
      await previous;
      await request(callWS, new Date(start.getTime() + day * 86400000));
    }, Promise.resolve());
    await request(callWS);
    assert.isUndefined(
      callWS.mock.calls[callWS.mock.calls.length - 1][0].cache_token
    );
  });

  it("scopes cache entries to the server connection", async () => {
    const first = vi.fn<HomeAssistant["callWS"]>().mockResolvedValue({
      cache_token: "first",
      not_modified: false,
      statistics: stats,
    });
    const second = vi.fn<HomeAssistant["callWS"]>().mockResolvedValue({
      cache_token: "second",
      not_modified: false,
      statistics: stats,
    });
    await request(first);
    await request(second);
    assert.isUndefined(second.mock.calls[0][0].cache_token);
  });

  describe.each([
    { budget: "entries", count: 24, points: 1 },
    { budget: "data points", count: 4, points: 25_000 },
  ])("concurrent eviction under the $budget budget", ({ count, points }) => {
    it.each([true, false])(
      "enforces limits when an evicted request returns not_modified=%s",
      async (notModified) => {
        const statistics: Statistics = {
          "sensor.energy": Array.from({ length: points }, () => ({
            start: 0,
            end: 1,
            change: 4,
          })),
        };
        const fullResponse = {
          cache_token: "first",
          not_modified: false,
          statistics,
        };
        let resolve!: (response: unknown) => void;
        const callWS = vi
          .fn<HomeAssistant["callWS"]>()
          .mockResolvedValue(fullResponse);
        await request(callWS);
        callWS.mockImplementationOnce(
          () =>
            new Promise((done) => {
              resolve = done;
            })
        );
        const delayed = request(callWS);
        await Array.from({ length: count }).reduce(async (previous, _, day) => {
          await previous;
          await request(
            callWS,
            new Date(start.getTime() + (day + 1) * 86400000)
          );
        }, Promise.resolve());
        resolve({ ...fullResponse, not_modified: notModified });
        assert.deepEqual(await delayed, statistics);
        // Restoring A must evict B, regardless of which server response arrived.
        await request(callWS, new Date(start.getTime() + 86400000));
        assert.isUndefined(
          callWS.mock.calls[callWS.mock.calls.length - 1][0].cache_token
        );
      }
    );
  });
});
