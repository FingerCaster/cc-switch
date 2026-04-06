import { describe, expect, it } from "vitest";
import {
  buildCustomUsageTimeRange,
  getUsageTimeRangeValue,
  isUsageTimeRangeWithinLimit,
  resolveUsageTimeRangeValue,
} from "@/components/usage/timeRange";

describe("usage time range helpers", () => {
  it("keeps rolling presets moving with the current time", () => {
    const frozenRange = getUsageTimeRangeValue(
      "last24Hours",
      new Date("2026-04-06T08:00:00+08:00"),
    );

    const resolvedLater = resolveUsageTimeRangeValue(
      frozenRange,
      new Date("2026-04-06T12:30:00+08:00"),
    );

    expect(resolvedLater.startDate).toBe(
      Math.floor(new Date("2026-04-05T12:30:00+08:00").getTime() / 1000),
    );
    expect(resolvedLater.endDate).toBe(
      Math.floor(new Date("2026-04-06T12:30:00+08:00").getTime() / 1000),
    );
  });

  it("treats today as the full local calendar day", () => {
    const todayRange = getUsageTimeRangeValue(
      "today",
      new Date("2026-04-06T08:00:00+08:00"),
    );

    expect(todayRange.startDate).toBe(
      Math.floor(new Date("2026-04-06T00:00:00+08:00").getTime() / 1000),
    );
    expect(todayRange.endDate).toBe(
      Math.floor(new Date("2026-04-07T00:00:00+08:00").getTime() / 1000),
    );
  });

  it("supports an all-time preset without date bounds", () => {
    const allTimeRange = getUsageTimeRangeValue("allTime");

    expect(allTimeRange).toEqual({
      preset: "allTime",
      startDate: 0,
      endDate: 0,
    });
  });

  it("keeps custom ranges unchanged when resolving", () => {
    const customRange = buildCustomUsageTimeRange(100, 200);

    expect(
      resolveUsageTimeRangeValue(
        customRange,
        new Date("2026-04-06T12:30:00+08:00"),
      ),
    ).toEqual(customRange);
  });

  it("rejects custom ranges longer than 30 days", () => {
    expect(
      isUsageTimeRangeWithinLimit(
        buildCustomUsageTimeRange(
          Math.floor(new Date("2026-01-01T00:00:00+08:00").getTime() / 1000),
          Math.floor(new Date("2026-04-06T12:00:00+08:00").getTime() / 1000),
        ),
      ),
    ).toBe(false);
  });

  it("allows month presets even when they span 31 days", () => {
    expect(
      isUsageTimeRangeWithinLimit(
        getUsageTimeRangeValue(
          "lastMonth",
          new Date("2026-04-06T12:00:00+08:00"),
        ),
      ),
    ).toBe(true);
  });
});
