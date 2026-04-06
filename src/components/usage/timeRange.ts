export type UsageTimeRangePreset =
  | "allTime"
  | "today"
  | "yesterday"
  | "last24Hours"
  | "last7Days"
  | "last14Days"
  | "last30Days"
  | "thisMonth"
  | "lastMonth"
  | "custom";

export interface UsageTimeRangeValue {
  preset: UsageTimeRangePreset;
  startDate: number;
  endDate: number;
}

export interface UsageTimeRangeQueryWindow {
  startDate?: number;
  endDate?: number;
}

export const MAX_USAGE_TIME_RANGE_SECONDS = 30 * 24 * 60 * 60;

export interface UsageTimeRangePresetOption {
  preset: Exclude<UsageTimeRangePreset, "custom">;
  labelKey: string;
  fallback: string;
}

export const USAGE_TIME_RANGE_PRESET_OPTIONS: UsageTimeRangePresetOption[] = [
  { preset: "allTime", labelKey: "usage.range.allTime", fallback: "全部用量" },
  { preset: "today", labelKey: "usage.range.today", fallback: "今天" },
  { preset: "yesterday", labelKey: "usage.range.yesterday", fallback: "昨天" },
  {
    preset: "last24Hours",
    labelKey: "usage.range.last24Hours",
    fallback: "近 24 小时",
  },
  {
    preset: "last7Days",
    labelKey: "usage.range.last7Days",
    fallback: "近 7 天",
  },
  {
    preset: "last14Days",
    labelKey: "usage.range.last14Days",
    fallback: "近 14 天",
  },
  {
    preset: "last30Days",
    labelKey: "usage.range.last30Days",
    fallback: "近 30 天",
  },
  { preset: "thisMonth", labelKey: "usage.range.thisMonth", fallback: "本月" },
  { preset: "lastMonth", labelKey: "usage.range.lastMonth", fallback: "上月" },
];

const toUnix = (date: Date) => Math.floor(date.getTime() / 1000);

const startOfLocalDay = (date: Date) =>
  new Date(
    date.getFullYear(),
    date.getMonth(),
    date.getDate(),
    0,
    0,
    0,
    0,
  );

const startOfNextLocalDay = (date: Date) => {
  const nextDay = new Date(date);
  nextDay.setDate(nextDay.getDate() + 1);
  return startOfLocalDay(nextDay);
};

const endOfLocalDay = (date: Date) =>
  new Date(
    date.getFullYear(),
    date.getMonth(),
    date.getDate(),
    23,
    59,
    59,
    999,
  );

const startOfMonth = (date: Date) =>
  new Date(date.getFullYear(), date.getMonth(), 1, 0, 0, 0, 0);

const endOfMonth = (date: Date) =>
  new Date(date.getFullYear(), date.getMonth() + 1, 0, 23, 59, 59, 999);

export function getUsageTimeRangeValue(
  preset: Exclude<UsageTimeRangePreset, "custom">,
  nowInput?: Date,
): UsageTimeRangeValue {
  const now = nowInput ? new Date(nowInput) : new Date(Date.now());

  switch (preset) {
    case "allTime":
      return {
        preset,
        startDate: 0,
        endDate: 0,
      };
    case "today":
      return {
        preset,
        startDate: toUnix(startOfLocalDay(now)),
        endDate: toUnix(startOfNextLocalDay(now)),
      };
    case "yesterday": {
      const yesterday = new Date(now);
      yesterday.setDate(yesterday.getDate() - 1);
      return {
        preset,
        startDate: toUnix(startOfLocalDay(yesterday)),
        endDate: toUnix(endOfLocalDay(yesterday)),
      };
    }
    case "last24Hours":
      return {
        preset,
        startDate: toUnix(new Date(now.getTime() - 24 * 60 * 60 * 1000)),
        endDate: toUnix(now),
      };
    case "last7Days":
      return {
        preset,
        startDate: toUnix(new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)),
        endDate: toUnix(now),
      };
    case "last14Days":
      return {
        preset,
        startDate: toUnix(new Date(now.getTime() - 14 * 24 * 60 * 60 * 1000)),
        endDate: toUnix(now),
      };
    case "last30Days":
      return {
        preset,
        startDate: toUnix(new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000)),
        endDate: toUnix(now),
      };
    case "thisMonth":
      return {
        preset,
        startDate: toUnix(startOfMonth(now)),
        endDate: toUnix(now),
      };
    case "lastMonth": {
      const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      return {
        preset,
        startDate: toUnix(startOfMonth(previousMonth)),
        endDate: toUnix(endOfMonth(previousMonth)),
      };
    }
  }
}

export function buildCustomUsageTimeRange(
  startDate: number,
  endDate: number,
): UsageTimeRangeValue {
  return {
    preset: "custom",
    startDate,
    endDate,
  };
}

export function resolveUsageTimeRangeValue(
  range: UsageTimeRangeValue,
  nowInput?: Date,
): UsageTimeRangeValue {
  if (range.preset === "custom" || range.preset === "allTime") {
    return range;
  }

  return getUsageTimeRangeValue(range.preset, nowInput);
}

export function getUsageTimeRangeQueryWindow(
  range: UsageTimeRangeValue,
  nowInput?: Date,
): UsageTimeRangeQueryWindow {
  if (range.preset === "allTime") {
    return {};
  }

  const resolvedRange = resolveUsageTimeRangeValue(range, nowInput);
  return {
    startDate: resolvedRange.startDate,
    endDate: resolvedRange.endDate,
  };
}

export function isUsageTimeRangeWithinLimit(range: UsageTimeRangeValue): boolean {
  if (range.preset !== "custom") {
    return true;
  }
  return range.endDate - range.startDate <= MAX_USAGE_TIME_RANGE_SECONDS;
}

export function timestampToLocalDatetime(timestamp: number): string {
  const date = new Date(timestamp * 1000);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

export function localDatetimeToTimestamp(datetime: string): number | undefined {
  if (!datetime || datetime.length < 16) return undefined;
  const timestamp = new Date(datetime).getTime();
  if (Number.isNaN(timestamp)) return undefined;
  return Math.floor(timestamp / 1000);
}
