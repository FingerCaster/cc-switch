import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { CalendarDays, ChevronDown, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import {
  buildCustomUsageTimeRange,
  getUsageTimeRangeValue,
  isUsageTimeRangeWithinLimit,
  localDatetimeToTimestamp,
  resolveUsageTimeRangeValue,
  timestampToLocalDatetime,
  USAGE_TIME_RANGE_PRESET_OPTIONS,
  type UsageTimeRangePreset,
  type UsageTimeRangeValue,
} from "./timeRange";

interface UsageTimeRangePickerProps {
  value: UsageTimeRangeValue;
  onApply: (value: UsageTimeRangeValue) => void;
  className?: string;
}

export function UsageTimeRangePicker({
  value,
  onApply,
  className,
}: UsageTimeRangePickerProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [draftPreset, setDraftPreset] =
    useState<UsageTimeRangePreset>(value.preset);
  const [draftStartDate, setDraftStartDate] = useState(value.startDate);
  const [draftEndDate, setDraftEndDate] = useState(value.endDate);
  const [validationError, setValidationError] = useState<string | null>(null);

  const presetLabelMap = useMemo(
    () =>
      new Map(
        USAGE_TIME_RANGE_PRESET_OPTIONS.map((option) => [
          option.preset,
          t(option.labelKey, option.fallback),
        ]),
      ),
    [t],
  );

  const triggerLabel =
    value.preset === "custom"
      ? t("usage.range.custom", "自定义")
      : (presetLabelMap.get(value.preset) ?? t("usage.range.today", "今天"));

  const syncDraftWithValue = () => {
    const resolvedValue = resolveUsageTimeRangeValue(value);
    setDraftPreset(value.preset);
    setDraftStartDate(resolvedValue.startDate);
    setDraftEndDate(resolvedValue.endDate);
    setValidationError(null);
  };

  const handlePresetSelect = (
    preset: Exclude<UsageTimeRangePreset, "custom">,
  ) => {
    const nextRange = getUsageTimeRangeValue(preset);
    setDraftPreset(nextRange.preset);
    setDraftStartDate(nextRange.startDate);
    setDraftEndDate(nextRange.endDate);
    setValidationError(null);
  };

  const handleOpenChange = (nextOpen: boolean) => {
    setOpen(nextOpen);
    if (nextOpen) {
      syncDraftWithValue();
    }
  };

  const handleApply = () => {
    const requiresExplicitDateBounds = draftPreset !== "allTime";

    if (
      requiresExplicitDateBounds &&
      (typeof draftStartDate !== "number" || typeof draftEndDate !== "number")
    ) {
      setValidationError(
        t("usage.invalidTimeRange", "请选择完整的开始/结束时间"),
      );
      return;
    }

    if (requiresExplicitDateBounds && draftStartDate > draftEndDate) {
      setValidationError(
        t("usage.invalidTimeRangeOrder", "开始时间不能晚于结束时间"),
      );
      return;
    }

    const nextValue =
      draftPreset === "custom"
        ? buildCustomUsageTimeRange(draftStartDate, draftEndDate)
        : getUsageTimeRangeValue(draftPreset);

    if (!isUsageTimeRangeWithinLimit(nextValue)) {
      setValidationError(
        t("usage.timeRangeTooLarge", "时间范围过大，请缩小范围"),
      );
      return;
    }

    onApply(nextValue);
    setOpen(false);
  };

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          className={cn(
            "justify-between gap-2 border-emerald-300 bg-background px-3 text-emerald-700 shadow-sm hover:bg-emerald-50 hover:text-emerald-800",
            className,
          )}
        >
          <span className="flex items-center gap-2">
            <CalendarDays className="h-4 w-4" />
            {triggerLabel}
          </span>
          <ChevronDown className="h-4 w-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        className="w-[min(420px,calc(100vw-2rem))] max-w-[calc(100vw-2rem)] overflow-hidden rounded-2xl p-0"
        align="end"
      >
        <div className="space-y-4 p-4">
          <div className="space-y-1">
            <div className="text-sm font-medium">
              {t("usage.timeRange", "时间范围")}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-2 rounded-xl border bg-muted/20 p-2">
            {USAGE_TIME_RANGE_PRESET_OPTIONS.map((option) => {
              const active = draftPreset === option.preset;
              return (
                <button
                  key={option.preset}
                  type="button"
                  className={cn(
                    "rounded-lg px-3 py-2 text-sm transition-colors",
                    active
                      ? "bg-emerald-100 text-emerald-800"
                      : "bg-transparent text-foreground hover:bg-muted",
                  )}
                  onClick={() => handlePresetSelect(option.preset)}
                >
                  {t(option.labelKey, option.fallback)}
                </button>
              );
            })}
          </div>

          {draftPreset === "allTime" ? (
            <div className="rounded-xl border border-dashed bg-muted/10 px-4 py-3 text-sm text-muted-foreground">
              {t("usage.range.allTimeHint", "统计全部历史数据")}
            </div>
          ) : (
            <div className="grid grid-cols-1 items-end gap-3 sm:grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)]">
              <div className="min-w-0 space-y-2">
                <label className="text-sm text-muted-foreground">
                  {t("usage.range.startDate", "开始日期")}
                </label>
                <Input
                  type="datetime-local"
                  className="min-w-0 text-xs sm:text-sm"
                  value={timestampToLocalDatetime(draftStartDate)}
                  onChange={(e) => {
                    const next = localDatetimeToTimestamp(e.target.value);
                    if (typeof next === "number") {
                      setDraftPreset("custom");
                      setDraftStartDate(next);
                    }
                  }}
                />
              </div>
              <div className="flex justify-center pb-0 text-muted-foreground sm:pb-2">
                <ArrowRight className="h-4 w-4" />
              </div>
              <div className="min-w-0 space-y-2">
                <label className="text-sm text-muted-foreground">
                  {t("usage.range.endDate", "结束日期")}
                </label>
                <Input
                  type="datetime-local"
                  className="min-w-0 text-xs sm:text-sm"
                  value={timestampToLocalDatetime(draftEndDate)}
                  onChange={(e) => {
                    const next = localDatetimeToTimestamp(e.target.value);
                    if (typeof next === "number") {
                      setDraftPreset("custom");
                      setDraftEndDate(next);
                    }
                  }}
                />
              </div>
            </div>
          )}

          {validationError ? (
            <div className="text-sm text-red-600">{validationError}</div>
          ) : null}

          <div className="flex justify-end">
            <Button type="button" size="sm" onClick={handleApply}>
              {t("common.apply", "应用")}
            </Button>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
