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
    setDraftPreset(value.preset);
    setDraftStartDate(value.startDate);
    setDraftEndDate(value.endDate);
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
    if (
      typeof draftStartDate !== "number" ||
      typeof draftEndDate !== "number"
    ) {
      setValidationError(
        t("usage.invalidTimeRange", "请选择完整的开始/结束时间"),
      );
      return;
    }

    if (draftStartDate > draftEndDate) {
      setValidationError(
        t("usage.invalidTimeRangeOrder", "开始时间不能晚于结束时间"),
      );
      return;
    }

    const nextValue =
      draftPreset === "custom"
        ? buildCustomUsageTimeRange(draftStartDate, draftEndDate)
        : {
            preset: draftPreset,
            startDate: draftStartDate,
            endDate: draftEndDate,
          };

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
      <PopoverContent className="w-[360px] rounded-2xl p-0" align="end">
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

          <div className="grid grid-cols-[1fr_auto_1fr] items-end gap-3">
            <div className="space-y-2">
              <label className="text-sm text-muted-foreground">
                {t("usage.range.startDate", "开始日期")}
              </label>
              <Input
                type="datetime-local"
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
            <div className="pb-2 text-muted-foreground">
              <ArrowRight className="h-4 w-4" />
            </div>
            <div className="space-y-2">
              <label className="text-sm text-muted-foreground">
                {t("usage.range.endDate", "结束日期")}
              </label>
              <Input
                type="datetime-local"
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
