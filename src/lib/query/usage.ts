import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { usageApi } from "@/lib/api/usage";
import type { LogFilters } from "@/types/usage";
import {
  getUsageTimeRangeQueryWindow,
  type UsageTimeRangeValue,
} from "@/components/usage/timeRange";

const DEFAULT_REFETCH_INTERVAL_MS = 30000;

type UsageQueryOptions = {
  refetchInterval?: number | false;
  refetchIntervalInBackground?: boolean;
};

type RequestLogsQueryArgs = {
  filters: LogFilters;
  range: UsageTimeRangeValue;
  page?: number;
  pageSize?: number;
  options?: UsageQueryOptions;
};

type RequestLogsKey = {
  preset: UsageTimeRangeValue["preset"];
  rangeStartDate: number;
  rangeEndDate: number;
  appType?: string;
  providerName?: string;
  model?: string;
  statusCode?: number;
};

// Query keys
export const usageKeys = {
  all: ["usage"] as const,
  summary: (startDate: number, endDate: number) =>
    [...usageKeys.all, "summary", startDate, endDate] as const,
  trends: (startDate: number, endDate: number) =>
    [...usageKeys.all, "trends", startDate, endDate] as const,
  providerStats: () => [...usageKeys.all, "provider-stats"] as const,
  modelStats: () => [...usageKeys.all, "model-stats"] as const,
  logs: (key: RequestLogsKey, page: number, pageSize: number) =>
    [
      ...usageKeys.all,
      "logs",
      key.preset,
      key.rangeStartDate,
      key.rangeEndDate,
      key.appType ?? "",
      key.providerName ?? "",
      key.model ?? "",
      key.statusCode ?? -1,
      page,
      pageSize,
    ] as const,
  detail: (requestId: string) =>
    [...usageKeys.all, "detail", requestId] as const,
  pricing: () => [...usageKeys.all, "pricing"] as const,
  limits: (providerId: string, appType: string) =>
    [...usageKeys.all, "limits", providerId, appType] as const,
};

// Hooks
export function useUsageSummary(
  range: UsageTimeRangeValue,
  options?: UsageQueryOptions,
) {
  return useQuery({
    queryKey: usageKeys.summary(range.startDate, range.endDate),
    queryFn: () => {
      const resolvedRange = getUsageTimeRangeQueryWindow(range);
      return usageApi.getUsageSummary(
        resolvedRange.startDate,
        resolvedRange.endDate,
      );
    },
    refetchInterval: options?.refetchInterval ?? DEFAULT_REFETCH_INTERVAL_MS, // 每30秒自动刷新
    refetchIntervalInBackground: options?.refetchIntervalInBackground ?? false, // 后台不刷新
  });
}

export function useUsageTrends(
  range: UsageTimeRangeValue,
  options?: UsageQueryOptions,
) {
  return useQuery({
    queryKey: usageKeys.trends(range.startDate, range.endDate),
    queryFn: () => {
      const resolvedRange = getUsageTimeRangeQueryWindow(range);
      return usageApi.getUsageTrends(
        resolvedRange.startDate,
        resolvedRange.endDate,
      );
    },
    refetchInterval: options?.refetchInterval ?? DEFAULT_REFETCH_INTERVAL_MS, // 每30秒自动刷新
    refetchIntervalInBackground: options?.refetchIntervalInBackground ?? false,
  });
}

export function useProviderStats(options?: UsageQueryOptions) {
  return useQuery({
    queryKey: usageKeys.providerStats(),
    queryFn: usageApi.getProviderStats,
    refetchInterval: options?.refetchInterval ?? DEFAULT_REFETCH_INTERVAL_MS, // 每30秒自动刷新
    refetchIntervalInBackground: options?.refetchIntervalInBackground ?? false,
  });
}

export function useModelStats(options?: UsageQueryOptions) {
  return useQuery({
    queryKey: usageKeys.modelStats(),
    queryFn: usageApi.getModelStats,
    refetchInterval: options?.refetchInterval ?? DEFAULT_REFETCH_INTERVAL_MS, // 每30秒自动刷新
    refetchIntervalInBackground: options?.refetchIntervalInBackground ?? false,
  });
}

export function useRequestLogs({
  filters,
  range,
  page = 0,
  pageSize = 20,
  options,
}: RequestLogsQueryArgs) {
  const key: RequestLogsKey = {
    preset: range.preset,
    rangeStartDate: range.startDate,
    rangeEndDate: range.endDate,
    appType: filters.appType,
    providerName: filters.providerName,
    model: filters.model,
    statusCode: filters.statusCode,
  };

  return useQuery({
    queryKey: usageKeys.logs(key, page, pageSize),
    queryFn: () => {
      const resolvedRange = getUsageTimeRangeQueryWindow(range);
      return usageApi.getRequestLogs(
        {
          ...filters,
          startDate: resolvedRange.startDate,
          endDate: resolvedRange.endDate,
        },
        page,
        pageSize,
      );
    },
    refetchInterval: options?.refetchInterval ?? DEFAULT_REFETCH_INTERVAL_MS, // 每30秒自动刷新
    refetchIntervalInBackground: options?.refetchIntervalInBackground ?? false,
  });
}

export function useRequestDetail(requestId: string) {
  return useQuery({
    queryKey: usageKeys.detail(requestId),
    queryFn: () => usageApi.getRequestDetail(requestId),
    enabled: !!requestId,
  });
}

export function useModelPricing() {
  return useQuery({
    queryKey: usageKeys.pricing(),
    queryFn: usageApi.getModelPricing,
  });
}

export function useProviderLimits(providerId: string, appType: string) {
  return useQuery({
    queryKey: usageKeys.limits(providerId, appType),
    queryFn: () => usageApi.checkProviderLimits(providerId, appType),
    enabled: !!providerId && !!appType,
  });
}

export function useUpdateModelPricing() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (params: {
      modelId: string;
      displayName: string;
      inputCost: string;
      outputCost: string;
      cacheReadCost: string;
      cacheCreationCost: string;
    }) =>
      usageApi.updateModelPricing(
        params.modelId,
        params.displayName,
        params.inputCost,
        params.outputCost,
        params.cacheReadCost,
        params.cacheCreationCost,
      ),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: usageKeys.pricing() });
    },
  });
}

export function useDeleteModelPricing() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (modelId: string) => usageApi.deleteModelPricing(modelId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: usageKeys.pricing() });
    },
  });
}
