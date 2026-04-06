import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { UsageDashboard } from "@/components/usage/UsageDashboard";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    t: (key: string, fallback?: string) => {
      const mapping: Record<string, string> = {
        "usage.title": "使用统计",
        "usage.subtitle": "统计说明",
        "usage.requestLogs": "请求日志",
        "usage.providerStats": "Provider 统计",
        "usage.modelStats": "模型统计",
      };
      return fallback ?? mapping[key] ?? key;
    },
  }),
}));

vi.mock("@/components/usage/UsageSummaryCards", () => ({
  UsageSummaryCards: ({ range }: { range: { preset: string } }) => (
    <div data-testid="summary-range">{range.preset}</div>
  ),
}));

vi.mock("@/components/usage/UsageTrendChart", () => ({
  UsageTrendChart: ({ range }: { range: { preset: string } }) => (
    <div data-testid="trend-range">{range.preset}</div>
  ),
}));

vi.mock("@/components/usage/RequestLogTable", () => ({
  RequestLogTable: () => <div data-testid="request-log-table" />,
}));

vi.mock("@/components/usage/ProviderStatsTable", () => ({
  ProviderStatsTable: () => <div data-testid="provider-stats-table" />,
}));

vi.mock("@/components/usage/ModelStatsTable", () => ({
  ModelStatsTable: () => <div data-testid="model-stats-table" />,
}));

vi.mock("@/components/usage/PricingConfigPanel", () => ({
  PricingConfigPanel: () => <div data-testid="pricing-panel" />,
}));

describe("UsageDashboard", () => {
  it("uses the new time range picker with today selected by default", () => {
    const client = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    render(
      <QueryClientProvider client={client}>
        <UsageDashboard />
      </QueryClientProvider>,
    );

    expect(screen.getByRole("button", { name: "今天" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "今天" }));
    expect(screen.getByRole("button", { name: "近 24 小时" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "近 7 天" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "本月" })).toBeInTheDocument();
    expect(screen.getByTestId("summary-range")).toHaveTextContent("today");
    expect(screen.getByTestId("trend-range")).toHaveTextContent("today");
  });
});
