import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RequestLogTable } from "@/components/usage/RequestLogTable";
import { server } from "../msw/server";

const TAURI_ENDPOINT = "http://tauri.local";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("RequestLogTable", () => {
  it("renders failed request attempts returned by get_request_logs", async () => {
    let capturedPayload: Record<string, unknown> | undefined;

    server.use(
      http.post(`${TAURI_ENDPOINT}/get_request_logs`, async ({ request }) => {
        capturedPayload = (await request.json()) as Record<string, unknown>;

        return HttpResponse.json({
          data: [
            {
              requestId: "req-failover-1",
              providerId: "provider-a",
              providerName: "Provider A",
              appType: "codex",
              model: "gpt-5",
              requestModel: "gpt-5",
              costMultiplier: "1.0",
              inputTokens: 0,
              outputTokens: 0,
              cacheReadTokens: 0,
              cacheCreationTokens: 0,
              inputCostUsd: "0",
              outputCostUsd: "0",
              cacheReadCostUsd: "0",
              cacheCreationCostUsd: "0",
              totalCostUsd: "0",
              isStreaming: true,
              latencyMs: 1250,
              firstTokenMs: 300,
              durationMs: 1250,
              statusCode: 429,
              errorMessage: "rate limit exceeded",
              createdAt: 1_715_000_000,
            },
          ],
          total: 1,
          page: 0,
          pageSize: 20,
        });
      }),
    );

    const client = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    render(
      <QueryClientProvider client={client}>
        <RequestLogTable refreshIntervalMs={0} />
      </QueryClientProvider>,
    );

    expect(await screen.findByText("Provider A")).toBeInTheDocument();
    expect(screen.getByText("gpt-5")).toBeInTheDocument();
    expect(screen.getByText("429")).toBeInTheDocument();

    await waitFor(() =>
      expect(capturedPayload).toMatchObject({
        page: 0,
        pageSize: 20,
      }),
    );

    const filters = capturedPayload?.filters as Record<string, unknown>;
    expect(typeof filters?.startDate).toBe("number");
    expect(typeof filters?.endDate).toBe("number");
  });

  it(
    "applies the selected preset time range to request log queries",
    async () => {
      const requests: Array<Record<string, unknown>> = [];

      server.use(
        http.post(`${TAURI_ENDPOINT}/get_request_logs`, async ({ request }) => {
          requests.push((await request.json()) as Record<string, unknown>);

          return HttpResponse.json({
            data: [],
            total: 0,
            page: 0,
            pageSize: 20,
          });
        }),
      );

      const client = new QueryClient({
        defaultOptions: {
          queries: {
            retry: false,
          },
        },
      });

      render(
        <QueryClientProvider client={client}>
          <RequestLogTable refreshIntervalMs={0} />
        </QueryClientProvider>,
      );

      await screen.findByText(/usage\.noData|暂无数据/);

      fireEvent.click(screen.getByRole("button", { name: "今天" }));
      fireEvent.click(screen.getByRole("button", { name: "近 7 天" }));
      fireEvent.click(screen.getByRole("button", { name: "应用" }));

      await waitFor(() => expect(requests.length).toBeGreaterThan(1));

      const firstRequest = requests[0];
      const lastRequest = requests.at(-1);
      const firstFilters = firstRequest?.filters as Record<string, number>;
      const filters = lastRequest?.filters as Record<string, number>;
      expect(filters.startDate).toBeLessThan(firstFilters.startDate);
      expect(firstFilters.startDate - filters.startDate).toBeGreaterThanOrEqual(
        5 * 24 * 60 * 60,
      );
      expect(typeof filters.endDate).toBe("number");
    },
    10000,
  );

  it("refreshes rolling presets using the latest current time", async () => {
    let currentTime = new Date("2026-04-06T08:00:00+08:00").getTime();
    vi.spyOn(Date, "now").mockImplementation(() => currentTime);

    const requests: Array<Record<string, unknown>> = [];

    server.use(
      http.post(`${TAURI_ENDPOINT}/get_request_logs`, async ({ request }) => {
        requests.push((await request.json()) as Record<string, unknown>);

        return HttpResponse.json({
          data: [],
          total: 0,
          page: 0,
          pageSize: 20,
        });
      }),
    );

    const client = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    render(
      <QueryClientProvider client={client}>
        <RequestLogTable refreshIntervalMs={0} />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(requests.length).toBe(1));

    fireEvent.click(screen.getByRole("button", { name: "今天" }));
    fireEvent.click(screen.getByRole("button", { name: "近 24 小时" }));
    fireEvent.click(screen.getByRole("button", { name: "应用" }));

    await waitFor(() => expect(requests.length).toBeGreaterThan(1));

    const rollingFilters = requests.at(-1)?.filters as Record<string, number>;
    currentTime = new Date("2026-04-06T10:00:00+08:00").getTime();

    await client.invalidateQueries({ queryKey: ["usage", "logs"] });

    await waitFor(() => expect(requests.length).toBeGreaterThan(1));

    const latestFilters = requests.at(-1)?.filters as Record<string, number>;

    expect(latestFilters.startDate).toBeGreaterThan(rollingFilters.startDate);
    expect(latestFilters.endDate).toBeGreaterThan(rollingFilters.endDate);
    expect(
      latestFilters.endDate - rollingFilters.endDate,
    ).toBeGreaterThanOrEqual(
      2 * 60 * 60,
    );
  }, 10000);

  it("keeps today pinned to the full calendar day while refreshes continue", async () => {
    let currentTime = new Date("2026-04-06T08:00:00+08:00").getTime();
    vi.spyOn(Date, "now").mockImplementation(() => currentTime);

    const requests: Array<Record<string, unknown>> = [];

    server.use(
      http.post(`${TAURI_ENDPOINT}/get_request_logs`, async ({ request }) => {
        requests.push((await request.json()) as Record<string, unknown>);

        return HttpResponse.json({
          data: [],
          total: 0,
          page: 0,
          pageSize: 20,
        });
      }),
    );

    const client = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    render(
      <QueryClientProvider client={client}>
        <RequestLogTable refreshIntervalMs={0} />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(requests.length).toBe(1));

    const firstFilters = requests[0]?.filters as Record<string, number>;
    expect(firstFilters.startDate).toBe(
      Math.floor(new Date("2026-04-06T00:00:00+08:00").getTime() / 1000),
    );
    expect(firstFilters.endDate).toBe(
      Math.floor(new Date("2026-04-07T00:00:00+08:00").getTime() / 1000),
    );

    currentTime = new Date("2026-04-06T10:00:00+08:00").getTime();

    await client.invalidateQueries({ queryKey: ["usage", "logs"] });

    await waitFor(() => expect(requests.length).toBeGreaterThan(1));

    const latestFilters = requests.at(-1)?.filters as Record<string, number>;

    expect(latestFilters.startDate).toBe(firstFilters.startDate);
    expect(latestFilters.endDate).toBe(firstFilters.endDate);
  }, 10000);

  it("queries all history when the all-time preset is selected", async () => {
    const requests: Array<Record<string, unknown>> = [];

    server.use(
      http.post(`${TAURI_ENDPOINT}/get_request_logs`, async ({ request }) => {
        requests.push((await request.json()) as Record<string, unknown>);

        return HttpResponse.json({
          data: [],
          total: 0,
          page: 0,
          pageSize: 20,
        });
      }),
    );

    const client = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });

    render(
      <QueryClientProvider client={client}>
        <RequestLogTable refreshIntervalMs={0} />
      </QueryClientProvider>,
    );

    await waitFor(() => expect(requests.length).toBe(1));

    fireEvent.click(screen.getByRole("button", { name: "今天" }));
    fireEvent.click(screen.getByRole("button", { name: "全部用量" }));
    fireEvent.click(screen.getByRole("button", { name: "应用" }));

    await waitFor(() => expect(requests.length).toBeGreaterThan(1));

    const latestFilters = requests.at(-1)?.filters as Record<string, unknown>;

    expect(latestFilters.startDate).toBeUndefined();
    expect(latestFilters.endDate).toBeUndefined();
  }, 10000);
});
