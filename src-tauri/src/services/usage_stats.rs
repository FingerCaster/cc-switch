//! 使用统计服务
//!
//! 提供使用量数据的聚合查询功能

use crate::database::{lock_conn, Database};
use crate::error::AppError;
use chrono::{Local, TimeZone};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;

/// 使用量汇总
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSummary {
    pub total_requests: u64,
    pub total_cost: String,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cache_creation_tokens: u64,
    pub total_cache_read_tokens: u64,
    pub success_rate: f32,
}

/// 每日统计
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DailyStats {
    pub date: String,
    pub request_count: u64,
    pub total_cost: String,
    pub total_tokens: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cache_creation_tokens: u64,
    pub total_cache_read_tokens: u64,
}

/// Provider 统计
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderStats {
    pub provider_id: String,
    pub provider_name: String,
    pub request_count: u64,
    pub total_tokens: u64,
    pub total_cost: String,
    pub success_rate: f32,
    pub avg_latency_ms: u64,
}

/// 模型统计
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelStats {
    pub model: String,
    pub request_count: u64,
    pub total_tokens: u64,
    pub total_cost: String,
    pub avg_cost_per_request: String,
}

/// 请求日志过滤器
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFilters {
    pub app_type: Option<String>,
    pub provider_name: Option<String>,
    pub model: Option<String>,
    pub source_group: Option<String>,
    pub status_code: Option<u16>,
    pub start_date: Option<i64>,
    pub end_date: Option<i64>,
}

/// 分页请求日志响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedLogs {
    pub data: Vec<RequestLogDetail>,
    pub total: u32,
    pub page: u32,
    pub page_size: u32,
}

/// 请求日志详情
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestLogDetail {
    pub request_id: String,
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_name: Option<String>,
    pub app_type: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_model: Option<String>,
    pub cost_multiplier: String,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_tokens: u32,
    pub cache_creation_tokens: u32,
    pub input_cost_usd: String,
    pub output_cost_usd: String,
    pub cache_read_cost_usd: String,
    pub cache_creation_cost_usd: String,
    pub total_cost_usd: String,
    pub is_streaming: bool,
    pub latency_ms: u64,
    pub first_token_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub status_code: u16,
    pub error_message: Option<String>,
    pub created_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source: Option<String>,
}

/// SQL fragment: resolve provider_name with fallback for session-based entries.
/// Session logs use placeholder provider_ids (_session, _codex_session, _gemini_session)
/// that don't exist in the providers table — this COALESCE gives them readable names.
fn provider_name_coalesce(log_alias: &str, provider_alias: &str) -> String {
    format!(
        "COALESCE({provider_alias}.name, CASE {log_alias}.provider_id \
         WHEN '_session' THEN 'Claude (Session)' \
         WHEN '_codex_session' THEN 'Codex (Session)' \
         WHEN '_gemini_session' THEN 'Gemini (Session)' \
         ELSE {log_alias}.provider_id END)"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestLogSourceKind {
    Proxy,
    Session,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestLogSourceMeta {
    kind: RequestLogSourceKind,
    apps: &'static [&'static str],
}

const REQUEST_LOG_SOURCE_META_PROXY: RequestLogSourceMeta = RequestLogSourceMeta {
    kind: RequestLogSourceKind::Proxy,
    apps: &[],
};

const REQUEST_LOG_SOURCE_META_OTHER: RequestLogSourceMeta = RequestLogSourceMeta {
    kind: RequestLogSourceKind::Other,
    apps: &[],
};

const REQUEST_LOG_SOURCE_REGISTRY: &[(&str, RequestLogSourceMeta)] = &[
    (
        "session_log",
        RequestLogSourceMeta {
            kind: RequestLogSourceKind::Session,
            apps: &["claude"],
        },
    ),
    (
        "codex_session",
        RequestLogSourceMeta {
            kind: RequestLogSourceKind::Session,
            apps: &["codex"],
        },
    ),
    (
        "gemini_session",
        RequestLogSourceMeta {
            kind: RequestLogSourceKind::Session,
            apps: &["gemini"],
        },
    ),
    (
        "codex_db",
        RequestLogSourceMeta {
            kind: RequestLogSourceKind::Other,
            apps: &["codex"],
        },
    ),
];

fn classify_request_log_source(data_source: Option<&str>) -> RequestLogSourceMeta {
    match data_source {
        None | Some("") | Some("proxy") => REQUEST_LOG_SOURCE_META_PROXY,
        Some(source) => REQUEST_LOG_SOURCE_REGISTRY
            .iter()
            .find_map(|(registered_source, meta)| {
                if *registered_source == source {
                    Some(*meta)
                } else {
                    None
                }
            })
            .unwrap_or(REQUEST_LOG_SOURCE_META_OTHER),
    }
}

fn normalized_app_type_filter(app_type: Option<&str>) -> Option<&str> {
    app_type.filter(|app| !app.is_empty() && *app != "all")
}

fn matches_source_app(meta: &RequestLogSourceMeta, app_type: Option<&str>) -> bool {
    match normalized_app_type_filter(app_type) {
        Some(app) => meta.apps.is_empty() || meta.apps.contains(&app),
        None => true,
    }
}

fn request_log_source_values(
    kind: RequestLogSourceKind,
    app_type: Option<&str>,
) -> Vec<&'static str> {
    REQUEST_LOG_SOURCE_REGISTRY
        .iter()
        .filter_map(|(source, _)| {
            let meta = classify_request_log_source(Some(source));
            if meta.kind == kind && matches_source_app(&meta, app_type) {
                Some(*source)
            } else {
                None
            }
        })
        .collect()
}

fn sql_quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn request_log_source_group_predicate(
    alias: &str,
    source_group: Option<&str>,
    app_type: Option<&str>,
) -> Option<String> {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    let data_source_expr = format!("COALESCE({prefix}data_source, 'proxy')");

    match source_group.filter(|group| !group.is_empty() && *group != "all") {
        Some("proxy") => match classify_request_log_source(Some("proxy")).kind {
            RequestLogSourceKind::Proxy => Some(format!("{data_source_expr} = 'proxy'")),
            RequestLogSourceKind::Session | RequestLogSourceKind::Other => {
                Some("1 = 0".to_string())
            }
        },
        Some("session") => {
            let sources = request_log_source_values(RequestLogSourceKind::Session, app_type);
            if sources.is_empty() {
                Some("1 = 0".to_string())
            } else {
                let values = sources
                    .iter()
                    .map(|source| sql_quote_string(source))
                    .collect::<Vec<_>>()
                    .join(", ");
                Some(format!("{data_source_expr} IN ({values})"))
            }
        }
        _ => None,
    }
}

fn proxy_detail_scope_predicate(alias: &str) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    format!(
        "COALESCE({prefix}data_source, 'proxy') = 'proxy' \
         AND {prefix}provider_id NOT IN ('_session', '_codex_session', '_gemini_session')"
    )
}

fn proxy_rollup_scope_predicate(alias: &str) -> String {
    let prefix = if alias.is_empty() {
        String::new()
    } else {
        format!("{alias}.")
    };
    format!("{prefix}provider_id NOT IN ('_session', '_codex_session', '_gemini_session')")
}

impl Database {
    fn backfill_missing_proxy_costs(
        conn: &Connection,
        app_type: Option<&str>,
        start_date: Option<i64>,
        end_date: Option<i64>,
        provider_id: Option<&str>,
    ) -> Result<u64, AppError> {
        let mut conditions = vec![proxy_detail_scope_predicate("")];
        conditions.push("CAST(COALESCE(NULLIF(total_cost_usd, ''), '0') AS REAL) = 0".to_string());
        conditions.push(
            "(input_tokens > 0 OR output_tokens > 0 OR cache_read_tokens > 0 OR cache_creation_tokens > 0)"
                .to_string(),
        );

        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(at) = app_type {
            conditions.push("app_type = ?".to_string());
            params.push(Box::new(at.to_string()));
        }
        if let Some(start) = start_date {
            conditions.push("created_at >= ?".to_string());
            params.push(Box::new(start));
        }
        if let Some(end) = end_date {
            conditions.push("created_at <= ?".to_string());
            params.push(Box::new(end));
        }
        if let Some(pid) = provider_id {
            conditions.push("provider_id = ?".to_string());
            params.push(Box::new(pid.to_string()));
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let sql = format!(
            "SELECT request_id, provider_id, app_type, model, request_model,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    total_cost_usd, cost_multiplier, created_at
             FROM proxy_request_logs
             {where_clause}"
        );

        let logs = {
            let mut stmt = conn.prepare(&sql)?;
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                Ok(RequestLogDetail {
                    request_id: row.get(0)?,
                    provider_id: row.get(1)?,
                    provider_name: None,
                    app_type: row.get(2)?,
                    model: row.get(3)?,
                    request_model: row.get(4)?,
                    cost_multiplier: row
                        .get::<_, Option<String>>(10)?
                        .unwrap_or_else(|| "1".to_string()),
                    input_tokens: row.get::<_, i64>(5)? as u32,
                    output_tokens: row.get::<_, i64>(6)? as u32,
                    cache_read_tokens: row.get::<_, i64>(7)? as u32,
                    cache_creation_tokens: row.get::<_, i64>(8)? as u32,
                    input_cost_usd: "0".to_string(),
                    output_cost_usd: "0".to_string(),
                    cache_read_cost_usd: "0".to_string(),
                    cache_creation_cost_usd: "0".to_string(),
                    total_cost_usd: row.get::<_, Option<String>>(9)?.unwrap_or_default(),
                    is_streaming: false,
                    latency_ms: 0,
                    first_token_ms: None,
                    duration_ms: None,
                    status_code: 200,
                    error_message: None,
                    created_at: row.get(11)?,
                    data_source: Some("proxy".to_string()),
                })
            })?;

            let mut logs = Vec::new();
            for row in rows {
                logs.push(row?);
            }
            logs
        };

        let mut provider_cache = HashMap::new();
        let mut pricing_cache = HashMap::new();
        let mut updated = 0u64;

        for mut log in logs {
            let original_cost = log.total_cost_usd.clone();
            Self::maybe_backfill_log_costs(
                conn,
                &mut log,
                &mut provider_cache,
                &mut pricing_cache,
            )?;
            if log.total_cost_usd != original_cost {
                updated += 1;
            }
        }

        Ok(updated)
    }

    /// 获取使用量汇总
    pub fn get_usage_summary(
        &self,
        start_date: Option<i64>,
        end_date: Option<i64>,
        app_type: Option<&str>,
    ) -> Result<UsageSummary, AppError> {
        let conn = lock_conn!(self.conn);
        let _ = Self::backfill_missing_proxy_costs(&conn, app_type, start_date, end_date, None)?;

        // Build detail WHERE clause
        let mut conditions = vec![proxy_detail_scope_predicate("")];
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(start) = start_date {
            conditions.push("created_at >= ?".to_string());
            params_vec.push(Box::new(start));
        }
        if let Some(end) = end_date {
            conditions.push("created_at <= ?".to_string());
            params_vec.push(Box::new(end));
        }
        if let Some(at) = app_type {
            conditions.push("app_type = ?".to_string());
            params_vec.push(Box::new(at.to_string()));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // Build rollup WHERE clause using date strings
        let mut rollup_conditions: Vec<String> = vec![proxy_rollup_scope_predicate("")];
        let mut rollup_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(start) = start_date {
            rollup_conditions.push("date >= date(?, 'unixepoch', 'localtime')".to_string());
            rollup_params.push(Box::new(start));
        }
        if let Some(end) = end_date {
            rollup_conditions.push("date <= date(?, 'unixepoch', 'localtime')".to_string());
            rollup_params.push(Box::new(end));
        }
        if let Some(at) = app_type {
            rollup_conditions.push("app_type = ?".to_string());
            rollup_params.push(Box::new(at.to_string()));
        }

        let rollup_where = if rollup_conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", rollup_conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT
                COALESCE(d.total_requests, 0) + COALESCE(r.total_requests, 0),
                COALESCE(d.total_cost, 0) + COALESCE(r.total_cost, 0),
                COALESCE(d.total_input_tokens, 0) + COALESCE(r.total_input_tokens, 0),
                COALESCE(d.total_output_tokens, 0) + COALESCE(r.total_output_tokens, 0),
                COALESCE(d.total_cache_creation_tokens, 0) + COALESCE(r.total_cache_creation_tokens, 0),
                COALESCE(d.total_cache_read_tokens, 0) + COALESCE(r.total_cache_read_tokens, 0),
                COALESCE(d.success_count, 0) + COALESCE(r.success_count, 0)
            FROM
                (SELECT
                    COUNT(*) as total_requests,
                    COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0) as total_cost,
                    COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                    COALESCE(SUM(output_tokens), 0) as total_output_tokens,
                    COALESCE(SUM(cache_creation_tokens), 0) as total_cache_creation_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) as total_cache_read_tokens,
                    COALESCE(SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END), 0) as success_count
                 FROM proxy_request_logs {where_clause}) d,
                (SELECT
                    COALESCE(SUM(request_count), 0) as total_requests,
                    COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0) as total_cost,
                    COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                    COALESCE(SUM(output_tokens), 0) as total_output_tokens,
                    COALESCE(SUM(cache_creation_tokens), 0) as total_cache_creation_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) as total_cache_read_tokens,
                    COALESCE(SUM(success_count), 0) as success_count
                 FROM usage_daily_rollups {rollup_where}) r"
        );

        // Combine params: detail params first, then rollup params
        let mut all_params: Vec<Box<dyn rusqlite::ToSql>> = params_vec;
        all_params.extend(rollup_params);
        let param_refs: Vec<&dyn rusqlite::ToSql> = all_params.iter().map(|p| p.as_ref()).collect();

        let result = conn.query_row(&sql, param_refs.as_slice(), |row| {
            let total_requests: i64 = row.get(0)?;
            let total_cost: f64 = row.get(1)?;
            let total_input_tokens: i64 = row.get(2)?;
            let total_output_tokens: i64 = row.get(3)?;
            let total_cache_creation_tokens: i64 = row.get(4)?;
            let total_cache_read_tokens: i64 = row.get(5)?;
            let success_count: i64 = row.get(6)?;

            let success_rate = if total_requests > 0 {
                (success_count as f32 / total_requests as f32) * 100.0
            } else {
                0.0
            };

            Ok(UsageSummary {
                total_requests: total_requests as u64,
                total_cost: format!("{total_cost:.6}"),
                total_input_tokens: total_input_tokens as u64,
                total_output_tokens: total_output_tokens as u64,
                total_cache_creation_tokens: total_cache_creation_tokens as u64,
                total_cache_read_tokens: total_cache_read_tokens as u64,
                success_rate,
            })
        })?;

        Ok(result)
    }

    /// 获取每日趋势（滑动窗口，<=24h 按小时，>24h 按天，窗口与汇总一致）
    pub fn get_daily_trends(
        &self,
        start_date: Option<i64>,
        end_date: Option<i64>,
        app_type: Option<&str>,
    ) -> Result<Vec<DailyStats>, AppError> {
        let conn = lock_conn!(self.conn);
        let _ = Self::backfill_missing_proxy_costs(&conn, app_type, start_date, end_date, None)?;

        let end_ts = end_date.unwrap_or_else(|| Local::now().timestamp());
        let mut start_ts = start_date.unwrap_or_else(|| end_ts - 24 * 60 * 60);

        if start_ts >= end_ts {
            start_ts = end_ts - 24 * 60 * 60;
        }

        let duration = end_ts - start_ts;
        let bucket_seconds: i64 = if duration <= 24 * 60 * 60 {
            60 * 60
        } else {
            24 * 60 * 60
        };
        let mut bucket_count: i64 = if duration <= 0 {
            1
        } else {
            ((duration as f64) / bucket_seconds as f64).ceil() as i64
        };

        // 固定 24 小时窗口为 24 个小时桶，避免浮点误差
        if bucket_seconds == 60 * 60 {
            bucket_count = 24;
        }

        if bucket_count < 1 {
            bucket_count = 1;
        }

        let app_type_filter = if app_type.is_some() {
            "AND app_type = ?4"
        } else {
            ""
        };
        let detail_scope_filter = proxy_detail_scope_predicate("");

        // Query detail logs
        let sql = format!(
            "SELECT
                CAST((created_at - ?1) / ?3 AS INTEGER) as bucket_idx,
                COUNT(*) as request_count,
                COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0) as total_cost,
                COALESCE(SUM(input_tokens + output_tokens), 0) as total_tokens,
                    COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                    COALESCE(SUM(output_tokens), 0) as total_output_tokens,
                    COALESCE(SUM(cache_creation_tokens), 0) as total_cache_creation_tokens,
                    COALESCE(SUM(cache_read_tokens), 0) as total_cache_read_tokens
            FROM proxy_request_logs
            WHERE created_at >= ?1 AND created_at <= ?2 {app_type_filter} AND {detail_scope_filter}
            GROUP BY bucket_idx
            ORDER BY bucket_idx ASC"
        );

        let mut stmt = conn.prepare(&sql)?;
        let row_mapper = |row: &rusqlite::Row| {
            Ok((
                row.get::<_, i64>(0)?,
                DailyStats {
                    date: String::new(),
                    request_count: row.get::<_, i64>(1)? as u64,
                    total_cost: format!("{:.6}", row.get::<_, f64>(2)?),
                    total_tokens: row.get::<_, i64>(3)? as u64,
                    total_input_tokens: row.get::<_, i64>(4)? as u64,
                    total_output_tokens: row.get::<_, i64>(5)? as u64,
                    total_cache_creation_tokens: row.get::<_, i64>(6)? as u64,
                    total_cache_read_tokens: row.get::<_, i64>(7)? as u64,
                },
            ))
        };

        let mut map: HashMap<i64, DailyStats> = HashMap::new();

        // Collect rows into map (need to handle both param variants)
        {
            let rows = if let Some(at) = app_type {
                stmt.query_map(params![start_ts, end_ts, bucket_seconds, at], row_mapper)?
            } else {
                stmt.query_map(params![start_ts, end_ts, bucket_seconds], row_mapper)?
            };
            for row in rows {
                let (mut bucket_idx, stat) = row?;
                if bucket_idx < 0 {
                    continue;
                }
                if bucket_idx >= bucket_count {
                    bucket_idx = bucket_count - 1;
                }
                map.insert(bucket_idx, stat);
            }
        }

        // Also query rollup data (daily granularity, only useful for daily buckets)
        if bucket_seconds >= 86400 {
            let rollup_scope_filter = proxy_rollup_scope_predicate("");
            let rollup_sql = format!(
                "SELECT
                    CAST((CAST(strftime('%s', date) AS INTEGER) - ?1) / ?3 AS INTEGER) as bucket_idx,
                    COALESCE(SUM(request_count), 0),
                    COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0),
                    COALESCE(SUM(input_tokens + output_tokens), 0),
                    COALESCE(SUM(input_tokens), 0),
                    COALESCE(SUM(output_tokens), 0),
                    COALESCE(SUM(cache_creation_tokens), 0),
                    COALESCE(SUM(cache_read_tokens), 0)
                FROM usage_daily_rollups
                WHERE date >= date(?1, 'unixepoch', 'localtime') AND date <= date(?2, 'unixepoch', 'localtime') {app_type_filter} AND {rollup_scope_filter}
                GROUP BY bucket_idx
                ORDER BY bucket_idx ASC"
            );

            let rollup_row_mapper = |row: &rusqlite::Row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    (
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, f64>(2)?,
                        row.get::<_, i64>(3)? as u64,
                        row.get::<_, i64>(4)? as u64,
                        row.get::<_, i64>(5)? as u64,
                        row.get::<_, i64>(6)? as u64,
                        row.get::<_, i64>(7)? as u64,
                    ),
                ))
            };

            let mut rstmt = conn.prepare(&rollup_sql)?;
            let rrows = if let Some(at) = app_type {
                rstmt.query_map(
                    params![start_ts, end_ts, bucket_seconds, at],
                    rollup_row_mapper,
                )?
            } else {
                rstmt.query_map(params![start_ts, end_ts, bucket_seconds], rollup_row_mapper)?
            };

            for row in rrows {
                let (mut bucket_idx, (req, cost, tok, inp, out, cc, cr)) = row?;
                if bucket_idx < 0 {
                    continue;
                }
                if bucket_idx >= bucket_count {
                    bucket_idx = bucket_count - 1;
                }
                let entry = map.entry(bucket_idx).or_insert_with(|| DailyStats {
                    date: String::new(),
                    request_count: 0,
                    total_cost: "0.000000".to_string(),
                    total_tokens: 0,
                    total_input_tokens: 0,
                    total_output_tokens: 0,
                    total_cache_creation_tokens: 0,
                    total_cache_read_tokens: 0,
                });
                entry.request_count += req;
                let existing_cost: f64 = entry.total_cost.parse().unwrap_or(0.0);
                entry.total_cost = format!("{:.6}", existing_cost + cost);
                entry.total_tokens += tok;
                entry.total_input_tokens += inp;
                entry.total_output_tokens += out;
                entry.total_cache_creation_tokens += cc;
                entry.total_cache_read_tokens += cr;
            }
        }

        let mut stats = Vec::with_capacity(bucket_count as usize);
        for i in 0..bucket_count {
            let bucket_start_ts = start_ts + i * bucket_seconds;
            let bucket_start = Local
                .timestamp_opt(bucket_start_ts, 0)
                .single()
                .unwrap_or_else(Local::now);

            let date = bucket_start.to_rfc3339();

            if let Some(mut stat) = map.remove(&i) {
                stat.date = date;
                stats.push(stat);
            } else {
                stats.push(DailyStats {
                    date,
                    request_count: 0,
                    total_cost: "0.000000".to_string(),
                    total_tokens: 0,
                    total_input_tokens: 0,
                    total_output_tokens: 0,
                    total_cache_creation_tokens: 0,
                    total_cache_read_tokens: 0,
                });
            }
        }

        Ok(stats)
    }

    /// 获取 Provider 统计
    pub fn get_provider_stats(
        &self,
        app_type: Option<&str>,
    ) -> Result<Vec<ProviderStats>, AppError> {
        let conn = lock_conn!(self.conn);
        let _ = Self::backfill_missing_proxy_costs(&conn, app_type, None, None, None)?;

        let (detail_where, rollup_where) = if app_type.is_some() {
            ("WHERE l.app_type = ?1", "WHERE r.app_type = ?2")
        } else {
            ("", "")
        };

        // UNION detail logs + rollup data, then aggregate
        let detail_pname = provider_name_coalesce("l", "p");
        let rollup_pname = provider_name_coalesce("r", "p2");
        let sql = format!(
            "SELECT
                provider_id, app_type, provider_name,
                SUM(request_count) as request_count,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost) as total_cost,
                SUM(success_count) as success_count,
                CASE WHEN SUM(request_count) > 0
                    THEN SUM(latency_sum) / SUM(request_count)
                    ELSE 0 END as avg_latency
            FROM (
                SELECT l.provider_id, l.app_type,
                    {detail_pname} as provider_name,
                    COUNT(*) as request_count,
                    COALESCE(SUM(l.input_tokens + l.output_tokens), 0) as total_tokens,
                    COALESCE(SUM(CAST(l.total_cost_usd AS REAL)), 0) as total_cost,
                    COALESCE(SUM(CASE WHEN l.status_code >= 200 AND l.status_code < 300 THEN 1 ELSE 0 END), 0) as success_count,
                    COALESCE(SUM(l.latency_ms), 0) as latency_sum
                FROM proxy_request_logs l
                LEFT JOIN providers p ON l.provider_id = p.id AND l.app_type = p.app_type
                {detail_where}
                GROUP BY l.provider_id, l.app_type
                UNION ALL
                SELECT r.provider_id, r.app_type,
                    {rollup_pname} as provider_name,
                    COALESCE(SUM(r.request_count), 0),
                    COALESCE(SUM(r.input_tokens + r.output_tokens), 0),
                    COALESCE(SUM(CAST(r.total_cost_usd AS REAL)), 0),
                    COALESCE(SUM(r.success_count), 0),
                    COALESCE(SUM(r.avg_latency_ms * r.request_count), 0)
                FROM usage_daily_rollups r
                LEFT JOIN providers p2 ON r.provider_id = p2.id AND r.app_type = p2.app_type
                {rollup_where}
                GROUP BY r.provider_id, r.app_type
            )
            GROUP BY provider_id, app_type
            ORDER BY total_cost DESC"
        );

        let mut stmt = conn.prepare(&sql)?;
        let row_mapper = |row: &rusqlite::Row| {
            let request_count: i64 = row.get(3)?;
            let success_count: i64 = row.get(6)?;
            let success_rate = if request_count > 0 {
                (success_count as f32 / request_count as f32) * 100.0
            } else {
                0.0
            };

            Ok(ProviderStats {
                provider_id: row.get(0)?,
                provider_name: row.get(2)?,
                request_count: request_count as u64,
                total_tokens: row.get::<_, i64>(4)? as u64,
                total_cost: format!("{:.6}", row.get::<_, f64>(5)?),
                success_rate,
                avg_latency_ms: row.get::<_, f64>(7)? as u64,
            })
        };

        let rows = if let Some(at) = app_type {
            stmt.query_map(params![at, at], row_mapper)?
        } else {
            stmt.query_map([], row_mapper)?
        };

        let mut stats = Vec::new();
        for row in rows {
            stats.push(row?);
        }

        Ok(stats)
    }

    /// 获取模型统计
    pub fn get_model_stats(&self, app_type: Option<&str>) -> Result<Vec<ModelStats>, AppError> {
        let conn = lock_conn!(self.conn);
        let _ = Self::backfill_missing_proxy_costs(&conn, app_type, None, None, None)?;

        let (detail_where, rollup_where) = if app_type.is_some() {
            ("WHERE app_type = ?1", "WHERE app_type = ?2")
        } else {
            ("", "")
        };

        // UNION detail logs + rollup data
        let sql = format!(
            "SELECT
                model,
                SUM(request_count) as request_count,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost) as total_cost
            FROM (
                SELECT model,
                    COUNT(*) as request_count,
                    COALESCE(SUM(input_tokens + output_tokens), 0) as total_tokens,
                    COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0) as total_cost
                FROM proxy_request_logs
                {detail_where}
                GROUP BY model
                UNION ALL
                SELECT model,
                    COALESCE(SUM(request_count), 0),
                    COALESCE(SUM(input_tokens + output_tokens), 0),
                    COALESCE(SUM(CAST(total_cost_usd AS REAL)), 0)
                FROM usage_daily_rollups
                {rollup_where}
                GROUP BY model
            )
            GROUP BY model
            ORDER BY total_cost DESC"
        );

        let mut stmt = conn.prepare(&sql)?;
        let row_mapper = |row: &rusqlite::Row| {
            let request_count: i64 = row.get(1)?;
            let total_cost: f64 = row.get(3)?;
            let avg_cost = if request_count > 0 {
                total_cost / request_count as f64
            } else {
                0.0
            };

            Ok(ModelStats {
                model: row.get(0)?,
                request_count: request_count as u64,
                total_tokens: row.get::<_, i64>(2)? as u64,
                total_cost: format!("{total_cost:.6}"),
                avg_cost_per_request: format!("{avg_cost:.6}"),
            })
        };

        let rows = if let Some(at) = app_type {
            stmt.query_map(params![at, at], row_mapper)?
        } else {
            stmt.query_map([], row_mapper)?
        };

        let mut stats = Vec::new();
        for row in rows {
            stats.push(row?);
        }

        Ok(stats)
    }

    /// 获取请求日志列表（分页）
    pub fn get_request_logs(
        &self,
        filters: &LogFilters,
        page: u32,
        page_size: u32,
    ) -> Result<PaginatedLogs, AppError> {
        let conn = lock_conn!(self.conn);

        let provider_name_expr = provider_name_coalesce("l", "p");
        let app_type_filter = normalized_app_type_filter(filters.app_type.as_deref());
        let mut conditions: Vec<String> = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(app_type) = app_type_filter {
            conditions.push("l.app_type = ?".to_string());
            params.push(Box::new(app_type.to_string()));
        }
        if let Some(ref provider_name) = filters.provider_name {
            conditions.push(format!("{provider_name_expr} LIKE ?"));
            params.push(Box::new(format!("%{provider_name}%")));
        }
        if let Some(ref model) = filters.model {
            conditions.push("l.model LIKE ?".to_string());
            params.push(Box::new(format!("%{model}%")));
        }
        if let Some(predicate) = request_log_source_group_predicate(
            "l",
            filters.source_group.as_deref(),
            app_type_filter,
        ) {
            conditions.push(predicate);
        }
        if let Some(status) = filters.status_code {
            conditions.push("l.status_code = ?".to_string());
            params.push(Box::new(status as i64));
        }
        if let Some(start) = filters.start_date {
            conditions.push("l.created_at >= ?".to_string());
            params.push(Box::new(start));
        }
        if let Some(end) = filters.end_date {
            conditions.push("l.created_at <= ?".to_string());
            params.push(Box::new(end));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // 获取总数
        let count_sql = format!(
            "SELECT COUNT(*) FROM proxy_request_logs l
             LEFT JOIN providers p ON l.provider_id = p.id AND l.app_type = p.app_type
             {where_clause}"
        );
        let count_params: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let total: u32 = conn.query_row(&count_sql, count_params.as_slice(), |row| {
            row.get::<_, i64>(0).map(|v| v as u32)
        })?;

        // 获取数据
        let offset = page * page_size;
        params.push(Box::new(page_size as i64));
        params.push(Box::new(offset as i64));

        let sql = format!(
            "SELECT l.request_id, l.provider_id, {provider_name_expr} as provider_name, l.app_type, l.model,
                    l.request_model, l.cost_multiplier,
                    l.input_tokens, l.output_tokens, l.cache_read_tokens, l.cache_creation_tokens,
                    l.input_cost_usd, l.output_cost_usd, l.cache_read_cost_usd, l.cache_creation_cost_usd, l.total_cost_usd,
                    l.is_streaming, l.latency_ms, l.first_token_ms, l.duration_ms,
                    l.status_code, l.error_message, l.created_at, l.data_source
             FROM proxy_request_logs l
             LEFT JOIN providers p ON l.provider_id = p.id AND l.app_type = p.app_type
             {where_clause}
             ORDER BY l.created_at DESC
             LIMIT ? OFFSET ?"
        );

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(RequestLogDetail {
                request_id: row.get(0)?,
                provider_id: row.get(1)?,
                provider_name: row.get(2)?,
                app_type: row.get(3)?,
                model: row.get(4)?,
                request_model: row.get(5)?,
                cost_multiplier: row
                    .get::<_, Option<String>>(6)?
                    .unwrap_or_else(|| "1".to_string()),
                input_tokens: row.get::<_, i64>(7)? as u32,
                output_tokens: row.get::<_, i64>(8)? as u32,
                cache_read_tokens: row.get::<_, i64>(9)? as u32,
                cache_creation_tokens: row.get::<_, i64>(10)? as u32,
                input_cost_usd: row.get(11)?,
                output_cost_usd: row.get(12)?,
                cache_read_cost_usd: row.get(13)?,
                cache_creation_cost_usd: row.get(14)?,
                total_cost_usd: row.get(15)?,
                is_streaming: row.get::<_, i64>(16)? != 0,
                latency_ms: row.get::<_, i64>(17)? as u64,
                first_token_ms: row.get::<_, Option<i64>>(18)?.map(|v| v as u64),
                duration_ms: row.get::<_, Option<i64>>(19)?.map(|v| v as u64),
                status_code: row.get::<_, i64>(20)? as u16,
                error_message: row.get(21)?,
                created_at: row.get(22)?,
                data_source: row.get(23)?,
            })
        })?;

        let mut logs = Vec::new();
        let mut provider_cache = HashMap::new();
        let mut pricing_cache = HashMap::new();

        for row in rows {
            let mut log = row?;
            Self::maybe_backfill_log_costs(
                &conn,
                &mut log,
                &mut provider_cache,
                &mut pricing_cache,
            )?;
            logs.push(log);
        }

        Ok(PaginatedLogs {
            data: logs,
            total,
            page,
            page_size,
        })
    }

    /// 获取单个请求详情
    pub fn get_request_detail(
        &self,
        request_id: &str,
    ) -> Result<Option<RequestLogDetail>, AppError> {
        let conn = lock_conn!(self.conn);

        let detail_pname = provider_name_coalesce("l", "p");
        let detail_sql = format!(
            "SELECT l.request_id, l.provider_id, {detail_pname} as provider_name, l.app_type, l.model,
                    l.request_model, l.cost_multiplier,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    input_cost_usd, output_cost_usd, cache_read_cost_usd, cache_creation_cost_usd, total_cost_usd,
                    is_streaming, latency_ms, first_token_ms, duration_ms,
                    status_code, error_message, created_at, l.data_source
             FROM proxy_request_logs l
             LEFT JOIN providers p ON l.provider_id = p.id AND l.app_type = p.app_type
             WHERE l.request_id = ?"
        );
        let result = conn.query_row(&detail_sql, [request_id], |row| {
            Ok(RequestLogDetail {
                request_id: row.get(0)?,
                provider_id: row.get(1)?,
                provider_name: row.get(2)?,
                app_type: row.get(3)?,
                model: row.get(4)?,
                request_model: row.get(5)?,
                cost_multiplier: row
                    .get::<_, Option<String>>(6)?
                    .unwrap_or_else(|| "1".to_string()),
                input_tokens: row.get::<_, i64>(7)? as u32,
                output_tokens: row.get::<_, i64>(8)? as u32,
                cache_read_tokens: row.get::<_, i64>(9)? as u32,
                cache_creation_tokens: row.get::<_, i64>(10)? as u32,
                input_cost_usd: row.get(11)?,
                output_cost_usd: row.get(12)?,
                cache_read_cost_usd: row.get(13)?,
                cache_creation_cost_usd: row.get(14)?,
                total_cost_usd: row.get(15)?,
                is_streaming: row.get::<_, i64>(16)? != 0,
                latency_ms: row.get::<_, i64>(17)? as u64,
                first_token_ms: row.get::<_, Option<i64>>(18)?.map(|v| v as u64),
                duration_ms: row.get::<_, Option<i64>>(19)?.map(|v| v as u64),
                status_code: row.get::<_, i64>(20)? as u16,
                error_message: row.get(21)?,
                created_at: row.get(22)?,
                data_source: row.get(23)?,
            })
        });

        match result {
            Ok(mut detail) => {
                let mut provider_cache = HashMap::new();
                let mut pricing_cache = HashMap::new();
                Self::maybe_backfill_log_costs(
                    &conn,
                    &mut detail,
                    &mut provider_cache,
                    &mut pricing_cache,
                )?;
                Ok(Some(detail))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AppError::Database(e.to_string())),
        }
    }

    /// 检查 Provider 使用限额
    pub fn check_provider_limits(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Result<ProviderLimitStatus, AppError> {
        let conn = lock_conn!(self.conn);
        let _ = Self::backfill_missing_proxy_costs(
            &conn,
            Some(app_type),
            None,
            None,
            Some(provider_id),
        )?;

        // 获取 provider 的限额设置
        let (limit_daily, limit_monthly) = conn
            .query_row(
                "SELECT meta FROM providers WHERE id = ? AND app_type = ?",
                params![provider_id, app_type],
                |row| {
                    let meta_str: String = row.get(0)?;
                    Ok(meta_str)
                },
            )
            .ok()
            .and_then(|meta_str| serde_json::from_str::<serde_json::Value>(&meta_str).ok())
            .map(|meta| {
                let daily = meta
                    .get("limitDailyUsd")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                let monthly = meta
                    .get("limitMonthlyUsd")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok());
                (daily, monthly)
            })
            .unwrap_or((None, None));

        // 计算今日使用量 (detail logs + rollup)
        let daily_usage: f64 = conn
            .query_row(
                "SELECT COALESCE(SUM(cost), 0) FROM (
                    SELECT CAST(total_cost_usd AS REAL) as cost
                    FROM proxy_request_logs
                    WHERE provider_id = ? AND app_type = ?
                      AND date(datetime(created_at, 'unixepoch', 'localtime')) = date('now', 'localtime')
                    UNION ALL
                    SELECT CAST(total_cost_usd AS REAL)
                    FROM usage_daily_rollups
                    WHERE provider_id = ? AND app_type = ?
                      AND date = date('now', 'localtime')
                )",
                params![provider_id, app_type, provider_id, app_type],
                |row| row.get(0),
            )
            .unwrap_or(0.0);

        // 计算本月使用量 (detail logs + rollup)
        let monthly_usage: f64 = conn
            .query_row(
                "SELECT COALESCE(SUM(cost), 0) FROM (
                    SELECT CAST(total_cost_usd AS REAL) as cost
                    FROM proxy_request_logs
                    WHERE provider_id = ? AND app_type = ?
                      AND strftime('%Y-%m', datetime(created_at, 'unixepoch', 'localtime')) = strftime('%Y-%m', 'now', 'localtime')
                    UNION ALL
                    SELECT CAST(total_cost_usd AS REAL)
                    FROM usage_daily_rollups
                    WHERE provider_id = ? AND app_type = ?
                      AND strftime('%Y-%m', date) = strftime('%Y-%m', 'now', 'localtime')
                )",
                params![provider_id, app_type, provider_id, app_type],
                |row| row.get(0),
            )
            .unwrap_or(0.0);

        let daily_exceeded = limit_daily
            .map(|limit| daily_usage >= limit)
            .unwrap_or(false);
        let monthly_exceeded = limit_monthly
            .map(|limit| monthly_usage >= limit)
            .unwrap_or(false);

        Ok(ProviderLimitStatus {
            provider_id: provider_id.to_string(),
            daily_usage: format!("{daily_usage:.6}"),
            daily_limit: limit_daily.map(|l| format!("{l:.2}")),
            daily_exceeded,
            monthly_usage: format!("{monthly_usage:.6}"),
            monthly_limit: limit_monthly.map(|l| format!("{l:.2}")),
            monthly_exceeded,
        })
    }
}

/// Provider 限额状态
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLimitStatus {
    pub provider_id: String,
    pub daily_usage: String,
    pub daily_limit: Option<String>,
    pub daily_exceeded: bool,
    pub monthly_usage: String,
    pub monthly_limit: Option<String>,
    pub monthly_exceeded: bool,
}

#[derive(Clone)]
struct PricingInfo {
    input: rust_decimal::Decimal,
    output: rust_decimal::Decimal,
    cache_read: rust_decimal::Decimal,
    cache_creation: rust_decimal::Decimal,
}

impl Database {
    fn maybe_backfill_log_costs(
        conn: &Connection,
        log: &mut RequestLogDetail,
        provider_cache: &mut HashMap<(String, String), rust_decimal::Decimal>,
        pricing_cache: &mut HashMap<String, PricingInfo>,
    ) -> Result<(), AppError> {
        let total_cost = rust_decimal::Decimal::from_str(&log.total_cost_usd)
            .unwrap_or(rust_decimal::Decimal::ZERO);
        let has_cost = total_cost > rust_decimal::Decimal::ZERO;
        let has_usage = log.input_tokens > 0
            || log.output_tokens > 0
            || log.cache_read_tokens > 0
            || log.cache_creation_tokens > 0;

        if has_cost || !has_usage {
            return Ok(());
        }

        let pricing = match Self::get_log_pricing_cached(conn, pricing_cache, log)? {
            Some(info) => info,
            None => return Ok(()),
        };
        let multiplier = Self::get_cost_multiplier_cached(
            conn,
            provider_cache,
            &log.provider_id,
            &log.app_type,
        )?;

        let million = rust_decimal::Decimal::from(1_000_000u64);

        // 与 CostCalculator::calculate 保持一致的计算逻辑：
        // 1. input_cost 需要扣除 cache_read_tokens（避免缓存部分被重复计费）
        // 2. 各项成本是基础成本（不含倍率）
        // 3. 倍率只作用于最终总价
        let billable_input_tokens =
            (log.input_tokens as u64).saturating_sub(log.cache_read_tokens as u64);
        let input_cost =
            rust_decimal::Decimal::from(billable_input_tokens) * pricing.input / million;
        let output_cost =
            rust_decimal::Decimal::from(log.output_tokens as u64) * pricing.output / million;
        let cache_read_cost = rust_decimal::Decimal::from(log.cache_read_tokens as u64)
            * pricing.cache_read
            / million;
        let cache_creation_cost = rust_decimal::Decimal::from(log.cache_creation_tokens as u64)
            * pricing.cache_creation
            / million;
        // 总成本 = 基础成本之和 × 倍率
        let base_total = input_cost + output_cost + cache_read_cost + cache_creation_cost;
        let total_cost = base_total * multiplier;

        log.input_cost_usd = format!("{input_cost:.6}");
        log.output_cost_usd = format!("{output_cost:.6}");
        log.cache_read_cost_usd = format!("{cache_read_cost:.6}");
        log.cache_creation_cost_usd = format!("{cache_creation_cost:.6}");
        log.total_cost_usd = format!("{total_cost:.6}");

        conn.execute(
            "UPDATE proxy_request_logs
             SET input_cost_usd = ?1,
                 output_cost_usd = ?2,
                 cache_read_cost_usd = ?3,
                 cache_creation_cost_usd = ?4,
                 total_cost_usd = ?5
             WHERE request_id = ?6",
            params![
                log.input_cost_usd,
                log.output_cost_usd,
                log.cache_read_cost_usd,
                log.cache_creation_cost_usd,
                log.total_cost_usd,
                log.request_id
            ],
        )
        .map_err(|e| AppError::Database(format!("更新请求成本失败: {e}")))?;

        Ok(())
    }

    fn get_cost_multiplier_cached(
        conn: &Connection,
        cache: &mut HashMap<(String, String), rust_decimal::Decimal>,
        provider_id: &str,
        app_type: &str,
    ) -> Result<rust_decimal::Decimal, AppError> {
        let key = (provider_id.to_string(), app_type.to_string());
        if let Some(multiplier) = cache.get(&key) {
            return Ok(*multiplier);
        }

        let meta_json: Option<String> = conn
            .query_row(
                "SELECT meta FROM providers WHERE id = ? AND app_type = ?",
                params![provider_id, app_type],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| AppError::Database(format!("查询 provider meta 失败: {e}")))?;

        let multiplier = meta_json
            .and_then(|meta| serde_json::from_str::<Value>(&meta).ok())
            .and_then(|value| value.get("costMultiplier").cloned())
            .and_then(|val| {
                val.as_str()
                    .and_then(|s| rust_decimal::Decimal::from_str(s).ok())
            })
            .unwrap_or(rust_decimal::Decimal::ONE);

        cache.insert(key, multiplier);
        Ok(multiplier)
    }

    fn get_model_pricing_cached(
        conn: &Connection,
        cache: &mut HashMap<String, PricingInfo>,
        model: &str,
    ) -> Result<Option<PricingInfo>, AppError> {
        if let Some(info) = cache.get(model) {
            return Ok(Some(info.clone()));
        }

        let row = find_model_pricing_row(conn, model)?;
        let Some((input, output, cache_read, cache_creation)) = row else {
            return Ok(None);
        };

        let pricing = PricingInfo {
            input: rust_decimal::Decimal::from_str(&input)
                .map_err(|e| AppError::Database(format!("解析输入价格失败: {e}")))?,
            output: rust_decimal::Decimal::from_str(&output)
                .map_err(|e| AppError::Database(format!("解析输出价格失败: {e}")))?,
            cache_read: rust_decimal::Decimal::from_str(&cache_read)
                .map_err(|e| AppError::Database(format!("解析缓存读取价格失败: {e}")))?,
            cache_creation: rust_decimal::Decimal::from_str(&cache_creation)
                .map_err(|e| AppError::Database(format!("解析缓存写入价格失败: {e}")))?,
        };

        cache.insert(model.to_string(), pricing.clone());
        Ok(Some(pricing))
    }

    fn get_log_pricing_cached(
        conn: &Connection,
        cache: &mut HashMap<String, PricingInfo>,
        log: &RequestLogDetail,
    ) -> Result<Option<PricingInfo>, AppError> {
        if let Some(info) = Self::get_model_pricing_cached(conn, cache, &log.model)? {
            return Ok(Some(info));
        }

        if let Some(request_model) = log.request_model.as_deref() {
            if request_model != log.model {
                return Self::get_model_pricing_cached(conn, cache, request_model);
            }
        }

        Ok(None)
    }
}

pub(crate) fn find_model_pricing_row(
    conn: &Connection,
    model_id: &str,
) -> Result<Option<(String, String, String, String)>, AppError> {
    // 清洗模型名称：去前缀(/)、去后缀(:)、@ 替换为 -
    // 例如 moonshotai/gpt-5.2-codex@low:v2 → gpt-5.2-codex-low
    let cleaned = model_id
        .rsplit_once('/')
        .map_or(model_id, |(_, r)| r)
        .split(':')
        .next()
        .unwrap_or(model_id)
        .trim()
        .replace('@', "-");

    // 精确匹配清洗后的名称
    let exact = conn
        .query_row(
            "SELECT input_cost_per_million, output_cost_per_million,
                    cache_read_cost_per_million, cache_creation_cost_per_million
             FROM model_pricing
             WHERE model_id = ?1",
            [&cleaned],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(|e| AppError::Database(format!("查询模型定价失败: {e}")))?;

    if exact.is_none() {
        log::warn!("模型 {model_id}（清洗后: {cleaned}）未找到定价信息，成本将记录为 0");
    }

    Ok(exact)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed_request_log_for_group_test(
        conn: &Connection,
        request_id: &str,
        provider_id: &str,
        app_type: &str,
        created_at: i64,
        data_source: &str,
    ) -> Result<(), AppError> {
        conn.execute(
            "INSERT INTO proxy_request_logs (
                request_id, provider_id, app_type, model,
                input_tokens, output_tokens, total_cost_usd,
                latency_ms, status_code, created_at, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                request_id,
                provider_id,
                app_type,
                "gpt-5",
                10,
                5,
                "0.01",
                100,
                200,
                created_at,
                data_source
            ],
        )?;
        Ok(())
    }

    fn seed_request_log_source_group_fixture(db: &Database) -> Result<(), AppError> {
        let conn = lock_conn!(db.conn);
        seed_request_log_for_group_test(&conn, "proxy-log", "p1", "codex", 1000, "proxy")?;
        seed_request_log_for_group_test(
            &conn,
            "claude-session-log",
            "_session",
            "claude",
            1001,
            "session_log",
        )?;
        seed_request_log_for_group_test(
            &conn,
            "codex-session-log",
            "_codex_session",
            "codex",
            1002,
            "codex_session",
        )?;
        seed_request_log_for_group_test(
            &conn,
            "gemini-session-log",
            "_gemini_session",
            "gemini",
            1003,
            "gemini_session",
        )?;
        seed_request_log_for_group_test(&conn, "codex-db-log", "p1", "codex", 1004, "codex_db")?;
        seed_request_log_for_group_test(
            &conn,
            "unknown-other-log",
            "p1",
            "codex",
            1005,
            "unknown_custom_source",
        )?;
        Ok(())
    }

    #[test]
    fn test_get_usage_summary() -> Result<(), AppError> {
        let db = Database::memory()?;

        // 插入测试数据
        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params!["req1", "p1", "claude", "claude-3", 100, 50, "0.01", 100, 200, 1000],
            )?;
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params!["req2", "p1", "claude", "claude-3", 200, 100, "0.02", 150, 200, 2000],
            )?;
        }

        let summary = db.get_usage_summary(None, None, None)?;
        assert_eq!(summary.total_requests, 2);
        assert_eq!(summary.success_rate, 100.0);

        Ok(())
    }

    #[test]
    fn test_get_usage_summary_excludes_session_sources() -> Result<(), AppError> {
        let db = Database::memory()?;
        let now = chrono::Utc::now().timestamp();
        let rollup_date = chrono::DateTime::from_timestamp(now - 40 * 86400, 0)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();

        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "proxy-detail",
                    "p1",
                    "codex",
                    "gpt-5",
                    100,
                    50,
                    "0.01",
                    100,
                    200,
                    now,
                    "proxy"
                ],
            )?;
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "session-detail",
                    "_codex_session",
                    "codex",
                    "gpt-5",
                    1000,
                    500,
                    "0.10",
                    100,
                    200,
                    now,
                    "codex_session"
                ],
            )?;
            conn.execute(
                "INSERT INTO usage_daily_rollups (
                    date, app_type, provider_id, model, request_count, success_count,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    total_cost_usd, avg_latency_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    rollup_date,
                    "codex",
                    "p2",
                    "gpt-5",
                    1,
                    1,
                    200,
                    100,
                    0,
                    0,
                    "0.02",
                    120
                ],
            )?;
            conn.execute(
                "INSERT INTO usage_daily_rollups (
                    date, app_type, provider_id, model, request_count, success_count,
                    input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens,
                    total_cost_usd, avg_latency_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    rollup_date,
                    "codex",
                    "_codex_session",
                    "gpt-5",
                    1,
                    1,
                    3000,
                    1500,
                    0,
                    0,
                    "0.30",
                    120
                ],
            )?;
        }

        let summary = db.get_usage_summary(None, None, None)?;

        assert_eq!(summary.total_requests, 2);
        assert_eq!(summary.total_input_tokens, 300);
        assert_eq!(summary.total_output_tokens, 150);
        assert_eq!(summary.total_cost, "0.030000");

        Ok(())
    }

    #[test]
    fn test_get_usage_summary_backfills_zero_cost_proxy_logs() -> Result<(), AppError> {
        let db = Database::memory()?;
        let now = chrono::Utc::now().timestamp();

        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT OR REPLACE INTO model_pricing (
                    model_id, display_name, input_cost_per_million, output_cost_per_million,
                    cache_read_cost_per_million, cache_creation_cost_per_million
                ) VALUES (?, ?, ?, ?, ?, ?)",
                params!["gpt-5.4-mini", "GPT-5.4 Mini", "0.75", "4.50", "0.075", "0"],
            )?;
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model, request_model,
                    input_tokens, output_tokens, cache_read_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "backfill-summary",
                    "p1",
                    "codex",
                    "gpt-5.4-mini-2026-03-17",
                    "gpt-5.4-mini",
                    1_000_000,
                    0,
                    0,
                    "0",
                    100,
                    200,
                    now,
                    "proxy"
                ],
            )?;
        }

        let summary = db.get_usage_summary(None, None, Some("codex"))?;

        assert_eq!(summary.total_requests, 1);
        assert_eq!(summary.total_cost, "0.750000");

        let conn = lock_conn!(db.conn);
        let persisted_cost: String = conn.query_row(
            "SELECT total_cost_usd FROM proxy_request_logs WHERE request_id = ?1",
            ["backfill-summary"],
            |row| row.get(0),
        )?;
        assert_eq!(persisted_cost, "0.750000");

        Ok(())
    }

    #[test]
    fn test_get_model_stats() -> Result<(), AppError> {
        let db = Database::memory()?;

        // 插入测试数据
        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "req1",
                    "p1",
                    "claude",
                    "claude-3-sonnet",
                    100,
                    50,
                    "0.01",
                    100,
                    200,
                    1000
                ],
            )?;
        }

        let stats = db.get_model_stats(None)?;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].model, "claude-3-sonnet");
        assert_eq!(stats[0].request_count, 1);

        Ok(())
    }

    #[test]
    fn test_get_daily_trends_excludes_session_sources() -> Result<(), AppError> {
        let db = Database::memory()?;
        let now = chrono::Utc::now().timestamp();
        let start = now - 3600;
        let end = now + 3600;

        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "proxy-trend",
                    "p1",
                    "codex",
                    "gpt-5",
                    120,
                    30,
                    "0.01",
                    100,
                    200,
                    now,
                    "proxy"
                ],
            )?;
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model,
                    input_tokens, output_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "session-trend",
                    "_codex_session",
                    "codex",
                    "gpt-5",
                    1000,
                    500,
                    "0.10",
                    100,
                    200,
                    now,
                    "codex_session"
                ],
            )?;
        }

        let trends = db.get_daily_trends(Some(start), Some(end), None)?;
        let total_requests: u64 = trends.iter().map(|stat| stat.request_count).sum();
        let total_input_tokens: u64 = trends.iter().map(|stat| stat.total_input_tokens).sum();
        let total_output_tokens: u64 = trends.iter().map(|stat| stat.total_output_tokens).sum();

        assert_eq!(total_requests, 1);
        assert_eq!(total_input_tokens, 120);
        assert_eq!(total_output_tokens, 30);

        Ok(())
    }

    #[test]
    fn test_get_daily_trends_backfills_zero_cost_proxy_logs() -> Result<(), AppError> {
        let db = Database::memory()?;
        let now = chrono::Utc::now().timestamp();

        {
            let conn = lock_conn!(db.conn);
            conn.execute(
                "INSERT OR REPLACE INTO model_pricing (
                    model_id, display_name, input_cost_per_million, output_cost_per_million,
                    cache_read_cost_per_million, cache_creation_cost_per_million
                ) VALUES (?, ?, ?, ?, ?, ?)",
                params!["gpt-5.4-mini", "GPT-5.4 Mini", "0.75", "4.50", "0.075", "0"],
            )?;
            conn.execute(
                "INSERT INTO proxy_request_logs (
                    request_id, provider_id, app_type, model, request_model,
                    input_tokens, output_tokens, cache_read_tokens, total_cost_usd,
                    latency_ms, status_code, created_at, data_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    "backfill-trends",
                    "p1",
                    "codex",
                    "gpt-5.4-mini-2026-03-17",
                    "gpt-5.4-mini",
                    1_000_000,
                    0,
                    0,
                    "0",
                    100,
                    200,
                    now,
                    "proxy"
                ],
            )?;
        }

        let trends = db.get_daily_trends(Some(now - 60), Some(now + 60), Some("codex"))?;
        let total_cost: f64 = trends
            .iter()
            .map(|stat| stat.total_cost.parse::<f64>().unwrap_or(0.0))
            .sum();

        assert!(
            (total_cost - 0.75).abs() < 1e-9,
            "expected 0.75, got {total_cost}"
        );

        Ok(())
    }

    #[test]
    fn test_model_pricing_matching() -> Result<(), AppError> {
        let db = Database::memory()?;
        let conn = lock_conn!(db.conn);

        // 准备额外定价数据，覆盖前缀/后缀清洗场景
        conn.execute(
            "INSERT OR REPLACE INTO model_pricing (
                model_id, display_name, input_cost_per_million, output_cost_per_million,
                cache_read_cost_per_million, cache_creation_cost_per_million
            ) VALUES (?, ?, ?, ?, ?, ?)",
            params![
                "claude-haiku-4.5",
                "Claude Haiku 4.5",
                "1.0",
                "2.0",
                "0.0",
                "0.0"
            ],
        )?;

        // 测试精确匹配（seed_model_pricing 已预置 claude-sonnet-4-5-20250929）
        let result = find_model_pricing_row(&conn, "claude-sonnet-4-5-20250929")?;
        assert!(
            result.is_some(),
            "应该能精确匹配 claude-sonnet-4-5-20250929"
        );

        // 清洗：去除前缀和冒号后缀
        let result = find_model_pricing_row(&conn, "anthropic/claude-haiku-4.5")?;
        assert!(
            result.is_some(),
            "带前缀的模型 anthropic/claude-haiku-4.5 应能匹配到 claude-haiku-4.5"
        );
        let result = find_model_pricing_row(&conn, "moonshotai/kimi-k2-0905:exa")?;
        assert!(
            result.is_some(),
            "带前缀+冒号后缀的模型应清洗后匹配到 kimi-k2-0905"
        );

        // 清洗：@ 替换为 -（seed_model_pricing 已预置 gpt-5.2-codex-low）
        let result = find_model_pricing_row(&conn, "gpt-5.2-codex@low")?;
        assert!(
            result.is_some(),
            "带 @ 分隔符的模型 gpt-5.2-codex@low 应能匹配到 gpt-5.2-codex-low"
        );

        // 测试不存在的模型
        let result = find_model_pricing_row(&conn, "unknown-model-123")?;
        assert!(result.is_none(), "不应该匹配不存在的模型");

        Ok(())
    }

    #[test]
    fn test_request_log_source_classifier_contract() {
        let cases = [
            (None, RequestLogSourceKind::Proxy, &[][..]),
            (Some("proxy"), RequestLogSourceKind::Proxy, &[][..]),
            (
                Some("session_log"),
                RequestLogSourceKind::Session,
                &["claude"][..],
            ),
            (
                Some("codex_session"),
                RequestLogSourceKind::Session,
                &["codex"][..],
            ),
            (
                Some("gemini_session"),
                RequestLogSourceKind::Session,
                &["gemini"][..],
            ),
            (
                Some("codex_db"),
                RequestLogSourceKind::Other,
                &["codex"][..],
            ),
            (
                Some("unknown_custom_source"),
                RequestLogSourceKind::Other,
                &[][..],
            ),
        ];

        for (data_source, expected_kind, expected_apps) in cases {
            let meta = classify_request_log_source(data_source);
            assert_eq!(
                meta.kind, expected_kind,
                "unexpected kind for {data_source:?}"
            );
            assert_eq!(
                meta.apps, expected_apps,
                "unexpected apps for {data_source:?}"
            );
        }
    }

    #[test]
    fn test_request_log_source_registry_integrity() {
        for (source, meta) in REQUEST_LOG_SOURCE_REGISTRY {
            match meta.kind {
                RequestLogSourceKind::Session => {
                    assert!(
                        !meta.apps.is_empty(),
                        "session source {source} must declare at least one app"
                    );
                }
                RequestLogSourceKind::Proxy | RequestLogSourceKind::Other => {}
            }
        }
    }

    #[test]
    fn test_get_request_logs_filters_by_source_group_proxy() -> Result<(), AppError> {
        let db = Database::memory()?;
        seed_request_log_source_group_fixture(&db)?;

        let result = db.get_request_logs(
            &LogFilters {
                source_group: Some("proxy".to_string()),
                ..Default::default()
            },
            0,
            20,
        )?;

        assert_eq!(result.total, 1);
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].request_id, "proxy-log");
        assert_eq!(result.data[0].data_source.as_deref(), Some("proxy"));

        Ok(())
    }

    #[test]
    fn test_get_request_logs_filters_by_source_group_session_and_app() -> Result<(), AppError> {
        let db = Database::memory()?;
        seed_request_log_source_group_fixture(&db)?;

        let session_result = db.get_request_logs(
            &LogFilters {
                source_group: Some("session".to_string()),
                app_type: Some("all".to_string()),
                ..Default::default()
            },
            0,
            20,
        )?;

        assert_eq!(session_result.total, 3);
        assert_eq!(session_result.data.len(), 3);
        assert_eq!(
            session_result
                .data
                .iter()
                .map(|log| log.request_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "gemini-session-log",
                "codex-session-log",
                "claude-session-log"
            ]
        );
        assert!(session_result
            .data
            .iter()
            .all(
                |log| classify_request_log_source(log.data_source.as_deref()).kind
                    == RequestLogSourceKind::Session
            ));

        let codex_result = db.get_request_logs(
            &LogFilters {
                source_group: Some("session".to_string()),
                app_type: Some("codex".to_string()),
                ..Default::default()
            },
            0,
            20,
        )?;

        assert_eq!(codex_result.total, 1);
        assert_eq!(codex_result.data.len(), 1);
        assert_eq!(codex_result.data[0].request_id, "codex-session-log");
        assert_eq!(
            codex_result.data[0].data_source.as_deref(),
            Some("codex_session")
        );

        Ok(())
    }

    #[test]
    fn test_get_request_logs_source_group_all_includes_other() -> Result<(), AppError> {
        let db = Database::memory()?;
        seed_request_log_source_group_fixture(&db)?;

        let result = db.get_request_logs(
            &LogFilters {
                source_group: Some("all".to_string()),
                ..Default::default()
            },
            0,
            20,
        )?;

        assert_eq!(result.total, 6);
        assert_eq!(result.data.len(), 6);
        assert!(result
            .data
            .iter()
            .any(|log| log.data_source.as_deref() == Some("codex_db")));
        assert!(result
            .data
            .iter()
            .any(|log| log.data_source.as_deref() == Some("unknown_custom_source")));

        Ok(())
    }

    #[test]
    fn test_get_request_logs_filters_by_session_provider_display_name() -> Result<(), AppError> {
        let db = Database::memory()?;
        seed_request_log_source_group_fixture(&db)?;

        let result = db.get_request_logs(
            &LogFilters {
                provider_name: Some("Codex (Session)".to_string()),
                source_group: Some("session".to_string()),
                app_type: Some("codex".to_string()),
                ..Default::default()
            },
            0,
            20,
        )?;

        assert_eq!(result.total, 1);
        assert_eq!(result.data.len(), 1);
        assert_eq!(result.data[0].request_id, "codex-session-log");
        assert_eq!(
            result.data[0].provider_name.as_deref(),
            Some("Codex (Session)")
        );

        Ok(())
    }
}
