//! 请求转发器
//!
//! 负责将请求转发到上游Provider，支持故障转移

use super::hyper_client::{ByteStream, ProxyResponse, ResponseTiming};
use super::{
    body_filter::filter_private_params_with_whitelist,
    error::*,
    error_mapper::{get_error_message, map_proxy_error_to_status},
    failover_switch::FailoverSwitchManager,
    log_codes::fwd as log_fwd,
    provider_router::ProviderRouter,
    providers::{get_adapter, AuthInfo, AuthStrategy, ProviderAdapter, ProviderType},
    response_processor::read_decoded_body,
    sse::strip_sse_field,
    thinking_budget_rectifier::{rectify_thinking_budget, should_rectify_thinking_budget},
    thinking_rectifier::{
        normalize_thinking_type, rectify_anthropic_request, should_rectify_thinking_signature,
    },
    types::{CopilotOptimizerConfig, OptimizerConfig, ProxyStatus, RectifierConfig},
    usage::{logger::UsageLogger, parser::TokenUsage},
    ProxyError,
};
use bytes::Bytes;
use futures::StreamExt;
use crate::commands::CopilotAuthState;
use crate::proxy::providers::copilot_auth::CopilotAuthManager;
use crate::proxy::extract_session_id;
use crate::{app_config::AppType, provider::Provider};
use http::Extensions;
use serde_json::Value;
use std::sync::Arc;
use tauri::Manager;
use tokio::sync::RwLock;

const CODEX_SEMANTIC_FAILURE_THRESHOLD: u32 = 3;

fn extract_model_from_endpoint(endpoint: &str) -> Option<String> {
    endpoint
        .split_once("/models/")
        .map(|(_, model_path)| model_path)
        .and_then(|model_path| {
            let model = model_path
                .split([':', '?'])
                .next()
                .unwrap_or(model_path)
                .trim();
            if model.is_empty() {
                None
            } else {
                Some(model.to_string())
            }
        })
}

fn extract_request_model(body: &Value, endpoint: &str) -> String {
    body.get("model")
        .and_then(|m| m.as_str())
        .map(str::to_string)
        .or_else(|| extract_model_from_endpoint(endpoint))
        .unwrap_or_else(|| "unknown".to_string())
}

pub struct ForwardResult {
    pub response: ProxyResponse,
    pub provider: Provider,
    pub claude_api_format: Option<String>,
}

pub struct ForwardError {
    pub error: ProxyError,
    pub provider: Option<Provider>,
}

pub struct RequestForwarder {
    /// 共享的 ProviderRouter（持有熔断器状态）
    router: Arc<ProviderRouter>,
    status: Arc<RwLock<ProxyStatus>>,
    current_providers: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
    /// 故障转移切换管理器
    failover_manager: Arc<FailoverSwitchManager>,
    /// AppHandle，用于发射事件和更新托盘
    app_handle: Option<tauri::AppHandle>,
    /// 请求开始时的"当前供应商 ID"（用于判断是否需要同步 UI/托盘）
    current_provider_id_at_start: String,
    /// 整流器配置
    rectifier_config: RectifierConfig,
    /// 优化器配置
    optimizer_config: OptimizerConfig,
    /// Copilot 优化器配置
    copilot_optimizer_config: CopilotOptimizerConfig,
    /// 非流式请求超时（秒）
    non_streaming_timeout: std::time::Duration,
    /// 流式首字节超时（秒）
    streaming_first_byte_timeout: std::time::Duration,
    /// 流式空闲超时（秒）
    streaming_idle_timeout: std::time::Duration,
}

enum SemanticValidationOutcome {
    Success(ProxyResponse),
    ToleratedAnomaly { response: ProxyResponse, note: String },
}

impl RequestForwarder {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: Arc<ProviderRouter>,
        non_streaming_timeout: u64,
        status: Arc<RwLock<ProxyStatus>>,
        current_providers: Arc<RwLock<std::collections::HashMap<String, (String, String)>>>,
        failover_manager: Arc<FailoverSwitchManager>,
        app_handle: Option<tauri::AppHandle>,
        current_provider_id_at_start: String,
        _streaming_first_byte_timeout: u64,
        _streaming_idle_timeout: u64,
        rectifier_config: RectifierConfig,
        optimizer_config: OptimizerConfig,
        copilot_optimizer_config: CopilotOptimizerConfig,
    ) -> Self {
        Self {
            router,
            status,
            current_providers,
            failover_manager,
            app_handle,
            current_provider_id_at_start,
            rectifier_config,
            optimizer_config,
            copilot_optimizer_config,
            non_streaming_timeout: std::time::Duration::from_secs(non_streaming_timeout),
            streaming_first_byte_timeout: std::time::Duration::from_secs(
                _streaming_first_byte_timeout,
            ),
            streaming_idle_timeout: std::time::Duration::from_secs(_streaming_idle_timeout),
        }
    }

    fn persist_attempt_failure(
        &self,
        provider: &Provider,
        app_type: &str,
        endpoint: &str,
        body: &Value,
        headers: &axum::http::HeaderMap,
        error: &ProxyError,
        latency_ms: u64,
    ) {
        let db = self.router.db();
        let logger = UsageLogger::new(db.as_ref());

        if let Err(log_error) = logger.log_error_with_context(
            uuid::Uuid::new_v4().to_string(),
            provider.id.clone(),
            app_type.to_string(),
            extract_request_model(body, endpoint),
            map_proxy_error_to_status(error),
            get_error_message(error),
            latency_ms,
            should_force_identity_encoding(endpoint, body, headers),
            Some(extract_session_id(headers, body, app_type).session_id),
            provider
                .meta
                .as_ref()
                .and_then(|meta| meta.provider_type.clone()),
        ) {
            log::warn!(
                "[{app_type}] 记录失败尝试日志失败 (provider={}): {log_error}",
                provider.id
            );
        }
    }

    /// 转发请求（带故障转移）
    ///
    /// # Arguments
    /// * `app_type` - 应用类型
    /// * `endpoint` - API 端点
    /// * `body` - 请求体
    /// * `headers` - 请求头
    /// * `providers` - 已选择的 Provider 列表（由 RequestContext 提供，避免重复调用 select_providers）
    pub async fn forward_with_retry(
        &self,
        app_type: &AppType,
        endpoint: &str,
        body: Value,
        headers: axum::http::HeaderMap,
        extensions: Extensions,
        providers: Vec<Provider>,
    ) -> Result<ForwardResult, ForwardError> {
        // 获取适配器
        let adapter = get_adapter(app_type);
        let app_type_str = app_type.as_str();

        if providers.is_empty() {
            return Err(ForwardError {
                error: ProxyError::NoAvailableProvider,
                provider: None,
            });
        }

        let mut last_error = None;
        let mut last_provider = None;
        let mut attempted_providers = 0usize;

        // 整流器重试标记：确保整流最多触发一次
        let mut rectifier_retried = false;
        let mut budget_rectifier_retried = false;

        // 单 Provider 场景下跳过熔断器检查（故障转移关闭时）
        let bypass_circuit_breaker = providers.len() == 1;

        // 依次尝试每个供应商
        for provider in providers.iter() {
            // 发起请求前先获取熔断器放行许可（HalfOpen 会占用探测名额）
            // 单 Provider 场景下跳过此检查，避免熔断器阻塞所有请求
            let (allowed, used_half_open_permit) = if bypass_circuit_breaker {
                (true, false)
            } else {
                let permit = self
                    .router
                    .allow_provider_request(&provider.id, app_type_str)
                    .await;
                (permit.allowed, permit.used_half_open_permit)
            };

            if !allowed {
                continue;
            }

            // PRE-SEND 优化器：每个 provider 独立决定是否优化
            // clone body 以避免 Bedrock 优化字段泄漏到非 Bedrock provider（failover 场景）
            let mut provider_body =
                if self.optimizer_config.enabled && is_bedrock_provider(provider) {
                    let mut b = body.clone();
                    if self.optimizer_config.thinking_optimizer {
                        super::thinking_optimizer::optimize(&mut b, &self.optimizer_config);
                    }
                    if self.optimizer_config.cache_injection {
                        super::cache_injector::inject(&mut b, &self.optimizer_config);
                    }
                    b
                } else {
                    body.clone()
                };

            attempted_providers += 1;

            // 更新状态中的当前Provider信息
            {
                let mut status = self.status.write().await;
                status.current_provider = Some(provider.name.clone());
                status.current_provider_id = Some(provider.id.clone());
                status.total_requests += 1;
                status.last_request_at = Some(chrono::Utc::now().to_rfc3339());
            }

            // 转发请求（每个 Provider 只尝试一次，重试由客户端控制）
            let attempt_started_at = std::time::Instant::now();
            let attempt_result = match self
                .forward(
                    provider,
                    endpoint,
                    &provider_body,
                    &headers,
                    &extensions,
                    adapter.as_ref(),
                )
                .await
            {
                Ok((response, claude_api_format)) => self
                    .validate_semantic_success(
                        app_type,
                        app_type_str,
                        &provider.id,
                        response,
                        attempt_started_at,
                    )
                    .await
                    .map(|validated_response| (validated_response, claude_api_format)),
                Err(e) => Err(e),
            };

            match attempt_result {
                Ok((SemanticValidationOutcome::Success(response), claude_api_format)) => {
                    // 成功：记录成功并更新熔断器
                    let _ = self
                        .router
                        .record_result(
                            &provider.id,
                            app_type_str,
                            used_half_open_permit,
                            true,
                            None,
                        )
                        .await;

                    // 更新当前应用类型使用的 provider
                    {
                        let mut current_providers = self.current_providers.write().await;
                        current_providers.insert(
                            app_type_str.to_string(),
                            (provider.id.clone(), provider.name.clone()),
                        );
                    }

                    // 更新成功统计
                    {
                        let mut status = self.status.write().await;
                        status.success_requests += 1;
                        status.last_error = None;
                        let should_switch =
                            self.current_provider_id_at_start.as_str() != provider.id.as_str();
                        if should_switch {
                            status.failover_count += 1;

                            // 异步触发供应商切换，更新 UI/托盘，并把“当前供应商”同步为实际使用的 provider
                            let fm = self.failover_manager.clone();
                            let ah = self.app_handle.clone();
                            let pid = provider.id.clone();
                            let pname = provider.name.clone();
                            let at = app_type_str.to_string();

                            tokio::spawn(async move {
                                let _ = fm.try_switch(ah.as_ref(), &at, &pid, &pname).await;
                            });
                        }
                        // 重新计算成功率
                        if status.total_requests > 0 {
                            status.success_rate = (status.success_requests as f32
                                / status.total_requests as f32)
                                * 100.0;
                        }
                    }

                    return Ok(ForwardResult {
                        response,
                        provider: provider.clone(),
                        claude_api_format,
                    });
                }
                Ok((
                    SemanticValidationOutcome::ToleratedAnomaly { response, note },
                    claude_api_format,
                )) => {
                    self.router
                        .release_permit_neutral(
                            &provider.id,
                            app_type_str,
                            used_half_open_permit,
                        )
                        .await;

                    {
                        let mut status = self.status.write().await;
                        status.failed_requests += 1;
                        status.last_error = Some(note.clone());
                        if status.total_requests > 0 {
                            status.success_rate = (status.success_requests as f32
                                / status.total_requests as f32)
                                * 100.0;
                        }
                    }

                    log::warn!("[{app_type_str}] [SEM-001] {note}");

                    return Ok(ForwardResult {
                        response,
                        provider: provider.clone(),
                        claude_api_format,
                    });
                }
                Err(e) => {
                    self.persist_attempt_failure(
                        provider,
                        app_type_str,
                        endpoint,
                        &provider_body,
                        &headers,
                        &e,
                        attempt_started_at.elapsed().as_millis() as u64,
                    );

                    // 检测是否需要触发整流器（仅 Claude/ClaudeAuth 供应商）
                    let provider_type = ProviderType::from_app_type_and_config(app_type, provider);
                    let is_anthropic_provider = matches!(
                        provider_type,
                        ProviderType::Claude | ProviderType::ClaudeAuth
                    );
                    let mut signature_rectifier_non_retryable_client_error = false;

                    if is_anthropic_provider {
                        let error_message = extract_error_message(&e);
                        if should_rectify_thinking_signature(
                            error_message.as_deref(),
                            &self.rectifier_config,
                        ) {
                            // 已经重试过：直接返回错误（不可重试客户端错误）
                            if rectifier_retried {
                                log::warn!("[{app_type_str}] [RECT-005] 整流器已触发过，不再重试");
                                // 释放 HalfOpen permit（不记录熔断器，这是客户端兼容性问题）
                                self.router
                                    .release_permit_neutral(
                                        &provider.id,
                                        app_type_str,
                                        used_half_open_permit,
                                    )
                                    .await;
                                let mut status = self.status.write().await;
                                status.failed_requests += 1;
                                status.last_error = Some(e.to_string());
                                if status.total_requests > 0 {
                                    status.success_rate = (status.success_requests as f32
                                        / status.total_requests as f32)
                                        * 100.0;
                                }
                                return Err(ForwardError {
                                    error: e,
                                    provider: Some(provider.clone()),
                                });
                            }

                            // 首次触发：整流请求体
                            let rectified = rectify_anthropic_request(&mut provider_body);

                            // 整流未生效：继续尝试 budget 整流路径，避免误判后短路
                            if !rectified.applied {
                                log::warn!(
                                    "[{app_type_str}] [RECT-006] thinking 签名整流器触发但无可整流内容，继续检查 budget；若 budget 也未命中则按客户端错误返回"
                                );
                                signature_rectifier_non_retryable_client_error = true;
                            } else {
                                log::info!(
                                    "[{}] [RECT-001] thinking 签名整流器触发, 移除 {} thinking blocks, {} redacted_thinking blocks, {} signature fields",
                                    app_type_str,
                                    rectified.removed_thinking_blocks,
                                    rectified.removed_redacted_thinking_blocks,
                                    rectified.removed_signature_fields
                                );

                                // 标记已重试（当前逻辑下重试后必定 return，保留标记以备将来扩展）
                                let _ = std::mem::replace(&mut rectifier_retried, true);

                                // 使用同一供应商重试（不计入熔断器）
                                let rectifier_retry_started_at = std::time::Instant::now();
                                match self
                                    .forward(
                                        provider,
                                        endpoint,
                                        &provider_body,
                                        &headers,
                                        &extensions,
                                        adapter.as_ref(),
                                    )
                                    .await
                                {
                                    Ok((response, claude_api_format)) => {
                                        log::info!("[{app_type_str}] [RECT-002] 整流重试成功");
                                        // 记录成功
                                        let _ = self
                                            .router
                                            .record_result(
                                                &provider.id,
                                                app_type_str,
                                                used_half_open_permit,
                                                true,
                                                None,
                                            )
                                            .await;

                                        // 更新当前应用类型使用的 provider
                                        {
                                            let mut current_providers =
                                                self.current_providers.write().await;
                                            current_providers.insert(
                                                app_type_str.to_string(),
                                                (provider.id.clone(), provider.name.clone()),
                                            );
                                        }

                                        // 更新成功统计
                                        {
                                            let mut status = self.status.write().await;
                                            status.success_requests += 1;
                                            status.last_error = None;
                                            let should_switch =
                                                self.current_provider_id_at_start.as_str()
                                                    != provider.id.as_str();
                                            if should_switch {
                                                status.failover_count += 1;

                                                // 异步触发供应商切换，更新 UI/托盘
                                                let fm = self.failover_manager.clone();
                                                let ah = self.app_handle.clone();
                                                let pid = provider.id.clone();
                                                let pname = provider.name.clone();
                                                let at = app_type_str.to_string();

                                                tokio::spawn(async move {
                                                    let _ = fm
                                                        .try_switch(ah.as_ref(), &at, &pid, &pname)
                                                        .await;
                                                });
                                            }
                                            if status.total_requests > 0 {
                                                status.success_rate = (status.success_requests
                                                    as f32
                                                    / status.total_requests as f32)
                                                    * 100.0;
                                            }
                                        }

                                        return Ok(ForwardResult {
                                            response,
                                            provider: provider.clone(),
                                            claude_api_format,
                                        });
                                    }
                                    Err(retry_err) => {
                                        self.persist_attempt_failure(
                                            provider,
                                            app_type_str,
                                            endpoint,
                                            &provider_body,
                                            &headers,
                                            &retry_err,
                                            rectifier_retry_started_at.elapsed().as_millis() as u64,
                                        );
                                        // 整流重试仍失败：区分错误类型决定是否记录熔断器
                                        log::warn!(
                                            "[{app_type_str}] [RECT-003] 整流重试仍失败: {retry_err}"
                                        );

                                        // 区分错误类型：Provider 问题记录失败，客户端问题仅释放 permit
                                        let is_provider_error = match &retry_err {
                                            ProxyError::Timeout(_)
                                            | ProxyError::ForwardFailed(_) => true,
                                            ProxyError::UpstreamError { status, .. } => {
                                                *status >= 500
                                            }
                                            _ => false,
                                        };

                                        if is_provider_error {
                                            // Provider 问题：记录失败到熔断器
                                            let _ = self
                                                .router
                                                .record_result(
                                                    &provider.id,
                                                    app_type_str,
                                                    used_half_open_permit,
                                                    false,
                                                    Some(retry_err.to_string()),
                                                )
                                                .await;
                                        } else {
                                            // 客户端问题：仅释放 permit，不记录熔断器
                                            self.router
                                                .release_permit_neutral(
                                                    &provider.id,
                                                    app_type_str,
                                                    used_half_open_permit,
                                                )
                                                .await;
                                        }

                                        let mut status = self.status.write().await;
                                        status.failed_requests += 1;
                                        status.last_error = Some(retry_err.to_string());
                                        if status.total_requests > 0 {
                                            status.success_rate = (status.success_requests as f32
                                                / status.total_requests as f32)
                                                * 100.0;
                                        }
                                        return Err(ForwardError {
                                            error: retry_err,
                                            provider: Some(provider.clone()),
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // 检测是否需要触发 budget 整流器（仅 Claude/ClaudeAuth 供应商）
                    if is_anthropic_provider {
                        let error_message = extract_error_message(&e);
                        if should_rectify_thinking_budget(
                            error_message.as_deref(),
                            &self.rectifier_config,
                        ) {
                            // 已经重试过：直接返回错误（不可重试客户端错误）
                            if budget_rectifier_retried {
                                log::warn!(
                                    "[{app_type_str}] [RECT-013] budget 整流器已触发过，不再重试"
                                );
                                self.router
                                    .release_permit_neutral(
                                        &provider.id,
                                        app_type_str,
                                        used_half_open_permit,
                                    )
                                    .await;
                                let mut status = self.status.write().await;
                                status.failed_requests += 1;
                                status.last_error = Some(e.to_string());
                                if status.total_requests > 0 {
                                    status.success_rate = (status.success_requests as f32
                                        / status.total_requests as f32)
                                        * 100.0;
                                }
                                return Err(ForwardError {
                                    error: e,
                                    provider: Some(provider.clone()),
                                });
                            }

                            let budget_rectified = rectify_thinking_budget(&mut provider_body);
                            if !budget_rectified.applied {
                                log::warn!(
                                    "[{app_type_str}] [RECT-014] budget 整流器触发但无可整流内容，不做无意义重试"
                                );
                                self.router
                                    .release_permit_neutral(
                                        &provider.id,
                                        app_type_str,
                                        used_half_open_permit,
                                    )
                                    .await;
                                let mut status = self.status.write().await;
                                status.failed_requests += 1;
                                status.last_error = Some(e.to_string());
                                if status.total_requests > 0 {
                                    status.success_rate = (status.success_requests as f32
                                        / status.total_requests as f32)
                                        * 100.0;
                                }
                                return Err(ForwardError {
                                    error: e,
                                    provider: Some(provider.clone()),
                                });
                            }

                            log::info!(
                                "[{}] [RECT-010] thinking budget 整流器触发, before={:?}, after={:?}",
                                app_type_str,
                                budget_rectified.before,
                                budget_rectified.after
                            );

                            let _ = std::mem::replace(&mut budget_rectifier_retried, true);

                            // 使用同一供应商重试（不计入熔断器）
                            let budget_retry_started_at = std::time::Instant::now();
                            match self
                                .forward(
                                    provider,
                                    endpoint,
                                    &provider_body,
                                    &headers,
                                    &extensions,
                                    adapter.as_ref(),
                                )
                                .await
                            {
                                Ok((response, claude_api_format)) => {
                                    log::info!("[{app_type_str}] [RECT-011] budget 整流重试成功");
                                    let _ = self
                                        .router
                                        .record_result(
                                            &provider.id,
                                            app_type_str,
                                            used_half_open_permit,
                                            true,
                                            None,
                                        )
                                        .await;

                                    {
                                        let mut current_providers =
                                            self.current_providers.write().await;
                                        current_providers.insert(
                                            app_type_str.to_string(),
                                            (provider.id.clone(), provider.name.clone()),
                                        );
                                    }

                                    {
                                        let mut status = self.status.write().await;
                                        status.success_requests += 1;
                                        status.last_error = None;
                                        let should_switch =
                                            self.current_provider_id_at_start.as_str()
                                                != provider.id.as_str();
                                        if should_switch {
                                            status.failover_count += 1;
                                            let fm = self.failover_manager.clone();
                                            let ah = self.app_handle.clone();
                                            let pid = provider.id.clone();
                                            let pname = provider.name.clone();
                                            let at = app_type_str.to_string();
                                            tokio::spawn(async move {
                                                let _ = fm
                                                    .try_switch(ah.as_ref(), &at, &pid, &pname)
                                                    .await;
                                            });
                                        }
                                        if status.total_requests > 0 {
                                            status.success_rate = (status.success_requests as f32
                                                / status.total_requests as f32)
                                                * 100.0;
                                        }
                                    }

                                    return Ok(ForwardResult {
                                        response,
                                        provider: provider.clone(),
                                        claude_api_format,
                                    });
                                }
                                Err(retry_err) => {
                                    self.persist_attempt_failure(
                                        provider,
                                        app_type_str,
                                        endpoint,
                                        &provider_body,
                                        &headers,
                                        &retry_err,
                                        budget_retry_started_at.elapsed().as_millis() as u64,
                                    );
                                    log::warn!(
                                        "[{app_type_str}] [RECT-012] budget 整流重试仍失败: {retry_err}"
                                    );

                                    let is_provider_error = match &retry_err {
                                        ProxyError::Timeout(_) | ProxyError::ForwardFailed(_) => {
                                            true
                                        }
                                        ProxyError::UpstreamError { status, .. } => *status >= 500,
                                        _ => false,
                                    };

                                    if is_provider_error {
                                        let _ = self
                                            .router
                                            .record_result(
                                                &provider.id,
                                                app_type_str,
                                                used_half_open_permit,
                                                false,
                                                Some(retry_err.to_string()),
                                            )
                                            .await;
                                    } else {
                                        self.router
                                            .release_permit_neutral(
                                                &provider.id,
                                                app_type_str,
                                                used_half_open_permit,
                                            )
                                            .await;
                                    }

                                    let mut status = self.status.write().await;
                                    status.failed_requests += 1;
                                    status.last_error = Some(retry_err.to_string());
                                    if status.total_requests > 0 {
                                        status.success_rate = (status.success_requests as f32
                                            / status.total_requests as f32)
                                            * 100.0;
                                    }
                                    return Err(ForwardError {
                                        error: retry_err,
                                        provider: Some(provider.clone()),
                                    });
                                }
                            }
                        }
                    }

                    if signature_rectifier_non_retryable_client_error {
                        self.router
                            .release_permit_neutral(
                                &provider.id,
                                app_type_str,
                                used_half_open_permit,
                            )
                            .await;
                        let mut status = self.status.write().await;
                        status.failed_requests += 1;
                        status.last_error = Some(e.to_string());
                        if status.total_requests > 0 {
                            status.success_rate = (status.success_requests as f32
                                / status.total_requests as f32)
                                * 100.0;
                        }
                        return Err(ForwardError {
                            error: e,
                            provider: Some(provider.clone()),
                        });
                    }

                    // 失败：记录失败并更新熔断器
                    let _ = self
                        .router
                        .record_result(
                            &provider.id,
                            app_type_str,
                            used_half_open_permit,
                            false,
                            Some(e.to_string()),
                        )
                        .await;

                    // 分类错误
                    let category = self.categorize_proxy_error(&e);

                    match category {
                        ErrorCategory::Retryable => {
                            // 可重试：更新错误信息，继续尝试下一个供应商
                            {
                                let mut status = self.status.write().await;
                                status.last_error =
                                    Some(format!("Provider {} 失败: {}", provider.name, e));
                            }

                            let (log_code, log_message) = build_retryable_failure_log(
                                &provider.name,
                                attempted_providers,
                                providers.len(),
                                &e,
                            );
                            log::warn!("[{app_type_str}] [{log_code}] {log_message}");

                            last_error = Some(e);
                            last_provider = Some(provider.clone());
                            // 继续尝试下一个供应商
                            continue;
                        }
                        ErrorCategory::NonRetryable | ErrorCategory::ClientAbort => {
                            // 不可重试：直接返回错误
                            {
                                let mut status = self.status.write().await;
                                status.failed_requests += 1;
                                status.last_error = Some(e.to_string());
                                if status.total_requests > 0 {
                                    status.success_rate = (status.success_requests as f32
                                        / status.total_requests as f32)
                                        * 100.0;
                                }
                            }
                            return Err(ForwardError {
                                error: e,
                                provider: Some(provider.clone()),
                            });
                        }
                    }
                }
            }
        }

        if attempted_providers == 0 {
            // providers 列表非空，但全部被熔断器拒绝（典型：HalfOpen 探测名额被占用）
            {
                let mut status = self.status.write().await;
                status.failed_requests += 1;
                status.last_error = Some("所有供应商暂时不可用（熔断器限制）".to_string());
                if status.total_requests > 0 {
                    status.success_rate =
                        (status.success_requests as f32 / status.total_requests as f32) * 100.0;
                }
            }
            return Err(ForwardError {
                error: ProxyError::NoAvailableProvider,
                provider: None,
            });
        }

        // 所有供应商都失败了
        {
            let mut status = self.status.write().await;
            status.failed_requests += 1;
            status.last_error = Some("所有供应商都失败".to_string());
            if status.total_requests > 0 {
                status.success_rate =
                    (status.success_requests as f32 / status.total_requests as f32) * 100.0;
            }
        }

        if let Some((log_code, log_message)) =
            build_terminal_failure_log(attempted_providers, providers.len(), last_error.as_ref())
        {
            log::warn!("[{app_type_str}] [{log_code}] {log_message}");
        }

        Err(ForwardError {
            error: last_error.unwrap_or(ProxyError::MaxRetriesExceeded),
            provider: last_provider,
        })
    }

    /// 转发单个请求（使用适配器）
    async fn forward(
        &self,
        provider: &Provider,
        endpoint: &str,
        body: &Value,
        headers: &axum::http::HeaderMap,
        extensions: &Extensions,
        adapter: &dyn ProviderAdapter,
    ) -> Result<(ProxyResponse, Option<String>), ProxyError> {
        // 使用适配器提取 base_url
        let mut base_url = adapter.extract_base_url(provider)?;

        let is_full_url = provider
            .meta
            .as_ref()
            .and_then(|meta| meta.is_full_url)
            .unwrap_or(false);

        // 应用模型映射（独立于格式转换）
        let (mapped_body, _original_model, _mapped_model) =
            super::model_mapper::apply_model_mapping(body.clone(), provider);

        // 与 CCH 对齐：请求前不做 thinking 主动改写（仅保留兼容入口）
        let mut mapped_body = normalize_thinking_type(mapped_body);

        // 确定有效端点
        // GitHub Copilot API 使用 /chat/completions（无 /v1 前缀）
        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot")
            || base_url.contains("githubcopilot.com");

        // --- Copilot 优化器：请求体优化 + 分类（在格式转换之前执行） ---
        // 注意：确定性 ID 也在此处计算，因为 mapped_body 在格式转换时会被 move
        let copilot_optimization = if is_copilot && self.copilot_optimizer_config.enabled {
            // 1. Tool result 合并 — 必须在分类之前执行
            //    合并将 [tool_result, text] 变为 [tool_result(含text)]，
            //    分类才能正确识别为 agent（全是 tool_result）而非 user（有 text block）
            if self.copilot_optimizer_config.tool_result_merging {
                mapped_body = super::copilot_optimizer::merge_tool_results(mapped_body);
            }

            // 2. 在合并后的 body 上进行分类
            let has_anthropic_beta = headers.contains_key("anthropic-beta");
            let classification = super::copilot_optimizer::classify_request(
                &mapped_body,
                has_anthropic_beta,
                self.copilot_optimizer_config.compact_detection,
            );

            log::debug!(
                "[Copilot] 优化器分类: initiator={}, is_warmup={}, is_compact={}",
                classification.initiator,
                classification.is_warmup,
                classification.is_compact
            );

            // 3. Warmup 小模型降级
            if self.copilot_optimizer_config.warmup_downgrade && classification.is_warmup {
                log::info!(
                    "[Copilot] Warmup 请求降级到模型: {}",
                    self.copilot_optimizer_config.warmup_model
                );
                mapped_body["model"] =
                    serde_json::json!(&self.copilot_optimizer_config.warmup_model);
            }

            // 预计算确定性 Request ID（在 body 被 move 之前）
            // 使用 session_id 从 body.metadata.user_id 或请求头提取
            let session_id = body
                .pointer("/metadata/user_id")
                .and_then(|v| v.as_str())
                .or_else(|| headers.get("x-session-id").and_then(|v| v.to_str().ok()))
                .unwrap_or("");
            let det_request_id = if self.copilot_optimizer_config.deterministic_request_id {
                Some(super::copilot_optimizer::deterministic_request_id(
                    &mapped_body,
                    session_id,
                ))
            } else {
                None
            };

            Some((classification, det_request_id))
        } else {
            None
        };

        // GitHub Copilot 动态 endpoint 路由
        // 从 CopilotAuthManager 获取缓存的 API endpoint（支持企业版等非默认 endpoint）
        if is_copilot && !is_full_url {
            if let Some(app_handle) = &self.app_handle {
                let copilot_state = app_handle.state::<CopilotAuthState>();
                let copilot_auth = copilot_state.0.read().await;

                // 从 provider.meta 获取关联的 GitHub 账号 ID
                let account_id = provider
                    .meta
                    .as_ref()
                    .and_then(|m| m.managed_account_id_for("github_copilot"));

                let dynamic_endpoint = match &account_id {
                    Some(id) => copilot_auth.get_api_endpoint(id).await,
                    None => copilot_auth.get_default_api_endpoint().await,
                };

                // 只在动态 endpoint 与当前 base_url 不同时替换
                if dynamic_endpoint != base_url {
                    log::debug!(
                        "[Copilot] 使用动态 API endpoint: {} (原: {})",
                        dynamic_endpoint,
                        base_url
                    );
                    base_url = dynamic_endpoint;
                }
            }
        }
        let resolved_claude_api_format = if adapter.name() == "Claude" {
            Some(
                self.resolve_claude_api_format(provider, &mapped_body, is_copilot)
                    .await,
            )
        } else {
            None
        };
        let needs_transform = match resolved_claude_api_format.as_deref() {
            Some(api_format) => super::providers::claude_api_format_needs_transform(api_format),
            None => adapter.needs_transform(provider),
        };
        let (effective_endpoint, passthrough_query) =
            if needs_transform && adapter.name() == "Claude" {
                let api_format = resolved_claude_api_format
                    .as_deref()
                    .unwrap_or_else(|| super::providers::get_claude_api_format(provider));
                rewrite_claude_transform_endpoint(endpoint, api_format, is_copilot)
            } else {
                (
                    endpoint.to_string(),
                    split_endpoint_and_query(endpoint)
                        .1
                        .map(ToString::to_string),
                )
            };

        let url = if is_full_url {
            append_query_to_full_url(&base_url, passthrough_query.as_deref())
        } else {
            adapter.build_url(&base_url, &effective_endpoint)
        };

        // 转换请求体（如果需要）
        let request_body = if needs_transform {
            if adapter.name() == "Claude" {
                let api_format = resolved_claude_api_format
                    .as_deref()
                    .unwrap_or_else(|| super::providers::get_claude_api_format(provider));
                super::providers::transform_claude_request_for_api_format(
                    mapped_body,
                    provider,
                    api_format,
                )?
            } else {
                adapter.transform_request(mapped_body, provider)?
            }
        } else {
            mapped_body
        };

        // 过滤私有参数（以 `_` 开头的字段），防止内部信息泄露到上游
        // 默认使用空白名单，过滤所有 _ 前缀字段
        let filtered_body = filter_private_params_with_whitelist(request_body, &[]);
        let force_identity_encoding = needs_transform
            || should_force_identity_encoding(&effective_endpoint, &filtered_body, headers);

        // 获取认证头（提前准备，用于内联替换）
        let mut auth_headers = if let Some(mut auth) = adapter.extract_auth(provider) {
            // GitHub Copilot 特殊处理：从 CopilotAuthManager 获取真实 token
            if auth.strategy == AuthStrategy::GitHubCopilot {
                if let Some(app_handle) = &self.app_handle {
                    let copilot_state = app_handle.state::<CopilotAuthState>();
                    let copilot_auth: tokio::sync::RwLockReadGuard<'_, CopilotAuthManager> =
                        copilot_state.0.read().await;

                    // 从 provider.meta 获取关联的 GitHub 账号 ID（多账号支持）
                    let account_id = provider
                        .meta
                        .as_ref()
                        .and_then(|m| m.managed_account_id_for("github_copilot"));

                    // 根据账号 ID 获取对应 token（向后兼容：无账号 ID 时使用第一个账号）
                    let token_result = match &account_id {
                        Some(id) => {
                            log::debug!("[Copilot] 使用指定账号 {id} 获取 token");
                            copilot_auth.get_valid_token_for_account(id).await
                        }
                        None => {
                            log::debug!("[Copilot] 使用默认账号获取 token");
                            copilot_auth.get_valid_token().await
                        }
                    };

                    match token_result {
                        Ok(token) => {
                            auth = AuthInfo::new(token, AuthStrategy::GitHubCopilot);
                            log::debug!(
                                "[Copilot] 成功获取 Copilot token (account={})",
                                account_id.as_deref().unwrap_or("default")
                            );
                        }
                        Err(e) => {
                            log::error!(
                                "[Copilot] 获取 Copilot token 失败 (account={}): {e}",
                                account_id.as_deref().unwrap_or("default")
                            );
                            return Err(ProxyError::AuthError(format!(
                                "GitHub Copilot 认证失败: {e}"
                            )));
                        }
                    }
                } else {
                    log::error!("[Copilot] AppHandle 不可用");
                    return Err(ProxyError::AuthError(
                        "GitHub Copilot 认证不可用（无 AppHandle）".to_string(),
                    ));
                }
            }
            adapter.get_auth_headers(&auth)
        } else {
            Vec::new()
        };

        // --- Copilot 优化器：动态 header 注入 ---
        if let Some((ref classification, ref det_request_id)) = copilot_optimization {
            for (name, value) in auth_headers.iter_mut() {
                match name.as_str() {
                    "x-initiator" if self.copilot_optimizer_config.request_classification => {
                        *value = http::HeaderValue::from_static(classification.initiator);
                    }
                    "x-request-id" | "x-agent-task-id" => {
                        if let Some(ref det_id) = det_request_id {
                            if let Ok(hv) = http::HeaderValue::from_str(det_id) {
                                *value = hv;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Copilot 指纹头名（由 get_auth_headers 注入，需在原始头中去重）
        let copilot_fingerprint_headers: &[&str] = if is_copilot {
            &[
                "user-agent",
                "editor-version",
                "editor-plugin-version",
                "copilot-integration-id",
                "x-github-api-version",
                "openai-intent",
                // 新增 headers
                "x-initiator",
                "x-interaction-type",
                "x-vscode-user-agent-library-version",
                "x-request-id",
                "x-agent-task-id",
            ]
        } else {
            &[]
        };

        // 预计算上游 host 值（用于在原位替换 host header）
        let upstream_host = url
            .parse::<http::Uri>()
            .ok()
            .and_then(|u| u.authority().map(|a| a.to_string()));

        // 预计算 anthropic-beta 值（仅 Claude）
        let anthropic_beta_value = if adapter.name() == "Claude" {
            const CLAUDE_CODE_BETA: &str = "claude-code-20250219";
            Some(if let Some(beta) = headers.get("anthropic-beta") {
                if let Ok(beta_str) = beta.to_str() {
                    if beta_str.contains(CLAUDE_CODE_BETA) {
                        beta_str.to_string()
                    } else {
                        format!("{CLAUDE_CODE_BETA},{beta_str}")
                    }
                } else {
                    CLAUDE_CODE_BETA.to_string()
                }
            } else {
                CLAUDE_CODE_BETA.to_string()
            })
        } else {
            None
        };

        // ============================================================
        // 构建有序 HeaderMap — 内联替换，保持客户端原始顺序
        // ============================================================
        let mut ordered_headers = http::HeaderMap::new();
        let mut saw_auth = false;
        let mut saw_accept_encoding = false;
        let mut saw_anthropic_beta = false;
        let mut saw_anthropic_version = false;

        for (key, value) in headers {
            let key_str = key.as_str();

            // --- host — 原位替换为上游 host（保持客户端原始位置） ---
            if key_str.eq_ignore_ascii_case("host") {
                if let Some(ref host_val) = upstream_host {
                    if let Ok(hv) = http::HeaderValue::from_str(host_val) {
                        ordered_headers.append(key.clone(), hv);
                    }
                }
                continue;
            }

            // --- 连接 / 追踪 / CDN 类 — 无条件跳过 ---
            if matches!(
                key_str,
                "content-length"
                    | "transfer-encoding"
                    | "x-forwarded-host"
                    | "x-forwarded-port"
                    | "x-forwarded-proto"
                    | "forwarded"
                    | "cf-connecting-ip"
                    | "cf-ipcountry"
                    | "cf-ray"
                    | "cf-visitor"
                    | "true-client-ip"
                    | "fastly-client-ip"
                    | "x-azure-clientip"
                    | "x-azure-fdid"
                    | "x-azure-ref"
                    | "akamai-origin-hop"
                    | "x-akamai-config-log-detail"
                    | "x-request-id"
                    | "x-correlation-id"
                    | "x-trace-id"
                    | "x-amzn-trace-id"
                    | "x-b3-traceid"
                    | "x-b3-spanid"
                    | "x-b3-parentspanid"
                    | "x-b3-sampled"
                    | "traceparent"
                    | "tracestate"
            ) {
                continue;
            }

            // --- 认证类 — 用 adapter 提供的认证头替换（在原始位置） ---
            if key_str.eq_ignore_ascii_case("authorization")
                || key_str.eq_ignore_ascii_case("x-api-key")
                || key_str.eq_ignore_ascii_case("x-goog-api-key")
            {
                if !saw_auth {
                    saw_auth = true;
                    for (ah_name, ah_value) in &auth_headers {
                        ordered_headers.append(ah_name.clone(), ah_value.clone());
                    }
                }
                continue;
            }

            // --- accept-encoding — transform / SSE 路径强制 identity，其余保留原值 ---
            if key_str.eq_ignore_ascii_case("accept-encoding") {
                if !saw_accept_encoding {
                    saw_accept_encoding = true;
                    if force_identity_encoding {
                        ordered_headers.append(
                            http::header::ACCEPT_ENCODING,
                            http::HeaderValue::from_static("identity"),
                        );
                    } else {
                        ordered_headers.append(key.clone(), value.clone());
                    }
                }
                continue;
            }

            // --- anthropic-beta — 用重建值替换（确保含 claude-code 标记） ---
            if key_str.eq_ignore_ascii_case("anthropic-beta") {
                if !saw_anthropic_beta {
                    saw_anthropic_beta = true;
                    if let Some(ref beta_val) = anthropic_beta_value {
                        if let Ok(hv) = http::HeaderValue::from_str(beta_val) {
                            ordered_headers.append("anthropic-beta", hv);
                        }
                    }
                }
                continue;
            }

            // --- anthropic-version — 透传客户端值 ---
            if key_str.eq_ignore_ascii_case("anthropic-version") {
                saw_anthropic_version = true;
                ordered_headers.append(key.clone(), value.clone());
                continue;
            }

            // --- Copilot 指纹头 — 跳过（由 auth_headers 提供） ---
            if copilot_fingerprint_headers
                .iter()
                .any(|h| key_str.eq_ignore_ascii_case(h))
            {
                continue;
            }

            // --- 默认：透传 ---
            ordered_headers.append(key.clone(), value.clone());
        }

        // 如果原始请求中没有认证头，在末尾追加
        if !saw_auth && !auth_headers.is_empty() {
            for (ah_name, ah_value) in &auth_headers {
                ordered_headers.append(ah_name.clone(), ah_value.clone());
            }
        }

        // transform / SSE 路径在缺失时补 identity；普通透传不主动补 accept-encoding
        if !saw_accept_encoding && force_identity_encoding {
            ordered_headers.append(
                http::header::ACCEPT_ENCODING,
                http::HeaderValue::from_static("identity"),
            );
        }

        // 如果原始请求中没有 anthropic-beta 且有值需要添加，追加
        if !saw_anthropic_beta {
            if let Some(ref beta_val) = anthropic_beta_value {
                if let Ok(hv) = http::HeaderValue::from_str(beta_val) {
                    ordered_headers.append("anthropic-beta", hv);
                }
            }
        }

        // anthropic-version：仅在缺失时补充默认值
        if adapter.name() == "Claude" && !saw_anthropic_version {
            ordered_headers.append(
                "anthropic-version",
                http::HeaderValue::from_static("2023-06-01"),
            );
        }

        // 序列化请求体
        let body_bytes = serde_json::to_vec(&filtered_body)
            .map_err(|e| ProxyError::Internal(format!("Failed to serialize request body: {e}")))?;

        // 确保 content-type 存在
        if !ordered_headers.contains_key(http::header::CONTENT_TYPE) {
            ordered_headers.insert(
                http::header::CONTENT_TYPE,
                http::HeaderValue::from_static("application/json"),
            );
        }

        // 输出请求信息日志
        let tag = adapter.name();
        let request_model = filtered_body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("<none>");
        log::info!("[{tag}] >>> 请求 URL: {url} (model={request_model})");
        if let Ok(body_str) = serde_json::to_string(&filtered_body) {
            log::debug!(
                "[{tag}] >>> 请求体内容 ({}字节): {}",
                body_str.len(),
                body_str
            );
        }

        // 确定超时
        let timeout = if self.non_streaming_timeout.is_zero() {
            std::time::Duration::from_secs(600) // 默认 600 秒
        } else {
            self.non_streaming_timeout
        };

        // 解析上游代理 URL（供应商单独代理 > 全局代理 > 无）
        let proxy_config = provider.meta.as_ref().and_then(|m| m.proxy_config.as_ref());
        let upstream_proxy_url: Option<String> = proxy_config
            .filter(|c| c.enabled)
            .and_then(super::http_client::build_proxy_url_from_config)
            .or_else(super::http_client::get_current_proxy_url);

        // SOCKS5 代理不支持 CONNECT 隧道，需要用 reqwest
        let is_socks_proxy = upstream_proxy_url
            .as_deref()
            .map(|u| u.starts_with("socks5"))
            .unwrap_or(false);

        let uri: http::Uri = url
            .parse()
            .map_err(|e| ProxyError::ForwardFailed(format!("Invalid URL '{url}': {e}")))?;

        // 发送请求
        let response = if is_socks_proxy {
            // SOCKS5 代理：只能走 reqwest（不支持 header case 保留）
            log::debug!("[Forwarder] Using reqwest for SOCKS5 proxy");
            let client = super::http_client::get_for_provider(proxy_config);
            let mut request = client.post(&url);
            if !self.non_streaming_timeout.is_zero() {
                request = request.timeout(self.non_streaming_timeout);
            }
            for (key, value) in &ordered_headers {
                request = request.header(key, value);
            }
            let reqwest_resp = request.body(body_bytes).send().await.map_err(|e| {
                if e.is_timeout() {
                    ProxyError::Timeout(format!("请求超时: {e}"))
                } else if e.is_connect() {
                    ProxyError::ForwardFailed(format!("连接失败: {e}"))
                } else {
                    ProxyError::ForwardFailed(e.to_string())
                }
            })?;
            ProxyResponse::Reqwest(reqwest_resp)
        } else {
            // HTTP 代理或直连：走 hyper raw write（保持 header 大小写）
            // 如果有 HTTP 代理，hyper_client 会用 CONNECT 隧道穿过代理
            super::hyper_client::send_request(
                uri,
                http::Method::POST,
                ordered_headers,
                extensions.clone(),
                body_bytes,
                timeout,
                upstream_proxy_url.as_deref(),
            )
            .await?
        };

        // 检查响应状态
        let status = response.status();

        if status.is_success() {
            Ok((response, resolved_claude_api_format))
        } else {
            let status_code = status.as_u16();
            let body_text = String::from_utf8(response.bytes().await?.to_vec()).ok();

            Err(ProxyError::UpstreamError {
                status: status_code,
                body: body_text,
            })
        }
    }

    async fn validate_semantic_success(
        &self,
        app_type: &AppType,
        app_type_str: &str,
        provider_id: &str,
        response: ProxyResponse,
        attempt_started_at: std::time::Instant,
    ) -> Result<SemanticValidationOutcome, ProxyError> {
        if !matches!(app_type, AppType::Codex) {
            self.router
                .reset_semantic_failure_streak(provider_id, app_type_str)
                .await;
            return Ok(SemanticValidationOutcome::Success(
                response.with_timing(ResponseTiming {
                    upstream_started_at: Some(attempt_started_at),
                    ..ResponseTiming::default()
                }),
            ));
        }

        if response.is_sse() {
            self.validate_codex_streaming_response(
                app_type_str,
                provider_id,
                response,
                attempt_started_at,
            )
                .await
        } else {
            self.validate_codex_non_streaming_response(
                app_type_str,
                provider_id,
                response,
                attempt_started_at,
            )
                .await
        }
    }

    async fn validate_codex_non_streaming_response(
        &self,
        app_type_str: &str,
        provider_id: &str,
        response: ProxyResponse,
        attempt_started_at: std::time::Instant,
    ) -> Result<SemanticValidationOutcome, ProxyError> {
        let (headers, status, body_bytes) =
            read_decoded_body(response, "CodexSemantic", self.non_streaming_timeout).await?;
        let timing = ResponseTiming {
            upstream_started_at: Some(attempt_started_at),
            upstream_duration_ms: Some(attempt_started_at.elapsed().as_millis() as u64),
            ..ResponseTiming::default()
        };

        let json_body = serde_json::from_slice::<Value>(&body_bytes).map_err(|_| {
            ProxyError::SemanticFailure("上游返回 200，但响应不是有效 JSON".to_string())
        })?;

        if is_meaningful_codex_non_streaming_body(&json_body) {
            self.router
                .reset_semantic_failure_streak(provider_id, app_type_str)
                .await;
            Ok(SemanticValidationOutcome::Success(
                ProxyResponse::from_buffered(status, headers, body_bytes).with_timing(timing),
            ))
        } else {
            let streak = self
                .router
                .record_semantic_failure(provider_id, app_type_str)
                .await;
            let base_message = "上游返回 200，但响应缺少有效输出，usage 全 0 或缺失";

            if streak < CODEX_SEMANTIC_FAILURE_THRESHOLD {
                let note = format!("语义异常（{streak}/{CODEX_SEMANTIC_FAILURE_THRESHOLD}）：{base_message}");
                Ok(SemanticValidationOutcome::ToleratedAnomaly {
                    response: ProxyResponse::from_buffered(status, headers, body_bytes)
                        .with_timing(timing)
                        .with_semantic_note(note.clone()),
                    note,
                })
            } else {
                Err(ProxyError::SemanticFailure(format!(
                    "语义异常连续 {streak}/{CODEX_SEMANTIC_FAILURE_THRESHOLD} 次，判定 Provider 失败并切换：{base_message}"
                )))
            }
        }
    }

    async fn validate_codex_streaming_response(
        &self,
        app_type_str: &str,
        provider_id: &str,
        response: ProxyResponse,
        attempt_started_at: std::time::Instant,
    ) -> Result<SemanticValidationOutcome, ProxyError> {
        let status = response.status();
        let headers = response.headers().clone();
        let mut stream = response.bytes_stream();
        let mut buffered_chunks = Vec::new();
        let mut sse_buffer = String::new();
        let mut is_first_chunk = true;
        let mut first_upstream_event_ms = None;
        loop {
            let timeout_duration = if is_first_chunk {
                self.streaming_first_byte_timeout
            } else {
                self.streaming_idle_timeout
            };

            let next_chunk = if timeout_duration.is_zero() {
                stream.next().await
            } else {
                tokio::time::timeout(timeout_duration, stream.next())
                    .await
                    .map_err(|_| {
                        if is_first_chunk {
                            ProxyError::Timeout(format!(
                                "流式响应首字节超时: {}秒",
                                timeout_duration.as_secs()
                            ))
                        } else {
                            ProxyError::StreamIdleTimeout(timeout_duration.as_secs())
                        }
                    })?
            };

            match next_chunk {
                Some(Ok(bytes)) => {
                    is_first_chunk = false;
                    if bytes.is_empty() {
                        continue;
                    }

                    sse_buffer.push_str(&String::from_utf8_lossy(&bytes));
                    buffered_chunks.push(bytes);

                    let events = drain_sse_json_events(&mut sse_buffer);
                    if first_upstream_event_ms.is_none() && !events.is_empty() {
                        first_upstream_event_ms =
                            Some(attempt_started_at.elapsed().as_millis() as u64);
                    }
                    if events.iter().any(is_meaningful_codex_stream_event) {
                        self.router
                            .reset_semantic_failure_streak(provider_id, app_type_str)
                            .await;
                        let first_token_ms = first_upstream_event_ms
                            .unwrap_or_else(|| attempt_started_at.elapsed().as_millis() as u64);
                        return Ok(SemanticValidationOutcome::Success(
                            ProxyResponse::from_stream(
                                status,
                                headers,
                                prepend_buffered_stream(buffered_chunks, stream),
                            )
                            .with_timing(ResponseTiming {
                                upstream_started_at: Some(attempt_started_at),
                                upstream_first_token_ms: Some(first_token_ms),
                                upstream_first_token_known: true,
                                upstream_duration_ms: None,
                            }),
                        ));
                    }
                }
                Some(Err(e)) => {
                    return Err(ProxyError::ForwardFailed(format!(
                        "流式响应读取失败: {e}"
                    )));
                }
                None => break,
            }
        }

        if !sse_buffer.trim().is_empty() {
            sse_buffer.push_str("\n\n");
            let events = drain_sse_json_events(&mut sse_buffer);
            if first_upstream_event_ms.is_none() && !events.is_empty() {
                first_upstream_event_ms = Some(attempt_started_at.elapsed().as_millis() as u64);
            }
            if events.iter().any(is_meaningful_codex_stream_event) {
                self.router
                    .reset_semantic_failure_streak(provider_id, app_type_str)
                    .await;
                let elapsed_ms = attempt_started_at.elapsed().as_millis() as u64;
                let first_token_ms = first_upstream_event_ms.unwrap_or(elapsed_ms);
                let response =
                    ProxyResponse::from_stream(status, headers, buffered_only_stream(buffered_chunks))
                        .with_timing(ResponseTiming {
                            upstream_started_at: Some(attempt_started_at),
                            upstream_first_token_ms: Some(first_token_ms),
                            upstream_first_token_known: true,
                            upstream_duration_ms: Some(elapsed_ms),
                        });
                return Ok(SemanticValidationOutcome::Success(response));
            }
        }

        let streak = self
            .router
            .record_semantic_failure(provider_id, app_type_str)
            .await;
        let base_message = "上游流式响应未产出有效内容，usage 全 0 或缺失";

        if streak < CODEX_SEMANTIC_FAILURE_THRESHOLD {
            let note = format!("语义异常（{streak}/{CODEX_SEMANTIC_FAILURE_THRESHOLD}）：{base_message}");
            let elapsed_ms = attempt_started_at.elapsed().as_millis() as u64;
            let response =
                ProxyResponse::from_stream(status, headers, buffered_only_stream(buffered_chunks))
                    .with_timing(ResponseTiming {
                        upstream_started_at: Some(attempt_started_at),
                        upstream_first_token_ms: first_upstream_event_ms,
                        upstream_first_token_known: true,
                        upstream_duration_ms: Some(elapsed_ms),
                    });
            Ok(SemanticValidationOutcome::ToleratedAnomaly {
                response: response.with_semantic_note(note.clone()),
                note,
            })
        } else {
            Err(ProxyError::SemanticFailure(format!(
                "语义异常连续 {streak}/{CODEX_SEMANTIC_FAILURE_THRESHOLD} 次，判定 Provider 失败并切换：{base_message}"
            )))
        }
    }

    async fn resolve_claude_api_format(
        &self,
        provider: &Provider,
        body: &Value,
        is_copilot: bool,
    ) -> String {
        if !is_copilot {
            return super::providers::get_claude_api_format(provider).to_string();
        }

        let model = body.get("model").and_then(|value| value.as_str());
        if let Some(model_id) = model {
            if self
                .is_copilot_openai_vendor_model(provider, model_id)
                .await
            {
                return "openai_responses".to_string();
            }
        }

        "openai_chat".to_string()
    }

    async fn is_copilot_openai_vendor_model(&self, provider: &Provider, model_id: &str) -> bool {
        let Some(app_handle) = &self.app_handle else {
            log::debug!("[Copilot] AppHandle unavailable, fallback to chat/completions");
            return false;
        };

        let copilot_state = app_handle.state::<CopilotAuthState>();
        let copilot_auth = copilot_state.0.read().await;
        let account_id = provider
            .meta
            .as_ref()
            .and_then(|m| m.managed_account_id_for("github_copilot"));

        let vendor_result = match account_id.as_deref() {
            Some(id) => {
                copilot_auth
                    .get_model_vendor_for_account(id, model_id)
                    .await
            }
            None => copilot_auth.get_model_vendor(model_id).await,
        };

        match vendor_result {
            Ok(Some(vendor)) => vendor.eq_ignore_ascii_case("openai"),
            Ok(None) => {
                log::debug!(
                    "[Copilot] Model vendor unavailable for {model_id}, fallback to chat/completions"
                );
                false
            }
            Err(err) => {
                log::warn!(
                    "[Copilot] Failed to resolve model vendor for {model_id}, fallback to chat/completions: {err}"
                );
                false
            }
        }
    }

    fn categorize_proxy_error(&self, error: &ProxyError) -> ErrorCategory {
        match error {
            // 网络和上游错误：都应该尝试下一个供应商
            ProxyError::Timeout(_) => ErrorCategory::Retryable,
            ProxyError::ForwardFailed(_) => ErrorCategory::Retryable,
            ProxyError::SemanticFailure(_) => ErrorCategory::Retryable,
            ProxyError::ProviderUnhealthy(_) => ErrorCategory::Retryable,
            // 上游 HTTP 错误：无论状态码如何，都尝试下一个供应商
            // 原因：不同供应商有不同的限制和认证，一个供应商的 4xx 错误
            // 不代表其他供应商也会失败
            ProxyError::UpstreamError { .. } => ErrorCategory::Retryable,
            // Provider 级配置/转换问题：换一个 Provider 可能就能成功
            ProxyError::ConfigError(_) => ErrorCategory::Retryable,
            ProxyError::TransformError(_) => ErrorCategory::Retryable,
            ProxyError::AuthError(_) => ErrorCategory::Retryable,
            ProxyError::StreamIdleTimeout(_) => ErrorCategory::Retryable,
            // 无可用供应商：所有供应商都试过了，无法重试
            ProxyError::NoAvailableProvider => ErrorCategory::NonRetryable,
            // 其他错误（数据库/内部错误等）：不是换供应商能解决的问题
            _ => ErrorCategory::NonRetryable,
        }
    }
}

fn prepend_buffered_stream(buffered_chunks: Vec<Bytes>, mut stream: ByteStream) -> ByteStream {
    Box::pin(async_stream::stream! {
        for chunk in buffered_chunks {
            yield Ok(chunk);
        }

        while let Some(chunk) = stream.next().await {
            yield chunk;
        }
    })
}

fn buffered_only_stream(buffered_chunks: Vec<Bytes>) -> ByteStream {
    Box::pin(async_stream::stream! {
        for chunk in buffered_chunks {
            yield Ok(chunk);
        }
    })
}

fn drain_sse_json_events(buffer: &mut String) -> Vec<Value> {
    let mut events = Vec::new();

    while let Some(pos) = buffer.find("\n\n") {
        let event_text = buffer[..pos].to_string();
        *buffer = buffer[pos + 2..].to_string();

        if event_text.trim().is_empty() {
            continue;
        }

        for line in event_text.lines() {
            let Some(data) = strip_sse_field(line, "data") else {
                continue;
            };

            if data.trim() == "[DONE]" {
                continue;
            }

            if let Ok(json_value) = serde_json::from_str::<Value>(data) {
                events.push(json_value);
            }
        }
    }

    events
}

fn is_meaningful_codex_stream_event(event: &Value) -> bool {
    if let Some(event_type) = event.get("type").and_then(|value| value.as_str()) {
        match event_type {
            "response.completed" => {
                return event
                    .get("response")
                    .map(is_meaningful_codex_non_streaming_body)
                    .unwrap_or(false);
            }
            "response.reasoning.delta" => {
                return event
                    .get("delta")
                    .and_then(|value| value.as_str())
                    .map(has_non_empty_text)
                    .unwrap_or(false);
            }
            "response.output_text.delta" => {
                return event
                    .get("delta")
                    .and_then(|value| value.as_str())
                    .map(has_non_empty_text)
                    .unwrap_or(false);
            }
            "response.content_part.added" => {
                return event
                    .get("part")
                    .and_then(|part| part.get("text"))
                    .and_then(|value| value.as_str())
                    .map(has_non_empty_text)
                    .unwrap_or(false);
            }
            "response.output_item.added" => {
                return event
                    .get("item")
                    .and_then(|item| item.get("type"))
                    .and_then(|value| value.as_str())
                    .map(|item_type| item_type == "function_call")
                    .unwrap_or(false);
            }
            "response.function_call_arguments.delta" => {
                return event
                    .get("delta")
                    .and_then(|value| value.as_str())
                    .map(has_non_empty_text)
                    .unwrap_or(false);
            }
            _ => {}
        }
    }

    if let Some(choices) = event.get("choices").and_then(|value| value.as_array()) {
        return choices.iter().any(is_meaningful_openai_choice);
    }

    false
}

fn is_meaningful_codex_non_streaming_body(body: &Value) -> bool {
    if TokenUsage::from_codex_response_auto(body)
        .as_ref()
        .map(has_non_zero_usage)
        .unwrap_or(false)
    {
        return true;
    }

    if body.get("usage").map(has_non_zero_usage_value).unwrap_or(false) {
        return true;
    }

    if body
        .get("output_text")
        .and_then(|value| value.as_str())
        .map(has_non_empty_text)
        .unwrap_or(false)
    {
        return true;
    }

    if body
        .get("output")
        .and_then(|value| value.as_array())
        .map(|items| items.iter().any(is_meaningful_response_output_item))
        .unwrap_or(false)
    {
        return true;
    }

    body.get("choices")
        .and_then(|value| value.as_array())
        .map(|choices| choices.iter().any(is_meaningful_openai_choice))
        .unwrap_or(false)
}

fn is_meaningful_response_output_item(item: &Value) -> bool {
    match item.get("type").and_then(|value| value.as_str()) {
        Some("message") => item
            .get("content")
            .and_then(|value| value.as_array())
            .map(|content| content.iter().any(is_meaningful_message_content_block))
            .unwrap_or(false),
        Some("function_call") | Some("function_call_output") => true,
        Some("reasoning") => false,
        _ => false,
    }
}

fn is_meaningful_message_content_block(block: &Value) -> bool {
    match block.get("type").and_then(|value| value.as_str()) {
        Some("output_text") | Some("input_text") | Some("text") => block
            .get("text")
            .and_then(|value| value.as_str())
            .map(has_non_empty_text)
            .unwrap_or(false),
        Some("refusal") => block
            .get("refusal")
            .and_then(|value| value.as_str())
            .map(has_non_empty_text)
            .unwrap_or(false),
        _ => false,
    }
}

fn is_meaningful_openai_choice(choice: &Value) -> bool {
    if choice
        .pointer("/delta/content")
        .and_then(|value| value.as_str())
        .map(has_non_empty_text)
        .unwrap_or(false)
    {
        return true;
    }

    if choice
        .pointer("/message/content")
        .map(value_has_non_empty_text)
        .unwrap_or(false)
    {
        return true;
    }

    choice
        .pointer("/delta/tool_calls")
        .and_then(|value| value.as_array())
        .map(|tool_calls| !tool_calls.is_empty())
        .unwrap_or(false)
        || choice
            .pointer("/message/tool_calls")
            .and_then(|value| value.as_array())
            .map(|tool_calls| !tool_calls.is_empty())
            .unwrap_or(false)
}

fn value_has_non_empty_text(value: &Value) -> bool {
    match value {
        Value::String(text) => has_non_empty_text(text),
        Value::Array(items) => items.iter().any(|item| {
            item.get("text")
                .and_then(|value| value.as_str())
                .map(has_non_empty_text)
                .unwrap_or(false)
                || item
                    .get("refusal")
                    .and_then(|value| value.as_str())
                    .map(has_non_empty_text)
                    .unwrap_or(false)
        }),
        _ => false,
    }
}

fn has_non_zero_usage(usage: &TokenUsage) -> bool {
    usage.input_tokens > 0
        || usage.output_tokens > 0
        || usage.cache_read_tokens > 0
        || usage.cache_creation_tokens > 0
}

fn has_non_zero_usage_value(usage: &Value) -> bool {
    let keys = [
        "input_tokens",
        "output_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
    ];

    if keys
        .iter()
        .any(|key| usage.get(*key).and_then(|value| value.as_u64()).unwrap_or(0) > 0)
    {
        return true;
    }

    usage.get("input_tokens_details")
        .and_then(|value| value.get("cached_tokens"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0)
        > 0
        || usage
            .get("prompt_tokens_details")
            .and_then(|value| value.get("cached_tokens"))
            .and_then(|value| value.as_u64())
            .unwrap_or(0)
            > 0
}

fn has_non_empty_text(text: &str) -> bool {
    !text.is_empty()
}

/// 从 ProxyError 中提取错误消息
fn extract_error_message(error: &ProxyError) -> Option<String> {
    match error {
        ProxyError::UpstreamError { body, .. } => body.clone(),
        _ => Some(error.to_string()),
    }
}

/// 检测 Provider 是否为 Bedrock（通过 CLAUDE_CODE_USE_BEDROCK 环境变量判断）
fn is_bedrock_provider(provider: &Provider) -> bool {
    provider
        .settings_config
        .get("env")
        .and_then(|e| e.get("CLAUDE_CODE_USE_BEDROCK"))
        .and_then(|v| v.as_str())
        .map(|v| v == "1")
        .unwrap_or(false)
}

fn build_retryable_failure_log(
    provider_name: &str,
    attempted_providers: usize,
    total_providers: usize,
    error: &ProxyError,
) -> (&'static str, String) {
    let error_summary = summarize_proxy_error(error);

    if total_providers <= 1 {
        (
            log_fwd::SINGLE_PROVIDER_FAILED,
            format!("Provider {provider_name} 请求失败: {error_summary}"),
        )
    } else {
        (
            log_fwd::PROVIDER_FAILED_RETRY,
            format!(
                "Provider {provider_name} 失败，继续尝试下一个 ({attempted_providers}/{total_providers}): {error_summary}"
            ),
        )
    }
}

fn build_terminal_failure_log(
    attempted_providers: usize,
    total_providers: usize,
    last_error: Option<&ProxyError>,
) -> Option<(&'static str, String)> {
    if total_providers <= 1 {
        return None;
    }

    let error_summary = last_error
        .map(summarize_proxy_error)
        .unwrap_or_else(|| "未知错误".to_string());

    Some((
        log_fwd::ALL_PROVIDERS_FAILED,
        format!(
            "已尝试 {attempted_providers}/{total_providers} 个 Provider，均失败。最后错误: {error_summary}"
        ),
    ))
}

fn summarize_proxy_error(error: &ProxyError) -> String {
    match error {
        ProxyError::UpstreamError { status, body } => {
            let body_summary = body
                .as_deref()
                .map(summarize_upstream_body)
                .filter(|summary| !summary.is_empty());

            match body_summary {
                Some(summary) => format!("上游 HTTP {status}: {summary}"),
                None => format!("上游 HTTP {status}"),
            }
        }
        ProxyError::Timeout(message) => {
            format!("请求超时: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::ForwardFailed(message) => {
            format!("请求转发失败: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::SemanticFailure(message) => {
            format!("语义失败: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::TransformError(message) => {
            format!("响应转换失败: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::ConfigError(message) => {
            format!("配置错误: {}", summarize_text_for_log(message, 180))
        }
        ProxyError::AuthError(message) => {
            format!("认证失败: {}", summarize_text_for_log(message, 180))
        }
        _ => summarize_text_for_log(&error.to_string(), 180),
    }
}

fn summarize_upstream_body(body: &str) -> String {
    if let Ok(json_body) = serde_json::from_str::<Value>(body) {
        if let Some(message) = extract_json_error_message(&json_body) {
            return summarize_text_for_log(&message, 180);
        }

        if let Ok(compact_json) = serde_json::to_string(&json_body) {
            return summarize_text_for_log(&compact_json, 180);
        }
    }

    summarize_text_for_log(body, 180)
}

fn extract_json_error_message(body: &Value) -> Option<String> {
    let candidates = [
        body.pointer("/error/message"),
        body.pointer("/message"),
        body.pointer("/detail"),
        body.pointer("/error"),
    ];

    candidates
        .into_iter()
        .flatten()
        .find_map(|value| value.as_str().map(ToString::to_string))
}

fn split_endpoint_and_query(endpoint: &str) -> (&str, Option<&str>) {
    endpoint
        .split_once('?')
        .map_or((endpoint, None), |(path, query)| (path, Some(query)))
}

fn strip_beta_query(query: Option<&str>) -> Option<String> {
    let filtered = query.map(|query| {
        query
            .split('&')
            .filter(|pair| !pair.is_empty() && !pair.starts_with("beta="))
            .collect::<Vec<_>>()
            .join("&")
    });

    match filtered.as_deref() {
        Some("") | None => None,
        Some(_) => filtered,
    }
}

fn is_claude_messages_path(path: &str) -> bool {
    matches!(path, "/v1/messages" | "/claude/v1/messages")
}

fn rewrite_claude_transform_endpoint(
    endpoint: &str,
    api_format: &str,
    is_copilot: bool,
) -> (String, Option<String>) {
    let (path, query) = split_endpoint_and_query(endpoint);
    let passthrough_query = if is_claude_messages_path(path) {
        strip_beta_query(query)
    } else {
        query.map(ToString::to_string)
    };

    if !is_claude_messages_path(path) {
        return (endpoint.to_string(), passthrough_query);
    }

    let target_path = if is_copilot && api_format == "openai_responses" {
        "/v1/responses"
    } else if is_copilot {
        "/chat/completions"
    } else if api_format == "openai_responses" {
        "/v1/responses"
    } else {
        "/v1/chat/completions"
    };

    let rewritten = match passthrough_query.as_deref() {
        Some(query) if !query.is_empty() => format!("{target_path}?{query}"),
        _ => target_path.to_string(),
    };

    (rewritten, passthrough_query)
}

fn append_query_to_full_url(base_url: &str, query: Option<&str>) -> String {
    match query {
        Some(query) if !query.is_empty() => {
            if base_url.contains('?') {
                format!("{base_url}&{query}")
            } else {
                format!("{base_url}?{query}")
            }
        }
        _ => base_url.to_string(),
    }
}

fn should_force_identity_encoding(
    endpoint: &str,
    body: &Value,
    headers: &axum::http::HeaderMap,
) -> bool {
    if body
        .get("stream")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return true;
    }

    if endpoint.contains("streamGenerateContent") || endpoint.contains("alt=sse") {
        return true;
    }

    headers
        .get(axum::http::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|accept| accept.contains("text/event-stream"))
        .unwrap_or(false)
}

fn summarize_text_for_log(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let trimmed = normalized.trim();

    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }

    let truncated: String = trimmed.chars().take(max_chars).collect();
    let truncated = truncated.trim_end();
    format!("{truncated}...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use crate::database::Database;
    use crate::proxy::circuit_breaker::CircuitBreakerConfig;
    use crate::proxy::failover_switch::FailoverSwitchManager;
    use crate::proxy::provider_router::ProviderRouter;
    use crate::proxy::types::ProxyStatus;
    use axum::body::Body;
    use axum::http::header::{HeaderValue, ACCEPT};
    use axum::http::HeaderMap;
    use axum::response::Response;
    use axum::{routing::post, Json, Router};
    use bytes::Bytes;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::RwLock;
    use tokio::time::{sleep, Duration};

    fn build_test_provider(id: &str, name: &str, base_url: &str) -> crate::provider::Provider {
        crate::provider::Provider {
            id: id.to_string(),
            name: name.to_string(),
            settings_config: json!({
                "base_url": base_url,
            }),
            website_url: None,
            category: Some("codex".to_string()),
            created_at: None,
            sort_index: None,
            notes: None,
            meta: None,
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        }
    }

    async fn start_test_server_for_path(
        path: &'static str,
        status: u16,
        body: serde_json::Value,
    ) -> String {
        let app = Router::new().route(
            path,
            post(move || {
                let body = body.clone();
                async move {
                    (
                        axum::http::StatusCode::from_u16(status).expect("valid status"),
                        Json(body),
                    )
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });
        format!("http://{}", addr)
    }

    async fn start_test_server(status: u16, body: serde_json::Value) -> String {
        start_test_server_for_path("/v1/responses", status, body).await
    }

    async fn start_test_sse_server_for_path(
        path: &'static str,
        status: u16,
        chunks: Vec<&'static str>,
    ) -> String {
        let app = Router::new().route(
            path,
            post(move || {
                let chunks = chunks.clone();
                async move {
                    let stream = futures::stream::iter(chunks.into_iter().map(|chunk| {
                        Ok::<Bytes, std::io::Error>(Bytes::from(chunk.to_string()))
                    }));

                    Response::builder()
                        .status(axum::http::StatusCode::from_u16(status).expect("valid status"))
                        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                        .body(Body::from_stream(stream))
                        .expect("build sse response")
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });
        format!("http://{}", addr)
    }

    async fn start_test_delayed_sse_server_for_path(
        path: &'static str,
        status: u16,
        chunks: Vec<(&'static str, u64)>,
    ) -> String {
        let app = Router::new().route(
            path,
            post(move || {
                let chunks = chunks.clone();
                async move {
                    let stream = async_stream::stream! {
                        for (chunk, delay_ms) in chunks {
                            if delay_ms > 0 {
                                sleep(Duration::from_millis(delay_ms)).await;
                            }
                            yield Ok::<Bytes, std::io::Error>(Bytes::from(chunk.to_string()));
                        }
                    };

                    Response::builder()
                        .status(axum::http::StatusCode::from_u16(status).expect("valid status"))
                        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
                        .body(Body::from_stream(stream))
                        .expect("build delayed sse response")
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("listener addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve delayed sse app");
        });
        format!("http://{}", addr)
    }

    #[test]
    fn single_provider_retryable_log_uses_single_provider_code() {
        let error = ProxyError::UpstreamError {
            status: 429,
            body: Some(r#"{"error":{"message":"rate limit exceeded"}}"#.to_string()),
        };

        let (code, message) = build_retryable_failure_log("PackyCode-response", 1, 1, &error);

        assert_eq!(code, log_fwd::SINGLE_PROVIDER_FAILED);
        assert!(message.contains("Provider PackyCode-response 请求失败"));
        assert!(message.contains("上游 HTTP 429"));
        assert!(message.contains("rate limit exceeded"));
        assert!(!message.contains("切换下一个"));
    }

    #[test]
    fn multi_provider_retryable_log_keeps_failover_wording() {
        let error = ProxyError::Timeout("upstream timed out after 30s".to_string());

        let (code, message) = build_retryable_failure_log("primary", 1, 3, &error);

        assert_eq!(code, log_fwd::PROVIDER_FAILED_RETRY);
        assert!(message.contains("继续尝试下一个 (1/3)"));
        assert!(message.contains("请求超时"));
    }

    #[test]
    fn single_provider_has_no_terminal_all_failed_log() {
        assert!(build_terminal_failure_log(1, 1, None).is_none());
    }

    #[test]
    fn multi_provider_terminal_log_contains_last_error_summary() {
        let error = ProxyError::ForwardFailed("connection reset by peer".to_string());

        let (code, message) =
            build_terminal_failure_log(2, 2, Some(&error)).expect("expected terminal log");

        assert_eq!(code, log_fwd::ALL_PROVIDERS_FAILED);
        assert!(message.contains("已尝试 2/2 个 Provider，均失败"));
        assert!(message.contains("connection reset by peer"));
    }

    #[test]
    fn summarize_upstream_body_prefers_json_message() {
        let body = json!({
            "error": {
                "message": "invalid_request_error: unsupported field"
            },
            "request_id": "req_123"
        });

        let summary = summarize_upstream_body(&body.to_string());

        assert_eq!(summary, "invalid_request_error: unsupported field");
    }

    #[test]
    fn summarize_text_for_log_collapses_whitespace_and_truncates() {
        let summary = summarize_text_for_log("line1\n\n line2   line3", 12);

        assert_eq!(summary, "line1 line2...");
    }

    #[test]
    fn rewrite_claude_transform_endpoint_strips_beta_for_chat_completions() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&foo=bar",
            "openai_chat",
            false,
        );

        assert_eq!(endpoint, "/v1/chat/completions?foo=bar");
        assert_eq!(passthrough_query.as_deref(), Some("foo=bar"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_strips_beta_for_responses() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/claude/v1/messages?beta=true&x-id=1",
            "openai_responses",
            false,
        );

        assert_eq!(endpoint, "/v1/responses?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_uses_copilot_path() {
        let (endpoint, passthrough_query) =
            rewrite_claude_transform_endpoint("/v1/messages?beta=true&x-id=1", "anthropic", true);

        assert_eq!(endpoint, "/chat/completions?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn rewrite_claude_transform_endpoint_uses_copilot_responses_path() {
        let (endpoint, passthrough_query) = rewrite_claude_transform_endpoint(
            "/v1/messages?beta=true&x-id=1",
            "openai_responses",
            true,
        );

        assert_eq!(endpoint, "/v1/responses?x-id=1");
        assert_eq!(passthrough_query.as_deref(), Some("x-id=1"));
    }

    #[test]
    fn append_query_to_full_url_preserves_existing_query_string() {
        let url = append_query_to_full_url("https://relay.example/api?foo=bar", Some("x-id=1"));

        assert_eq!(url, "https://relay.example/api?foo=bar&x-id=1");
    }

    #[test]
    fn force_identity_for_stream_flag_requests() {
        let headers = HeaderMap::new();

        assert!(should_force_identity_encoding(
            "/v1/responses",
            &json!({ "stream": true }),
            &headers
        ));
    }

    #[test]
    fn force_identity_for_gemini_stream_endpoints() {
        let headers = HeaderMap::new();

        assert!(should_force_identity_encoding(
            "/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
            &json!({ "model": "gemini-2.5-pro" }),
            &headers
        ));
    }

    #[test]
    fn force_identity_for_sse_accept_header() {
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));

        assert!(should_force_identity_encoding(
            "/v1/responses",
            &json!({ "model": "gpt-5" }),
            &headers
        ));
    }

    #[test]
    fn non_streaming_requests_allow_automatic_compression() {
        let headers = HeaderMap::new();

        assert!(!should_force_identity_encoding(
            "/v1/responses",
            &json!({ "model": "gpt-5" }),
            &headers
        ));
    }

    // ==================== Copilot 动态 endpoint 路由相关测试 ====================

    /// 验证 is_copilot 检测逻辑：通过 provider_type 判断
    #[test]
    fn copilot_detection_via_provider_type() {
        use crate::provider::{Provider, ProviderMeta};

        let provider = Provider {
            id: "test".to_string(),
            name: "Test Copilot".to_string(),
            settings_config: serde_json::json!({}),
            website_url: None,
            category: None,
            created_at: None,
            sort_index: None,
            notes: None,
            meta: Some(ProviderMeta {
                provider_type: Some("github_copilot".to_string()),
                ..Default::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        };

        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot");

        assert!(is_copilot, "应该通过 provider_type 检测为 Copilot");
    }

    /// 验证 is_copilot 检测逻辑：通过 base_url 判断
    #[test]
    fn copilot_detection_via_base_url() {
        let base_url = "https://api.githubcopilot.com";
        let is_copilot = base_url.contains("githubcopilot.com");
        assert!(is_copilot, "应该通过 base_url 检测为 Copilot");

        let non_copilot_url = "https://api.anthropic.com";
        let is_not_copilot = non_copilot_url.contains("githubcopilot.com");
        assert!(!is_not_copilot, "非 Copilot URL 不应被检测为 Copilot");
    }

    /// 验证企业版 endpoint（不包含 githubcopilot.com）场景下 is_copilot 仍然正确
    #[test]
    fn copilot_detection_for_enterprise_endpoint() {
        use crate::provider::{Provider, ProviderMeta};

        // 企业版场景：provider_type 是 github_copilot，但 base_url 可能是企业内部域名
        let provider = Provider {
            id: "enterprise".to_string(),
            name: "Enterprise Copilot".to_string(),
            settings_config: serde_json::json!({}),
            website_url: None,
            category: None,
            created_at: None,
            sort_index: None,
            notes: None,
            meta: Some(ProviderMeta {
                provider_type: Some("github_copilot".to_string()),
                ..Default::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        };

        let enterprise_base_url = "https://copilot-api.corp.example.com";

        // is_copilot 应该通过 provider_type 检测成功，即使 base_url 不包含 githubcopilot.com
        let is_copilot = provider
            .meta
            .as_ref()
            .and_then(|m| m.provider_type.as_deref())
            == Some("github_copilot")
            || enterprise_base_url.contains("githubcopilot.com");

        assert!(
            is_copilot,
            "企业版 Copilot 应该通过 provider_type 被正确检测"
        );
    }

    /// 验证动态 endpoint 替换条件
    #[test]
    fn dynamic_endpoint_replacement_conditions() {
        // 条件：is_copilot && !is_full_url
        let test_cases = [
            (true, false, true, "Copilot + 非 full_url 应该替换"),
            (true, true, false, "Copilot + full_url 不应替换"),
            (false, false, false, "非 Copilot 不应替换"),
            (false, true, false, "非 Copilot + full_url 不应替换"),
        ];

        for (is_copilot, is_full_url, should_replace, desc) in test_cases {
            let will_replace = is_copilot && !is_full_url;
            assert_eq!(will_replace, should_replace, "{desc}");
        }
    }

    #[tokio::test]
    async fn retryable_failover_attempts_should_be_persisted_to_request_logs() -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let failure_base_url = start_test_server(
            429,
            json!({
                "error": {
                    "message": "rate limit exceeded"
                }
            }),
        )
        .await;
        let success_base_url = start_test_server(
            200,
            json!({
                "id": "resp_ok",
                "model": "gpt-5",
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 1
                }
            }),
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            0,
            0,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let providers = vec![
            build_test_provider("provider-a", "Provider A", &failure_base_url),
            build_test_provider("provider-b", "Provider B", &success_base_url),
        ];

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello"
                }),
                HeaderMap::new(),
                Extensions::new(),
                providers,
            )
            .await;

        assert!(result.is_ok(), "second provider should succeed");

        let conn = crate::database::lock_conn!(db.conn);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'",
                [],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        assert_eq!(count, 1, "failed upstream attempt should be persisted");
        let (status_code, provider_id): (i64, String) = conn
            .query_row(
                "SELECT status_code, provider_id
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'
                 LIMIT 1",
                [],
                |row: &rusqlite::Row<'_>| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        assert_eq!(status_code, 429);
        assert_eq!(provider_id, "provider-a");
        Ok(())
    }

    #[tokio::test]
    async fn retryable_failover_attempt_logs_should_keep_codex_session_id() -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let failure_base_url = start_test_server(
            429,
            json!({
                "error": {
                    "message": "rate limit exceeded"
                }
            }),
        )
        .await;
        let success_base_url = start_test_server(
            200,
            json!({
                "id": "resp_ok",
                "model": "gpt-5",
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 1
                }
            }),
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            0,
            0,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let providers = vec![
            build_test_provider("provider-a", "Provider A", &failure_base_url),
            build_test_provider("provider-b", "Provider B", &success_base_url),
        ];

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello",
                    "previous_response_id": "resp_prev_123"
                }),
                HeaderMap::new(),
                Extensions::new(),
                providers,
            )
            .await;

        assert!(result.is_ok(), "second provider should succeed");

        let conn = crate::database::lock_conn!(db.conn);
        let session_id: String = conn
            .query_row(
                "SELECT session_id
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'
                 LIMIT 1",
                [],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        assert_eq!(session_id, "codex_resp_prev_123");
        Ok(())
    }

    #[tokio::test]
    async fn gemini_failover_attempt_logs_should_extract_model_from_endpoint() -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let failure_base_url = start_test_server_for_path(
            "/v1beta/models/gemini-2.5-pro:generateContent",
            429,
            json!({
                "error": {
                    "message": "quota exceeded"
                }
            }),
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            0,
            0,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let providers = vec![build_test_provider(
            "provider-a",
            "Provider A",
            &failure_base_url,
        )];

        let result = forwarder
            .forward_with_retry(
                &AppType::Gemini,
                "/v1beta/models/gemini-2.5-pro:generateContent",
                json!({
                    "contents": []
                }),
                HeaderMap::new(),
                Extensions::new(),
                providers,
            )
            .await;

        assert!(result.is_err(), "single provider should fail");

        let conn = crate::database::lock_conn!(db.conn);
        let model: String = conn
            .query_row(
                "SELECT model
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'
                 LIMIT 1",
                [],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;

        assert_eq!(model, "gemini-2.5-pro");
        Ok(())
    }

    #[tokio::test]
    async fn codex_non_streaming_semantic_empty_response_should_failover_on_third_hit() -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 60,
            ..Default::default()
        })
        .await
        .map_err(|e| AppError::Database(e.to_string()))?;

        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let empty_base_url = start_test_server(
            200,
            json!({
                "id": "resp_empty",
                "status": "completed",
                "model": "gpt-5",
                "output": [],
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "input_tokens_details": {
                        "cached_tokens": 0
                    }
                }
            }),
        )
        .await;
        let success_base_url = start_test_server(
            200,
            json!({
                "id": "resp_ok",
                "status": "completed",
                "model": "gpt-5",
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "Hello from provider B"
                    }]
                }],
                "usage": {
                    "input_tokens": 12,
                    "output_tokens": 6
                }
            }),
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            0,
            0,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let providers = vec![
            build_test_provider("provider-a", "Provider A", &empty_base_url),
            build_test_provider("provider-b", "Provider B", &success_base_url),
        ];

        for expected_streak in 1..=2 {
            let result = forwarder
                .forward_with_retry(
                    &AppType::Codex,
                    "/responses",
                    json!({
                        "model": "gpt-5",
                        "input": "hello"
                    }),
                    HeaderMap::new(),
                    Extensions::new(),
                    providers.clone(),
                )
                .await
                .map_err(|e| AppError::Message(format!("semantic anomaly should be tolerated before threshold: {}", e.error)))?;

            assert_eq!(result.provider.id, "provider-a");
            let semantic_note = result
                .response
                .semantic_note()
                .map(str::to_string)
                .ok_or_else(|| AppError::Message("expected semantic anomaly note".to_string()))?;
            assert!(
                semantic_note.contains(&format!("{expected_streak}/3")),
                "semantic anomaly note should expose current streak: {semantic_note}"
            );

            let body_bytes = result
                .response
                .bytes()
                .await
                .map_err(|e| AppError::Message(e.to_string()))?;
            let body_json: Value = serde_json::from_slice(&body_bytes)
                .map_err(|e| AppError::Message(e.to_string()))?;
            assert_eq!(body_json["id"], "resp_empty");

            let allow_result = forwarder
                .router
                .allow_provider_request("provider-a", "codex")
                .await;
            assert!(
                allow_result.allowed,
                "semantic anomaly {expected_streak}/3 should not trip circuit yet"
            );
        }

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello"
                }),
                HeaderMap::new(),
                Extensions::new(),
                providers,
            )
            .await
            .map_err(|e| AppError::Message(format!("third semantic anomaly should failover: {}", e.error)))?;

        assert_eq!(result.provider.id, "provider-b");

        let body_bytes = result
            .response
            .bytes()
            .await
            .map_err(|e| AppError::Message(e.to_string()))?;
        let body_json: Value = serde_json::from_slice(&body_bytes)
            .map_err(|e| AppError::Message(e.to_string()))?;
        assert_eq!(body_json["id"], "resp_ok");

        let conn = crate::database::lock_conn!(db.conn);
        let error_message: String = conn
            .query_row(
                "SELECT error_message
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'
                   AND status_code = 502
                 LIMIT 1",
                [],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        assert!(
            error_message.contains("连续 3/3 次"),
            "third semantic failure should explain threshold breach: {error_message}"
        );

        let allow_result = forwarder
            .router
            .allow_provider_request("provider-a", "codex")
            .await;
        assert!(
            !allow_result.allowed,
            "third semantic failure should be counted into the circuit breaker"
        );

        Ok(())
    }

    #[tokio::test]
    async fn codex_streaming_semantic_empty_response_should_failover_on_third_hit() -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 60,
            ..Default::default()
        })
        .await
        .map_err(|e| AppError::Database(e.to_string()))?;

        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db.clone()));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let empty_base_url = start_test_sse_server_for_path(
            "/v1/responses",
            200,
            vec![concat!(
                "event: response.created\n",
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_empty\",\"model\":\"gpt-5\"}}\n\n",
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_empty\",\"model\":\"gpt-5\",\"usage\":{\"input_tokens\":0,\"output_tokens\":0,\"input_tokens_details\":{\"cached_tokens\":0}}}}\n\n"
            )],
        )
        .await;
        let success_base_url = start_test_sse_server_for_path(
            "/v1/responses",
            200,
            vec![concat!(
                "event: response.created\n",
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\",\"model\":\"gpt-5\"}}\n\n",
                "event: response.output_text.delta\n",
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello from provider B\"}\n\n",
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"model\":\"gpt-5\",\"usage\":{\"input_tokens\":12,\"output_tokens\":6}}}\n\n"
            )],
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            5,
            5,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let providers = vec![
            build_test_provider("provider-a", "Provider A", &empty_base_url),
            build_test_provider("provider-b", "Provider B", &success_base_url),
        ];

        for expected_streak in 1..=2 {
            let result = forwarder
                .forward_with_retry(
                    &AppType::Codex,
                    "/responses",
                    json!({
                        "model": "gpt-5",
                        "input": "hello",
                        "stream": true
                    }),
                    HeaderMap::new(),
                    Extensions::new(),
                    providers.clone(),
                )
                .await
                .map_err(|e| AppError::Message(format!("semantic stream anomaly should be tolerated before threshold: {}", e.error)))?;

            assert_eq!(result.provider.id, "provider-a");
            assert!(result.response.is_sse(), "should keep streaming response semantics");
            let semantic_note = result
                .response
                .semantic_note()
                .map(str::to_string)
                .ok_or_else(|| AppError::Message("expected semantic anomaly note".to_string()))?;
            assert!(
                semantic_note.contains(&format!("{expected_streak}/3")),
                "semantic stream anomaly note should expose current streak: {semantic_note}"
            );

            let sse_text = String::from_utf8(
                result
                    .response
                    .bytes()
                    .await
                    .map_err(|e| AppError::Message(e.to_string()))?
                    .to_vec(),
            )
            .map_err(|e| AppError::Message(e.to_string()))?;
            assert!(sse_text.contains("resp_empty"));

            let allow_result = forwarder
                .router
                .allow_provider_request("provider-a", "codex")
                .await;
            assert!(
                allow_result.allowed,
                "semantic stream anomaly {expected_streak}/3 should not trip circuit yet"
            );
        }

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello",
                    "stream": true
                }),
                HeaderMap::new(),
                Extensions::new(),
                providers,
            )
            .await
            .map_err(|e| AppError::Message(format!("third semantic stream anomaly should failover: {}", e.error)))?;

        assert_eq!(result.provider.id, "provider-b");
        assert!(result.response.is_sse(), "should keep streaming response semantics");

        let sse_text = String::from_utf8(
            result
                .response
                .bytes()
                .await
                .map_err(|e| AppError::Message(e.to_string()))?
                .to_vec(),
        )
        .map_err(|e| AppError::Message(e.to_string()))?;
        assert!(sse_text.contains("Hello from provider B"));
        assert!(!sse_text.contains("resp_empty"));

        let conn = crate::database::lock_conn!(db.conn);
        let error_message: String = conn
            .query_row(
                "SELECT error_message
                 FROM proxy_request_logs
                 WHERE provider_id = 'provider-a'
                   AND status_code = 502
                 LIMIT 1",
                [],
                |row: &rusqlite::Row<'_>| row.get(0),
            )
            .map_err(|e| AppError::Database(e.to_string()))?;
        assert!(
            error_message.contains("连续 3/3 次"),
            "third semantic stream failure should explain threshold breach: {error_message}"
        );

        let allow_result = forwarder
            .router
            .allow_provider_request("provider-a", "codex")
            .await;
        assert!(
            !allow_result.allowed,
            "third semantic stream failure should be counted into the circuit breaker"
        );

        Ok(())
    }

    #[tokio::test]
    async fn codex_streaming_semantic_success_should_capture_upstream_first_token_timing()
    -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let base_url = start_test_delayed_sse_server_for_path(
            "/v1/responses",
            200,
            vec![
                (
                    concat!(
                        "event: response.created\n",
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ok\",\"model\":\"gpt-5\"}}\n\n"
                    ),
                    40,
                ),
                (
                    concat!(
                        "event: response.output_text.delta\n",
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n"
                    ),
                    40,
                ),
                (
                    concat!(
                        "event: response.completed\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ok\",\"model\":\"gpt-5\",\"usage\":{\"input_tokens\":12,\"output_tokens\":6}}}\n\n"
                    ),
                    40,
                ),
            ],
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            5,
            5,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello",
                    "stream": true
                }),
                HeaderMap::new(),
                Extensions::new(),
                vec![build_test_provider("provider-a", "Provider A", &base_url)],
            )
            .await
            .map_err(|e| AppError::Message(format!("codex semantic success should not fail: {}", e.error)))?;

        let timing = result
            .response
            .timing()
            .ok_or_else(|| AppError::Message("expected upstream timing metadata".to_string()))?;
        assert!(
            timing.upstream_started_at.is_some(),
            "successful codex stream should preserve final upstream attempt start"
        );
        let first_token_ms = timing
            .upstream_first_token_ms
            .ok_or_else(|| AppError::Message("expected upstream first token timing".to_string()))?;
        assert!(
            (20..180).contains(&first_token_ms),
            "first token timing should reflect the first upstream event, got {first_token_ms}ms"
        );

        let sse_text = String::from_utf8(
            result
                .response
                .bytes()
                .await
                .map_err(|e| AppError::Message(e.to_string()))?
                .to_vec(),
        )
        .map_err(|e| AppError::Message(e.to_string()))?;
        assert!(sse_text.contains("Hello"));

        Ok(())
    }

    #[tokio::test]
    async fn codex_streaming_first_upstream_event_should_count_as_first_token_timing()
    -> Result<(), AppError> {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let db = Arc::new(Database::memory()?);
        let router = Arc::new(ProviderRouter::new(db.clone()));
        let failover_manager = Arc::new(FailoverSwitchManager::new(db));
        let status = Arc::new(RwLock::new(ProxyStatus::default()));
        let current_providers = Arc::new(RwLock::new(HashMap::new()));

        let base_url = start_test_delayed_sse_server_for_path(
            "/v1/responses",
            200,
            vec![
                (
                    concat!(
                        "event: response.created\n",
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_reasoning\",\"model\":\"gpt-5\"}}\n\n"
                    ),
                    40,
                ),
                (
                    concat!(
                        "event: response.reasoning.delta\n",
                        "data: {\"type\":\"response.reasoning.delta\",\"delta\":\"Let me think...\"}\n\n"
                    ),
                    220,
                ),
                (
                    concat!(
                        "event: response.output_text.delta\n",
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Answer\"}\n\n"
                    ),
                    40,
                ),
                (
                    concat!(
                        "event: response.completed\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_reasoning\",\"model\":\"gpt-5\",\"usage\":{\"input_tokens\":12,\"output_tokens\":6}}}\n\n"
                    ),
                    40,
                ),
            ],
        )
        .await;

        let forwarder = RequestForwarder::new(
            router,
            5,
            status,
            current_providers,
            failover_manager,
            None,
            "provider-a".to_string(),
            5,
            5,
            RectifierConfig::default(),
            OptimizerConfig::default(),
            CopilotOptimizerConfig::default(),
        );

        let result = forwarder
            .forward_with_retry(
                &AppType::Codex,
                "/responses",
                json!({
                    "model": "gpt-5",
                    "input": "hello",
                    "stream": true
                }),
                HeaderMap::new(),
                Extensions::new(),
                vec![build_test_provider("provider-a", "Provider A", &base_url)],
            )
            .await
            .map_err(|e| {
                AppError::Message(format!(
                    "codex reasoning delta should not fail semantic validation: {}",
                    e.error
                ))
            })?;

        let timing = result
            .response
            .timing()
            .ok_or_else(|| AppError::Message("expected upstream timing metadata".to_string()))?;
        let first_token_ms = timing
            .upstream_first_token_ms
            .ok_or_else(|| AppError::Message("expected upstream first token timing".to_string()))?;

        assert!(
            (20..180).contains(&first_token_ms),
            "first upstream event should be treated as first token timing, got {first_token_ms}ms"
        );

        let sse_text = String::from_utf8(
            result
                .response
                .bytes()
                .await
                .map_err(|e| AppError::Message(e.to_string()))?
                .to_vec(),
        )
        .map_err(|e| AppError::Message(e.to_string()))?;
        assert!(sse_text.contains("Answer"));

        Ok(())
    }
}
