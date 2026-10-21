//! MCP server at `/mcp` (Streamable HTTP) for external AI clients.
//!
//! Tools come from the active Tailrmade tab through the bridge (see
//! `bridge.rs`), plus two companion-level tools that work without a tab.
//! Clients on protocol revisions up to 2025-11-25 get a session and hear
//! about tool changes through `notifications/tools/list_changed`; 2026-07-28
//! clients are stateless and use `subscriptions/listen` for the same.
//!
//! Every request needs `Authorization: Bearer <pairing token>`. Origin checks
//! (`origin_policy.rs`) still apply to requests that carry an Origin, and rmcp
//! rejects non-loopback Host headers, which together stop DNS rebinding.

use crate::{
    bridge::{CallError, SharedBridge, ToolResult},
    secrets,
};
use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        InitializeRequestParams, InitializeResult, JsonObject, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig, SubscriptionFilter, Tool,
    },
    service::{NotificationContext, RequestContext, SubscriptionContext},
    transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ErrorData as McpError, RoleServer, ServerHandler,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

pub const SERVER_NAME: &str = "tailrmade";
pub const STATUS_TOOL: &str = "tailrmade_status";
pub const TABS_TOOL: &str = "list_tailrmade_tabs";

pub const NO_TAB_MESSAGE: &str = "No Tailrmade tab is connected. Open Tailrmade in your browser and enable Agent access (AI assistant → Settings → Agent access: Read-only or Read & edit). Then call tailrmade_status to check the connection and retry.";

const FALLBACK_INSTRUCTIONS: &str = "Tailrmade is a browser-based node editor for building apps from nodes. The tools edit the graph open in the user's Tailrmade tab. No tab is connected yet: ask the user to open Tailrmade and enable Agent access (AI assistant → Settings → Agent access). Once connected, call tailrmade_status with include_instructions true to get the working instructions and node catalogue, and list the tools again.";

#[derive(Clone)]
pub struct TailrmadeServer {
    bridge: SharedBridge,
}

impl TailrmadeServer {
    pub fn new(bridge: SharedBridge) -> Self {
        Self { bridge }
    }

    fn note_request(&self, context: &RequestContext<RoleServer>) {
        match context.client_info() {
            Some(info) => self.bridge.note_client(&info.name, &info.version, false),
            None => self.bridge.touch_client(),
        }
    }

    fn status(&self, arguments: Option<&JsonObject>) -> CallToolResult {
        let active = self.bridge.active_tools();
        let include_instructions = arguments
            .and_then(|args| args.get("include_instructions"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut status = json!({
            "connected": active.tab.is_some(),
            "activeTab": active.tab,
            "connectedTabs": self.bridge.tabs().len(),
            "tabTools": active.tools.len(),
        });
        if active.tab.is_none() {
            status["hint"] = NO_TAB_MESSAGE.into();
        } else if include_instructions {
            status["instructions"] = active.instructions.into();
        }
        CallToolResult::success(vec![ContentBlock::text(status.to_string())])
    }

    fn tabs(&self) -> CallToolResult {
        let tabs = self.bridge.tabs();
        CallToolResult::success(vec![ContentBlock::text(
            json!({ "tabs": tabs }).to_string(),
        )])
    }
}

fn empty_schema() -> JsonObject {
    json!({ "type": "object", "properties": {} })
        .as_object()
        .cloned()
        .unwrap_or_default()
}

pub fn companion_tools() -> Vec<Tool> {
    let status_schema = json!({
        "type": "object",
        "properties": {
            "include_instructions": {
                "type": "boolean",
                "description": "Also return the active tab's working instructions and node catalogue. Use when you connected before any Tailrmade tab was open.",
            },
        },
    });
    vec![
        Tool::new(
            STATUS_TOOL,
            "Check whether a Tailrmade browser tab is connected to the companion and which one receives tool calls. Call this first when a Tailrmade tool reports that no tab is connected.",
            Arc::new(status_schema.as_object().cloned().unwrap_or_default()),
        ),
        Tool::new(
            TABS_TOOL,
            "List the Tailrmade browser tabs connected to the companion. Tool calls go to the active tab: the one the user focused most recently.",
            Arc::new(empty_schema()),
        ),
    ]
}

/// Maps a tab's tool definition (`input_schema`) to an MCP tool (`inputSchema`).
fn tab_tool(value: &Value) -> Option<Tool> {
    let name = value.get("name")?.as_str()?.to_string();
    if name == STATUS_TOOL || name == TABS_TOOL {
        return None;
    }
    let description = value
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let schema = value
        .get("input_schema")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_else(empty_schema);
    Some(Tool::new(name, description, Arc::new(schema)))
}

/// Splits `data:image/png;base64,AAAA` into an MCP image block.
fn image_block(data_url: &str) -> Option<ContentBlock> {
    let (meta, data) = data_url.strip_prefix("data:")?.split_once(',')?;
    let mime_type = meta.strip_suffix(";base64")?;
    if !mime_type.starts_with("image/") || data.is_empty() {
        return None;
    }
    Some(ContentBlock::image(data, mime_type))
}

pub fn map_result(result: ToolResult) -> CallToolResult {
    let mut content = vec![ContentBlock::text(result.content)];
    for image in &result.images {
        match image_block(image) {
            Some(block) => content.push(block),
            None => tracing::warn!("Dropped a tool result image that is not a base64 data URL"),
        }
    }
    if result.is_error {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    }
}

fn call_error_message(error: CallError, timeout: std::time::Duration) -> String {
    match error {
        CallError::NoTab => NO_TAB_MESSAGE.to_string(),
        CallError::TabClosed => "The Tailrmade tab disconnected before it answered. Check the tab, then call tailrmade_status and retry. The call may already have run.".to_string(),
        CallError::Timeout => format!(
            "Tailrmade did not answer within {} seconds. The tab may be busy or its window may be in the background; ask the user to check it, then retry. The call may still finish.",
            timeout.as_secs()
        ),
    }
}

impl ServerHandler for TailrmadeServer {
    fn get_info(&self) -> ServerConfig {
        let instructions = self
            .bridge
            .active_tools()
            .instructions
            .filter(|text| !text.trim().is_empty())
            .unwrap_or_else(|| FALLBACK_INSTRUCTIONS.to_string());
        ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
        .with_server_info(
            Implementation::new(SERVER_NAME, env!("CARGO_PKG_VERSION")).with_title("Tailrmade"),
        )
        .with_instructions(instructions)
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        context.peer.set_peer_info(request.clone());
        self.bridge.note_client(
            &request.client_info.name,
            &request.client_info.version,
            true,
        );
        self.negotiate_initialize(&request)
    }

    /// Sessions (protocol ≤ 2025-11-25) hear about tool changes from here on.
    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        let mut changes = self.bridge.subscribe_tools_changed();
        let peer = context.peer;
        tokio::spawn(async move {
            while let Ok(()) | Err(RecvError::Lagged(_)) = changes.recv().await {
                if peer.notify_tool_list_changed().await.is_err() {
                    break;
                }
            }
        });
    }

    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        requested
            .tools_list_changed
            .unwrap_or(false)
            .then(|| SubscriptionFilter::builder().tools_list_changed().build())
    }

    /// `subscriptions/listen` (protocol 2026-07-28).
    async fn listen(&self, context: SubscriptionContext) -> Result<(), McpError> {
        let mut changes = self.bridge.subscribe_tools_changed();
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                change = changes.recv() => match change {
                    Ok(()) | Err(RecvError::Lagged(_)) => {
                        if context.sink().notify_tool_list_changed().await.is_err() {
                            return Ok(());
                        }
                    }
                    Err(RecvError::Closed) => return Ok(()),
                },
            }
        }
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.note_request(&context);
        let mut tools = companion_tools();
        tools.extend(self.bridge.active_tools().tools.iter().filter_map(tab_tool));
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        self.note_request(&context);
        let result = match request.name.as_ref() {
            STATUS_TOOL => self.status(request.arguments.as_ref()),
            TABS_TOOL => self.tabs(),
            name => {
                let arguments = Value::Object(request.arguments.clone().unwrap_or_default());
                let call = self.bridge.call_tool(name, arguments);
                let outcome = tokio::select! {
                    outcome = call => outcome,
                    _ = context.ct.cancelled() => return Err(McpError::internal_error("Request cancelled", None)),
                };
                match outcome {
                    Ok(result) => map_result(result),
                    Err(error) => CallToolResult::error(vec![ContentBlock::text(
                        call_error_message(error, self.bridge.call_timeout()),
                    )]),
                }
            }
        };
        Ok(result.into())
    }
}

pub fn service(
    bridge: SharedBridge,
) -> StreamableHttpService<TailrmadeServer, LocalSessionManager> {
    let config = StreamableHttpServerConfig::default()
        .with_json_response(true)
        .with_sse_keep_alive(Some(std::time::Duration::from_secs(15)));
    StreamableHttpService::new(
        move || Ok(TailrmadeServer::new(bridge.clone())),
        Default::default(),
        config,
    )
}

fn plain(status: StatusCode, message: &'static str) -> Response {
    (status, message).into_response()
}

/// Constant-time comparison so response timing reveals nothing about the token.
fn same_token(given: &[u8], expected: &[u8]) -> bool {
    given.len() == expected.len()
        && given
            .iter()
            .zip(expected)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

async fn ensure_token(bridge: &SharedBridge) -> Result<String, String> {
    if let Some(token) = bridge.token() {
        return Ok(token);
    }
    let token = tokio::task::spawn_blocking(secrets::mcp_token)
        .await
        .map_err(|error| error.to_string())??;
    bridge.set_token(token.clone());
    Ok(token)
}

/// Guards `/mcp`: the switch in settings, then the pairing token.
pub async fn require_token(
    State(bridge): State<SharedBridge>,
    request: Request,
    next: Next,
) -> Response {
    if !bridge.is_enabled() {
        return plain(
            StatusCode::FORBIDDEN,
            "AI agent access (MCP) is turned off. Turn it on in Tailrmade Companion settings.",
        );
    }
    let expected = match ensure_token(&bridge).await {
        Ok(token) => token,
        Err(error) => {
            tracing::error!(%error, "MCP pairing token unavailable");
            return plain(
                StatusCode::SERVICE_UNAVAILABLE,
                "The companion could not read its pairing token from the system keychain. See the companion logs.",
            );
        }
    };
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    match given {
        Some(given) if same_token(given.as_bytes(), expected.as_bytes()) => next.run(request).await,
        _ => {
            tracing::warn!("Rejected MCP request without a valid pairing token");
            let mut response = plain(
                StatusCode::UNAUTHORIZED,
                "Missing or wrong pairing token. Copy the current token from Tailrmade Companion settings → AI agents (MCP).",
            );
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_data_url_images() {
        let result = map_result(ToolResult {
            content: "{}".into(),
            is_error: false,
            images: vec![
                "data:image/png;base64,iVBORw0KGgo=".into(),
                "data:image/jpeg;base64,/9j/4AAQ".into(),
                "https://example.com/not-inline.png".into(),
                "data:text/plain;base64,aGk=".into(),
            ],
        });
        let value = serde_json::to_value(&result).unwrap();
        let content = value["content"].as_array().unwrap();
        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[1]["type"], "image");
        assert_eq!(content[1]["mimeType"], "image/png");
        assert_eq!(content[1]["data"], "iVBORw0KGgo=");
        assert_eq!(content[2]["mimeType"], "image/jpeg");
        assert_eq!(value["isError"], false);
    }

    #[test]
    fn renames_input_schema_and_keeps_companion_names() {
        let tool = tab_tool(&json!({
            "name": "add_node",
            "description": "Add a node",
            "input_schema": { "type": "object", "properties": { "node_type": { "type": "string" } } },
        }))
        .unwrap();
        let value = serde_json::to_value(&tool).unwrap();
        assert_eq!(
            value["inputSchema"]["properties"]["node_type"]["type"],
            "string"
        );
        assert!(value.get("input_schema").is_none());
        assert!(tab_tool(&json!({ "name": STATUS_TOOL })).is_none());
    }

    #[test]
    fn compares_tokens_exactly() {
        assert!(same_token(b"tm_abc", b"tm_abc"));
        assert!(!same_token(b"tm_abd", b"tm_abc"));
        assert!(!same_token(b"tm_ab", b"tm_abc"));
        assert!(!same_token(b"", b"tm_abc"));
    }
}
