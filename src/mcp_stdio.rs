//! `tm-companion mcp-stdio`: MCP over stdin/stdout for clients that only
//! launch local processes (Claude Desktop's config file, some IDEs).
//!
//! Proxies every request to the running companion's `/mcp`, authenticated
//! with the pairing token from the keychain, and passes tool list changes
//! back. It never starts Tauri or the tray, and stdout carries nothing but
//! MCP messages; logs go to stderr and the log file.

use crate::{config, mcp, secrets};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities, ClientConfig,
        ContentBlock, Implementation, InitializeRequestParams, InitializeResult, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerConfig,
    },
    service::{NotificationContext, RequestContext, RunningService},
    transport::{
        streamable_http_client::StreamableHttpClientTransportConfig, StreamableHttpClientTransport,
    },
    ClientHandler, ErrorData as McpError, Peer, RoleClient, RoleServer, ServerHandler, ServiceExt,
};
use std::sync::{Arc, Mutex};

type Downstream = Arc<Mutex<Option<Peer<RoleServer>>>>;
type TokenSource = Arc<dyn Fn() -> Result<String, String> + Send + Sync>;

/// The client side, facing the companion.
#[derive(Clone)]
struct Upstream {
    client_info: Implementation,
    downstream: Downstream,
}

impl ClientHandler for Upstream {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(ClientCapabilities::default(), self.client_info.clone())
    }

    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        let peer = self.downstream.lock().unwrap().clone();
        if let Some(peer) = peer {
            let _ = peer.notify_tool_list_changed().await;
        }
    }
}

/// The server side, facing the stdio client.
#[derive(Clone)]
pub struct Proxy {
    url: String,
    token: TokenSource,
    connection: Arc<tokio::sync::Mutex<Option<RunningService<RoleClient, Upstream>>>>,
    instructions: Arc<Mutex<Option<String>>>,
    downstream: Downstream,
}

impl Proxy {
    pub fn new(url: String, token: TokenSource) -> Self {
        Self {
            url,
            token,
            connection: Default::default(),
            instructions: Default::default(),
            downstream: Default::default(),
        }
    }

    fn unreachable(&self, error: &str) -> String {
        format!(
            "Could not reach Tailrmade Companion at {}: {error}. Start the companion, turn on AI agents (MCP) in its settings, and retry.",
            self.url
        )
    }

    /// Connects on first use and again after a failure, re-reading the token
    /// so a regenerated one is picked up.
    async fn upstream(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<Peer<RoleClient>, String> {
        self.downstream
            .lock()
            .unwrap()
            .get_or_insert_with(|| context.peer.clone());
        let mut connection = self.connection.lock().await;
        if let Some(service) = connection.as_ref().filter(|service| !service.is_closed()) {
            return Ok(service.peer().clone());
        }
        let token = tokio::task::spawn_blocking({
            let token = self.token.clone();
            move || token()
        })
        .await
        .map_err(|error| error.to_string())??;
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(self.url.as_str()).auth_header(token),
        );
        let handler = Upstream {
            client_info: context.client_info().unwrap_or_else(|| {
                Implementation::new("tm-companion mcp-stdio", env!("CARGO_PKG_VERSION"))
            }),
            downstream: self.downstream.clone(),
        };
        let service = handler
            .serve(transport)
            .await
            .map_err(|error| error.to_string())?;
        *self.instructions.lock().unwrap() = service
            .peer()
            .peer_info()
            .and_then(|info| info.instructions.clone());
        let peer = service.peer().clone();
        *connection = Some(service);
        Ok(peer)
    }

    async fn reset(&self) {
        if let Some(service) = self.connection.lock().await.take() {
            let _ = service.cancel().await;
        }
    }
}

impl ServerHandler for Proxy {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .build(),
        )
        .with_server_info(
            Implementation::new(mcp::SERVER_NAME, env!("CARGO_PKG_VERSION"))
                .with_title("Tailrmade"),
        );
        info.instructions = self.instructions.lock().unwrap().clone();
        info
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        context.peer.set_peer_info(request.clone());
        // Connect now so the instructions are in the reply; failures surface
        // on the first tool call instead.
        if let Err(error) = self.upstream(&context).await {
            tracing::warn!(%error, "Companion not reachable during initialize");
        }
        self.negotiate_initialize(&request)
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        match self.upstream(&context).await {
            Ok(peer) => match peer.list_tools(request).await {
                Ok(result) => return Ok(result),
                Err(error) => tracing::warn!(%error, "Listing tools failed"),
            },
            Err(error) => tracing::warn!(%error, "Companion not reachable"),
        }
        self.reset().await;
        // tailrmade_status still answers, with the connection error.
        Ok(ListToolsResult::with_all_items(mcp::companion_tools()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let error = match self.upstream(&context).await {
            Ok(peer) => match peer.call_tool(request).await {
                Ok(result) => return Ok(result.into()),
                Err(error) => error.to_string(),
            },
            Err(error) => error,
        };
        self.reset().await;
        tracing::warn!(%error, "Tool call could not reach the companion");
        Ok(CallToolResult::error(vec![ContentBlock::text(self.unreachable(&error))]).into())
    }
}

/// reqwest 0.13 panics on building a client until a rustls provider exists,
/// even for plain HTTP. Same provider as the updater.
pub fn install_tls_provider() {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }
}

/// Runs until stdin closes. Returns the process exit code.
pub fn run() -> i32 {
    install_tls_provider();
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "Could not start the async runtime");
            return 1;
        }
    };
    runtime.block_on(async {
        let (port, _) = config::port();
        let proxy = Proxy::new(
            format!("http://127.0.0.1:{port}/mcp"),
            Arc::new(secrets::mcp_token),
        );
        tracing::info!(port, "mcp-stdio proxy started");
        let service = match proxy.serve(rmcp::transport::io::stdio_pipes()).await {
            Ok(service) => service,
            Err(error) => {
                tracing::error!(%error, "mcp-stdio could not start");
                return 1;
            }
        };
        match service.waiting().await {
            Ok(_) => 0,
            Err(error) => {
                tracing::error!(%error, "mcp-stdio stopped");
                1
            }
        }
    })
}
