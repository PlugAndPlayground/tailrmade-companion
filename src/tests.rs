//! End-to-end tests of `/mcp` and `/bridge`: a real server on a random port,
//! a mock Tailrmade tab over WebSocket and an rmcp client.

use crate::{
    bridge::{Bridge, SharedBridge},
    server,
};
use futures_util::{SinkExt, StreamExt};
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig, Implementation,
    },
    service::{NotificationContext, RunningService},
    transport::{
        streamable_http_client::StreamableHttpClientTransportConfig, StreamableHttpClientTransport,
    },
    ClientHandler, RoleClient, ServiceExt,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

const TOKEN: &str = "tm_test-token";
const TAB_ORIGIN: &str = "http://localhost:8080";

struct Server {
    addr: std::net::SocketAddr,
    bridge: SharedBridge,
}

impl Server {
    async fn start(call_timeout: Duration) -> Self {
        let bridge = Bridge::with_timeout(true, call_timeout);
        bridge.set_token(TOKEN.into());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = server::router(Arc::new(None), bridge.clone());
        tokio::spawn(async move { axum::serve(listener, router).await });
        Self { addr, bridge }
    }

    async fn new() -> Self {
        Self::start(Duration::from_secs(5)).await
    }

    fn mcp_url(&self) -> String {
        format!("http://{}/mcp", self.addr)
    }

    async fn client_with<H: ClientHandler>(&self, handler: H) -> RunningService<RoleClient, H> {
        crate::mcp_stdio::install_tls_provider();
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(self.mcp_url()).auth_header(TOKEN),
        );
        handler.serve(transport).await.expect("MCP client connects")
    }

    async fn client(&self) -> RunningService<RoleClient, ClientConfig> {
        self.client_with(ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::new("Test Client", "1.2.3"),
        ))
        .await
    }

    async fn tab(&self, tab_id: &str) -> MockTab {
        let tab = MockTab::connect(self.addr, tab_id).await;
        self.wait_for_tabs(|count| count >= 1).await;
        tab
    }

    async fn wait_for_tabs(&self, done: impl Fn(usize) -> bool) {
        for _ in 0..200 {
            if done(self.bridge.tabs().len()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("tab count never matched");
    }
}

struct MockTab {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

fn tab_tools() -> Value {
    json!([
        {
            "name": "inspect_graph",
            "description": "Summarize the current graph.",
            "input_schema": { "type": "object", "properties": {} },
        },
        {
            "name": "add_node",
            "description": "Add one node.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "node_type": { "type": "string" },
                    "node_id": { "type": "string" },
                },
                "required": ["node_type", "node_id"],
            },
        },
    ])
}

impl MockTab {
    async fn open(addr: std::net::SocketAddr, origin: Option<&str>) -> Result<Self, String> {
        let mut request = format!("ws://{addr}/bridge").into_client_request().unwrap();
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("Origin", origin.parse().unwrap());
        }
        let (ws, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self { ws })
    }

    async fn connect(addr: std::net::SocketAddr, tab_id: &str) -> Self {
        let mut tab = Self::open(addr, Some(TAB_ORIGIN)).await.unwrap();
        tab.send(json!({
            "v": 1,
            "type": "hello",
            "tabId": tab_id,
            "title": format!("Graph {tab_id}"),
            "tools": tab_tools(),
            "instructions": format!("Instructions from {tab_id}"),
        }))
        .await;
        tab
    }

    async fn send(&mut self, message: Value) {
        self.ws
            .send(Message::Text(message.to_string()))
            .await
            .unwrap();
    }

    /// The next message of `kind`, skipping others.
    async fn expect(&mut self, kind: &str) -> Value {
        let wait = async {
            loop {
                match self.ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if value["type"] == kind {
                            return value;
                        }
                    }
                    Some(Ok(_)) => {}
                    other => panic!("socket ended while waiting for {kind}: {other:?}"),
                }
            }
        };
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .unwrap_or_else(|_| panic!("no {kind} message"))
    }

    /// Answers the next call and returns it.
    async fn answer(&mut self, content: &str, is_error: bool, images: Value) -> Value {
        let call = self.expect("call").await;
        self.send(json!({
            "v": 1,
            "type": "result",
            "id": call["id"],
            "content": content,
            "isError": is_error,
            "images": images,
        }))
        .await;
        call
    }
}

fn texts(result: &CallToolResult) -> Vec<String> {
    result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect()
}

fn call(name: &str, arguments: Value) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_string())
        .with_arguments(arguments.as_object().cloned().unwrap_or_default())
}

async fn post_initialize(
    server: &Server,
    token: Option<&str>,
    origin: Option<&str>,
    protocol_version: &str,
) -> (u16, Option<Value>) {
    let mut request = reqwest::Client::new()
        .post(server.mcp_url())
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": protocol_version,
                    "capabilities": {},
                    "clientInfo": { "name": "raw", "version": "0" },
                },
            })
            .to_string(),
        );
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(origin) = origin {
        request = request.header("Origin", origin);
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    // JSON, or a server-sent event stream carrying it
    let body = serde_json::from_str(&text).ok().or_else(|| {
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .find_map(|data| serde_json::from_str::<Value>(data.trim()).ok())
    });
    (status, body)
}

#[tokio::test]
async fn initialize_uses_tab_instructions_and_negotiates_version() {
    let server = Server::new().await;
    let _tab = server.tab("a").await;

    let client = server.client().await;
    let info = client.peer().peer_info().expect("initialized");
    assert_eq!(info.server_info.as_ref().unwrap().name, "tailrmade");
    assert_eq!(info.instructions.as_deref(), Some("Instructions from a"));
    assert!(info.capabilities.tools.is_some());

    // an older client gets its own version back
    let (status, body) = post_initialize(&server, Some(TOKEN), None, "2025-03-26").await;
    assert_eq!(status, 200);
    assert_eq!(body.unwrap()["result"]["protocolVersion"], "2025-03-26");
}

#[tokio::test]
async fn initialize_without_tab_explains_setup() {
    let server = Server::new().await;
    let client = server.client().await;
    let instructions = client
        .peer()
        .peer_info()
        .unwrap()
        .instructions
        .clone()
        .unwrap();
    assert!(
        instructions.contains("enable Agent access"),
        "{instructions}"
    );
}

#[tokio::test]
async fn tab_hears_which_client_connected() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    let _client = server.client().await;
    let message = tab.expect("client_connected").await;
    assert_eq!(message["name"], "Test Client");
    assert_eq!(message["version"], "1.2.3");
    assert_eq!(
        server.bridge.active_client().as_deref(),
        Some("Test Client")
    );
}

#[tokio::test]
async fn lists_tab_tools_with_input_schema_and_companion_tools() {
    let server = Server::new().await;
    let _tab = server.tab("a").await;
    let client = server.client().await;

    let tools = client.peer().list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "tailrmade_status",
            "list_tailrmade_tabs",
            "inspect_graph",
            "add_node"
        ]
    );
    let add_node = serde_json::to_value(&tools[3]).unwrap();
    assert_eq!(
        add_node["inputSchema"]["required"],
        json!(["node_type", "node_id"])
    );
}

#[tokio::test]
async fn tool_call_round_trip() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    let client = server.client().await;

    let request = call(
        "add_node",
        json!({ "node_type": "Slider", "node_id": "ai-node-1" }),
    );
    let (result, received) = tokio::join!(
        client.peer().call_tool(request),
        tab.answer(r#"{"status":"added"}"#, false, json!([])),
    );
    assert_eq!(received["v"], 1);
    assert_eq!(received["name"], "add_node");
    assert_eq!(received["arguments"]["node_id"], "ai-node-1");

    let result = result.unwrap();
    assert_eq!(result.is_error, Some(false));
    assert_eq!(texts(&result), [r#"{"status":"added"}"#]);
}

#[tokio::test]
async fn tool_errors_and_images_pass_through() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    let client = server.client().await;

    let (result, _) = tokio::join!(
        client.peer().call_tool(call("inspect_graph", json!({}))),
        tab.answer(
            "{}",
            false,
            json!([
                "data:image/png;base64,iVBORw0KGgo=",
                "data:image/jpeg;base64,/9j/4A=="
            ]),
        ),
    );
    let value = serde_json::to_value(result.unwrap()).unwrap();
    let content = value["content"].as_array().unwrap();
    assert_eq!(content.len(), 3);
    assert_eq!(content[1]["type"], "image");
    assert_eq!(content[1]["mimeType"], "image/png");
    assert_eq!(content[1]["data"], "iVBORw0KGgo=");
    assert_eq!(content[2]["mimeType"], "image/jpeg");

    let (result, _) = tokio::join!(
        client.peer().call_tool(call("add_node", json!({}))),
        tab.answer("add_node requires node_type", true, json!([])),
    );
    let result = result.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(texts(&result), ["add_node requires node_type"]);
}

#[tokio::test]
async fn calls_without_a_tab_explain_how_to_connect() {
    let server = Server::new().await;
    let client = server.client().await;

    let result = client
        .peer()
        .call_tool(call("add_node", json!({})))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    let text = texts(&result).join("");
    assert!(
        text.contains("Open Tailrmade in your browser and enable Agent access"),
        "{text}"
    );

    let status = client
        .peer()
        .call_tool(call("tailrmade_status", json!({})))
        .await
        .unwrap();
    let status: Value = serde_json::from_str(&texts(&status)[0]).unwrap();
    assert_eq!(status["connected"], false);
}

#[tokio::test]
async fn calls_time_out_when_the_tab_does_not_answer() {
    let server = Server::start(Duration::from_millis(200)).await;
    let mut tab = server.tab("a").await;
    let client = server.client().await;

    let (result, _) = tokio::join!(
        client.peer().call_tool(call("inspect_graph", json!({}))),
        tab.expect("call"),
    );
    let result = result.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(texts(&result)[0].contains("did not answer"));
}

#[tokio::test]
async fn calls_fail_when_the_tab_disconnects() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    let client = server.client().await;

    let close = async {
        tab.expect("call").await;
        tab.ws.close(None).await.unwrap();
    };
    let (result, _) = tokio::join!(
        client.peer().call_tool(call("inspect_graph", json!({}))),
        close
    );
    let result = result.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(texts(&result)[0].contains("disconnected"));
}

#[tokio::test]
async fn calls_go_to_the_tab_marked_active() {
    let server = Server::new().await;
    let mut first = server.tab("first").await;
    let mut second = MockTab::connect(server.addr, "second").await;
    server.wait_for_tabs(|count| count == 2).await;
    let client = server.client().await;

    // newest connection wins until a tab says it is active
    assert_eq!(server.bridge.active_tools().tab.unwrap().tab_id, "second");
    first
        .send(json!({ "v": 1, "type": "active", "title": "Renamed" }))
        .await;
    for _ in 0..200 {
        if server.bridge.active_tools().tab.unwrap().tab_id == "first" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (result, _) = tokio::join!(
        client.peer().call_tool(call("inspect_graph", json!({}))),
        first.answer("from first", false, json!([])),
    );
    assert_eq!(texts(&result.unwrap()), ["from first"]);

    let tabs = client
        .peer()
        .call_tool(call("list_tailrmade_tabs", json!({})))
        .await
        .unwrap();
    let tabs: Value = serde_json::from_str(&texts(&tabs)[0]).unwrap();
    let active: Vec<&Value> = tabs["tabs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|tab| tab["active"] == true)
        .collect();
    assert_eq!(active.len(), 1);
    assert_eq!(active[0]["title"], "Renamed");

    // the other tab saw no call
    second.send(json!({ "v": 1, "type": "active" })).await;
    drop(second);
}

#[derive(Clone)]
struct ListChangedClient(mpsc::UnboundedSender<()>);

impl ClientHandler for ListChangedClient {
    async fn on_tool_list_changed(&self, _context: NotificationContext<RoleClient>) {
        let _ = self.0.send(());
    }
}

#[tokio::test]
async fn clients_hear_when_a_tab_brings_tools() {
    let server = Server::new().await;
    let (sender, mut changed) = mpsc::unbounded_channel();
    let client = server.client_with(ListChangedClient(sender)).await;
    assert_eq!(client.peer().list_all_tools().await.unwrap().len(), 2);

    let _tab = server.tab("a").await;
    tokio::time::timeout(Duration::from_secs(5), changed.recv())
        .await
        .expect("tools/list_changed notification")
        .unwrap();
    assert_eq!(client.peer().list_all_tools().await.unwrap().len(), 4);
}

#[tokio::test]
async fn rejects_missing_or_wrong_token() {
    let server = Server::new().await;
    let (status, _) = post_initialize(&server, None, None, "2025-06-18").await;
    assert_eq!(status, 401);
    let (status, _) = post_initialize(&server, Some("tm_wrong-token"), None, "2025-06-18").await;
    assert_eq!(status, 401);
    let (status, _) = post_initialize(&server, Some(TOKEN), None, "2025-06-18").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn rejects_disallowed_origins() {
    let server = Server::new().await;
    let (status, _) = post_initialize(
        &server,
        Some(TOKEN),
        Some("https://evil.example"),
        "2025-06-18",
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = post_initialize(
        &server,
        Some(TOKEN),
        Some("http://localhost:3000"),
        "2025-06-18",
    )
    .await;
    assert_eq!(status, 200);

    assert!(MockTab::open(server.addr, Some("https://evil.example"))
        .await
        .is_err());
    // browsers always send Origin on WebSocket handshakes
    assert!(MockTab::open(server.addr, None).await.is_err());
}

#[tokio::test]
async fn switched_off_rejects_clients_and_tabs() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    server.bridge.set_enabled(false);

    let (status, _) = post_initialize(&server, Some(TOKEN), None, "2025-06-18").await;
    assert_eq!(status, 403);
    // the open tab is told to go away
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match tab.ws.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|f| u16::from(f.code)),
                Some(Ok(_)) => {}
                _ => return None,
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(closed, Some(4001));
    assert!(MockTab::open(server.addr, Some(TAB_ORIGIN)).await.is_err());
}

#[tokio::test]
async fn rejects_other_bridge_protocol_versions() {
    let server = Server::new().await;
    let mut tab = MockTab::open(server.addr, Some(TAB_ORIGIN)).await.unwrap();
    tab.send(json!({ "v": 2, "type": "hello", "tabId": "future" }))
        .await;
    let frame = tokio::time::timeout(Duration::from_secs(5), tab.ws.next())
        .await
        .unwrap();
    match frame {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 4000),
        other => panic!("expected a close frame, got {other:?}"),
    }
    assert!(server.bridge.tabs().is_empty());
}

#[tokio::test]
async fn serves_stateless_2026_clients_without_initialize() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;
    let response = reqwest::Client::new()
        .post(server.mcp_url())
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {TOKEN}"))
        .header("MCP-Protocol-Version", "2026-07-28")
        .header("Mcp-Method", "tools/list")
        .body(
            json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/list",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientInfo": { "name": "Future Client", "version": "9" },
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                },
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let text = response.text().await.unwrap();
    let body: Value = serde_json::from_str(&text).ok().unwrap_or_else(|| {
        text.lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .find_map(|data| serde_json::from_str(data.trim()).ok())
            .unwrap()
    });
    assert_eq!(body["id"], 7);
    assert_eq!(body["result"]["tools"].as_array().unwrap().len(), 4);
    assert_eq!(
        tab.expect("client_connected").await["name"],
        "Future Client"
    );
}

#[tokio::test]
async fn stdio_proxy_forwards_to_the_companion() {
    let server = Server::new().await;
    let mut tab = server.tab("a").await;

    let proxy = crate::mcp_stdio::Proxy::new(server.mcp_url(), Arc::new(|| Ok(TOKEN.to_string())));
    let (client_end, proxy_end) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let service = proxy.serve(tokio::io::split(proxy_end)).await.unwrap();
        let _ = service.waiting().await;
    });
    crate::mcp_stdio::install_tls_provider();
    let client = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::new("Claude Desktop", "1.0"),
    )
    .serve(tokio::io::split(client_end))
    .await
    .unwrap();

    let info = client.peer().peer_info().unwrap();
    assert_eq!(info.instructions.as_deref(), Some("Instructions from a"));
    // the companion sees the real client, not the proxy
    assert_eq!(
        tab.expect("client_connected").await["name"],
        "Claude Desktop"
    );
    assert_eq!(client.peer().list_all_tools().await.unwrap().len(), 4);

    let (result, _) = tokio::join!(
        client.peer().call_tool(call("inspect_graph", json!({}))),
        tab.answer(
            "graph",
            false,
            json!(["data:image/png;base64,iVBORw0KGgo="])
        ),
    );
    let result = result.unwrap();
    assert_eq!(texts(&result), ["graph"]);
    assert_eq!(result.content.len(), 2);
}

#[tokio::test]
async fn stdio_proxy_reports_an_unreachable_companion() {
    let proxy = crate::mcp_stdio::Proxy::new(
        "http://127.0.0.1:9/mcp".into(),
        Arc::new(|| Ok(TOKEN.to_string())),
    );
    let (client_end, proxy_end) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let service = proxy.serve(tokio::io::split(proxy_end)).await.unwrap();
        let _ = service.waiting().await;
    });
    crate::mcp_stdio::install_tls_provider();
    let client = ClientConfig::new(ClientCapabilities::default(), Implementation::new("c", "1"))
        .serve(tokio::io::split(client_end))
        .await
        .unwrap();
    let tools = client.peer().list_all_tools().await.unwrap();
    assert_eq!(tools[0].name, "tailrmade_status");
    let result = client
        .peer()
        .call_tool(call("tailrmade_status", json!({})))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));
    assert!(texts(&result)[0].contains("Could not reach Tailrmade Companion"));
}
