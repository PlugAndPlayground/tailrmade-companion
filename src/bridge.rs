//! Bridge between MCP clients and Tailrmade browser tabs.
//!
//! The graph only exists inside a browser tab, so the companion relays tool
//! calls: MCP clients talk to `/mcp` (see `mcp.rs`), tabs connect to
//! `/bridge` over a WebSocket and execute the calls against their live graph.
//!
//! Bridge protocol, JSON text frames, every message carries `"v": 1`:
//! - tab → companion `hello` `{ tabId, title, tools, instructions }`
//! - tab → companion `result` `{ id, content, isError, images? }` (data URLs)
//! - tab → companion `active` `{ title? }` when the tab gains focus
//! - tab → companion `tools_changed` `{ tools, title?, instructions? }`
//! - companion → tab `call` `{ id, name, arguments }`
//! - companion → tab `client_connected` `{ name, version }`
//!
//! Calls go to the active tab: the one that most recently said `active`, or
//! the most recently connected one when no tab has.

use axum::{
    extract::{
        ws::{CloseFrame, Message, WebSocket},
        WebSocketUpgrade,
    },
    http::{header::ORIGIN, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub const PROTOCOL_VERSION: u64 = 1;
pub const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long an MCP client counts as attached after its last request.
pub const CLIENT_ACTIVE_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Tool results carry screenshots; keep well above a few data URLs.
const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;
const CLOSE_UNSUPPORTED: u16 = 4000;
const CLOSE_DISABLED: u16 = 4001;
const CLOSE_REPLACED: u16 = 4002;

pub type SharedBridge = Arc<Bridge>;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
    /// Data URLs, e.g. `data:image/png;base64,...`.
    pub images: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CallError {
    NoTab,
    TabClosed,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TabInfo {
    pub tab_id: String,
    pub title: String,
    pub active: bool,
    /// Seconds since the Unix epoch.
    pub connected_at: u64,
    pub last_active: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
    /// Seconds since this client's last request.
    pub idle_secs: u64,
}

/// What the active tab offers MCP clients.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ActiveTools {
    pub tab: Option<TabInfo>,
    /// The tab's tool definitions as sent, with `input_schema`.
    pub tools: Vec<Value>,
    pub instructions: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Incoming {
    #[serde(rename_all = "camelCase")]
    Hello {
        tab_id: String,
        #[serde(default)]
        title: String,
        #[serde(default)]
        tools: Vec<Value>,
        #[serde(default)]
        instructions: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    Result {
        id: u64,
        #[serde(default)]
        content: String,
        #[serde(default)]
        is_error: bool,
        #[serde(default)]
        images: Vec<String>,
    },
    Active {
        #[serde(default)]
        title: Option<String>,
    },
    ToolsChanged {
        tools: Vec<Value>,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        instructions: Option<String>,
    },
}

enum Outgoing {
    Text(String),
    Close(u16, &'static str),
}

struct Tab {
    conn: u64,
    tab_id: String,
    title: String,
    tools: Vec<Value>,
    instructions: Option<String>,
    connected_at: SystemTime,
    last_active: SystemTime,
    connected_seq: u64,
    /// 0 until the tab says `active`.
    active_seq: u64,
    outbox: mpsc::UnboundedSender<Outgoing>,
}

struct Client {
    name: String,
    version: String,
    seen: Instant,
}

#[derive(Default)]
struct State {
    tabs: Vec<Tab>,
    seq: u64,
    pending: HashMap<u64, (u64, oneshot::Sender<ToolResult>)>,
    client: Option<Client>,
}

impl State {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    fn active_index(&self) -> Option<usize> {
        let marked = self
            .tabs
            .iter()
            .enumerate()
            .filter(|(_, tab)| tab.active_seq > 0)
            .max_by_key(|(_, tab)| tab.active_seq);
        marked
            .or_else(|| {
                self.tabs
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, tab)| tab.connected_seq)
            })
            .map(|(index, _)| index)
    }

    fn tab_info(&self, index: usize, active: bool) -> TabInfo {
        let tab = &self.tabs[index];
        TabInfo {
            tab_id: tab.tab_id.clone(),
            title: tab.title.clone(),
            active,
            connected_at: unix_secs(tab.connected_at),
            last_active: unix_secs(tab.last_active),
        }
    }

    fn active_tools(&self) -> ActiveTools {
        match self.active_index() {
            Some(index) => ActiveTools {
                tab: Some(self.tab_info(index, true)),
                tools: self.tabs[index].tools.clone(),
                instructions: self.tabs[index].instructions.clone(),
            },
            None => ActiveTools::default(),
        }
    }
}

fn unix_secs(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct Bridge {
    enabled: AtomicBool,
    token: RwLock<Option<String>>,
    call_timeout: Duration,
    next_id: AtomicU64,
    state: Mutex<State>,
    /// Fires when the active tab's tool list may have changed.
    tools_changed: broadcast::Sender<()>,
    /// Bumped on any change the tray or settings window shows.
    changed: watch::Sender<u64>,
}

impl Bridge {
    pub fn new(enabled: bool) -> SharedBridge {
        Self::with_timeout(enabled, CALL_TIMEOUT)
    }

    pub fn with_timeout(enabled: bool, call_timeout: Duration) -> SharedBridge {
        Arc::new(Self {
            enabled: AtomicBool::new(enabled),
            token: RwLock::new(None),
            call_timeout,
            next_id: AtomicU64::new(1),
            state: Mutex::new(State::default()),
            tools_changed: broadcast::channel(16).0,
            changed: watch::channel(0).0,
        })
    }

    pub fn call_timeout(&self) -> Duration {
        self.call_timeout
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Turning the bridge off disconnects every tab.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
        if !enabled {
            let state = self.state.lock().unwrap();
            for tab in &state.tabs {
                let _ = tab
                    .outbox
                    .send(Outgoing::Close(CLOSE_DISABLED, "Agent access turned off"));
            }
        }
        self.notify_changed();
    }

    pub fn token(&self) -> Option<String> {
        self.token.read().unwrap().clone()
    }

    pub fn set_token(&self, token: String) {
        *self.token.write().unwrap() = Some(token);
    }

    pub fn subscribe_tools_changed(&self) -> broadcast::Receiver<()> {
        self.tools_changed.subscribe()
    }

    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn notify_changed(&self) {
        self.changed.send_modify(|count| *count += 1);
    }

    /// Runs `change` and fires `tools_changed` if it changed what MCP clients see.
    fn update<T>(&self, change: impl FnOnce(&mut State) -> T) -> T {
        let mut state = self.state.lock().unwrap();
        let before = state.active_tools();
        let result = change(&mut state);
        let after = state.active_tools();
        drop(state);
        if before.tools != after.tools || before.instructions != after.instructions {
            let _ = self.tools_changed.send(());
        }
        self.notify_changed();
        result
    }

    pub fn active_tools(&self) -> ActiveTools {
        self.state.lock().unwrap().active_tools()
    }

    pub fn tabs(&self) -> Vec<TabInfo> {
        let state = self.state.lock().unwrap();
        let active = state.active_index();
        (0..state.tabs.len())
            .map(|index| state.tab_info(index, Some(index) == active))
            .collect()
    }

    /// The MCP client seen most recently, whether or not it is still active.
    pub fn last_client(&self) -> Option<ClientInfo> {
        let state = self.state.lock().unwrap();
        state.client.as_ref().map(|client| ClientInfo {
            name: client.name.clone(),
            version: client.version.clone(),
            idle_secs: client.seen.elapsed().as_secs(),
        })
    }

    /// The client name while it has made a request within `CLIENT_ACTIVE_WINDOW`.
    pub fn active_client(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        state
            .client
            .as_ref()
            .filter(|client| client.seen.elapsed() < CLIENT_ACTIVE_WINDOW)
            .map(|client| client.name.clone())
    }

    /// Records MCP client activity. Tabs hear about a client when it
    /// initializes, changes, or comes back after being idle.
    pub fn note_client(&self, name: &str, version: &str, initialized: bool) {
        let mut state = self.state.lock().unwrap();
        let announce = initialized
            || state.client.as_ref().is_none_or(|client| {
                client.name != name || client.seen.elapsed() >= CLIENT_ACTIVE_WINDOW
            });
        state.client = Some(Client {
            name: name.to_string(),
            version: version.to_string(),
            seen: Instant::now(),
        });
        if announce {
            tracing::info!(client = name, version, "MCP client connected");
            let message = client_connected_message(name, version);
            for tab in &state.tabs {
                let _ = tab.outbox.send(Outgoing::Text(message.clone()));
            }
        }
        drop(state);
        if announce {
            self.notify_changed();
        }
    }

    /// Refreshes the last client's activity time for requests that carry no
    /// client info (legacy sessions only send it in `initialize`).
    pub fn touch_client(&self) {
        if let Some(client) = self.state.lock().unwrap().client.as_mut() {
            client.seen = Instant::now();
        }
    }

    /// Sends a tool call to the active tab and waits for its result.
    pub async fn call_tool(&self, name: &str, arguments: Value) -> Result<ToolResult, CallError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap();
            let index = state.active_index().ok_or(CallError::NoTab)?;
            let message = json!({
                "v": PROTOCOL_VERSION,
                "type": "call",
                "id": id,
                "name": name,
                "arguments": arguments,
            })
            .to_string();
            let tab = &mut state.tabs[index];
            tab.last_active = SystemTime::now();
            let conn = tab.conn;
            if tab.outbox.send(Outgoing::Text(message)).is_err() {
                return Err(CallError::TabClosed);
            }
            state.pending.insert(id, (conn, sender));
        }
        tracing::debug!(id, tool = name, "Forwarded tool call to tab");

        match tokio::time::timeout(self.call_timeout, receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(CallError::TabClosed),
            Err(_) => {
                self.state.lock().unwrap().pending.remove(&id);
                tracing::warn!(id, tool = name, "Tool call timed out");
                Err(CallError::Timeout)
            }
        }
    }

    fn register(&self, tab: Tab) {
        self.update(|state| {
            // A reload or reconnect of the same tab replaces the old socket.
            if let Some(index) = state.tabs.iter().position(|t| t.tab_id == tab.tab_id) {
                let old = state.tabs.remove(index);
                let _ = old.outbox.send(Outgoing::Close(
                    CLOSE_REPLACED,
                    "Replaced by a newer connection",
                ));
                state.pending.retain(|_, (conn, _)| *conn != old.conn);
            }
            let mut tab = tab;
            tab.connected_seq = state.next_seq();
            state.tabs.push(tab);
        });
    }

    fn unregister(&self, conn: u64) {
        self.update(|state| {
            state.tabs.retain(|tab| tab.conn != conn);
            // Dropping the senders fails the waiting calls with TabClosed.
            state.pending.retain(|_, (owner, _)| *owner != conn);
        });
    }

    fn handle_message(&self, conn: u64, message: Incoming) {
        match message {
            Incoming::Hello { .. } => {
                tracing::debug!(conn, "Ignoring repeated hello");
            }
            Incoming::Result {
                id,
                content,
                is_error,
                images,
            } => {
                let waiting = {
                    let mut state = self.state.lock().unwrap();
                    match state.pending.get(&id) {
                        // Only the tab the call went to may answer it.
                        Some((owner, _)) if *owner == conn => state.pending.remove(&id),
                        _ => None,
                    }
                };
                match waiting {
                    Some((_, sender)) => {
                        let _ = sender.send(ToolResult {
                            content,
                            is_error,
                            images,
                        });
                    }
                    None => tracing::debug!(id, "Result for an unknown or expired call"),
                }
            }
            Incoming::Active { title } => self.update(|state| {
                let seq = state.next_seq();
                if let Some(tab) = state.tabs.iter_mut().find(|tab| tab.conn == conn) {
                    tab.active_seq = seq;
                    tab.last_active = SystemTime::now();
                    if let Some(title) = title {
                        tab.title = title;
                    }
                }
            }),
            Incoming::ToolsChanged {
                tools,
                title,
                instructions,
            } => self.update(|state| {
                if let Some(tab) = state.tabs.iter_mut().find(|tab| tab.conn == conn) {
                    tab.tools = tools;
                    if let Some(title) = title {
                        tab.title = title;
                    }
                    if instructions.is_some() {
                        tab.instructions = instructions;
                    }
                }
            }),
        }
    }

    fn client_connected_for_new_tab(&self) -> Option<String> {
        let state = self.state.lock().unwrap();
        state
            .client
            .as_ref()
            .filter(|client| client.seen.elapsed() < CLIENT_ACTIVE_WINDOW)
            .map(|client| client_connected_message(&client.name, &client.version))
    }
}

fn client_connected_message(name: &str, version: &str) -> String {
    json!({
        "v": PROTOCOL_VERSION,
        "type": "client_connected",
        "name": name,
        "version": version,
    })
    .to_string()
}

/// Parses one frame, rejecting other protocol versions.
fn parse(text: &str) -> Result<Incoming, String> {
    let value: Value = serde_json::from_str(text).map_err(|error| error.to_string())?;
    match value.get("v").and_then(Value::as_u64) {
        Some(PROTOCOL_VERSION) => {}
        other => return Err(format!("unsupported bridge protocol version {other:?}")),
    }
    serde_json::from_value(value).map_err(|error| error.to_string())
}

/// `GET /bridge`: WebSocket for Tailrmade tabs.
pub async fn handle_upgrade(
    axum::extract::State(bridge): axum::extract::State<SharedBridge>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    if !bridge.is_enabled() {
        return (
            StatusCode::FORBIDDEN,
            "AI agent access (MCP) is turned off in Tailrmade Companion settings",
        )
            .into_response();
    }
    // check_origin already rejected disallowed origins. Browsers always send
    // Origin on WebSocket handshakes, so a missing one is not a TM tab.
    if !headers.contains_key(ORIGIN) {
        return (StatusCode::FORBIDDEN, "Missing Origin").into_response();
    }
    upgrade
        .max_message_size(MAX_MESSAGE_BYTES)
        .max_frame_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| run_socket(bridge, socket))
}

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

async fn run_socket(bridge: SharedBridge, socket: WebSocket) {
    let conn = NEXT_CONN.fetch_add(1, Ordering::SeqCst);
    let (mut sink, mut stream) = socket.split();

    // The first frame must be hello.
    let hello = loop {
        match stream.next().await {
            Some(Ok(Message::Text(text))) => break parse(&text),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            _ => return,
        }
    };
    let (tab_id, title, tools, instructions) = match hello {
        Ok(Incoming::Hello {
            tab_id,
            title,
            tools,
            instructions,
        }) => (tab_id, title, tools, instructions),
        Ok(_) => {
            tracing::warn!(conn, "Bridge connection did not start with hello");
            return;
        }
        Err(error) => {
            tracing::warn!(conn, %error, "Rejected bridge connection");
            let _ = sink
                .send(Message::Close(Some(CloseFrame {
                    code: CLOSE_UNSUPPORTED,
                    reason: "Unsupported bridge protocol. Update Tailrmade or the companion."
                        .into(),
                })))
                .await;
            return;
        }
    };

    let (outbox, mut outbox_rx) = mpsc::unbounded_channel();
    let now = SystemTime::now();
    tracing::info!(
        conn,
        tools = tools.len(),
        "Tailrmade tab connected to the agent bridge"
    );
    bridge.register(Tab {
        conn,
        tab_id,
        title,
        tools,
        instructions,
        connected_at: now,
        last_active: now,
        connected_seq: 0,
        active_seq: 0,
        outbox: outbox.clone(),
    });
    if let Some(message) = bridge.client_connected_for_new_tab() {
        let _ = outbox.send(Outgoing::Text(message));
    }
    drop(outbox);

    let writer = tokio::spawn(async move {
        while let Some(outgoing) = outbox_rx.recv().await {
            let result = match outgoing {
                Outgoing::Text(text) => sink.send(Message::Text(text)).await,
                Outgoing::Close(code, reason) => {
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })))
                        .await;
                    break;
                }
            };
            if result.is_err() {
                break;
            }
        }
    });

    let reader = async {
        while let Some(Ok(message)) = stream.next().await {
            match message {
                Message::Text(text) => match parse(&text) {
                    Ok(message) => bridge.handle_message(conn, message),
                    Err(error) => tracing::warn!(conn, %error, "Ignoring bridge message"),
                },
                Message::Close(_) => break,
                _ => {}
            }
        }
    };

    tokio::select! {
        _ = reader => {}
        _ = writer => {}
    }
    bridge.unregister(conn);
    tracing::info!(conn, "Tailrmade tab disconnected from the agent bridge");
}
