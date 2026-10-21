const { invoke } = window.__TAURI__.core;
const $ = (id) => document.getElementById(id);

let toastTimer;
function toast(message, isError = false) {
  const el = $("toast");
  el.textContent = message;
  el.className = isError ? "toast err" : "toast";
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), isError ? 6000 : 2500);
}

async function call(command, args, success) {
  try {
    const result = await invoke(command, args);
    if (success) toast(success);
    return result;
  } catch (error) {
    toast(String(error), true);
    throw error;
  }
}

function describeSource(key) {
  if (!key.source) return { text: "Not set", set: false };
  switch (key.source.kind) {
    case "keychain":
      return { text: `Keychain ${key.masked}`, set: true };
    case "environment":
      return { text: `Environment variable ${key.masked}`, set: true };
    case "envFile":
      return { text: `.env file ${key.masked}`, set: true, title: key.source.path };
  }
  return { text: "Set", set: true };
}

function keyRow(key) {
  const li = document.createElement("li");
  const source = describeSource(key);

  const head = document.createElement("div");
  head.className = "key-head";
  const label = document.createElement("div");
  const title = document.createElement("div");
  title.className = "key-label";
  title.textContent = key.label || key.name;
  const name = document.createElement("code");
  name.className = "muted";
  name.textContent = key.name;
  label.append(title, name);
  const status = document.createElement("div");
  status.className = source.set ? "key-source set" : "key-source";
  status.textContent = source.text;
  if (source.title) status.title = source.title;
  head.append(label, status);

  const form = document.createElement("form");
  form.className = "row";
  const input = document.createElement("input");
  input.type = "password";
  input.autocomplete = "off";
  input.placeholder = source.set ? "Replace key" : "Paste key";
  const save = document.createElement("button");
  save.type = "submit";
  save.textContent = "Save";
  form.append(input, save);

  const inKeychain = key.source && key.source.kind === "keychain";
  if (inKeychain || !key.known) {
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "danger";
    remove.textContent = "Remove";
    remove.addEventListener("click", async () => {
      await call("remove_key", { name: key.name }, `${key.name} removed`);
      load();
    });
    form.append(remove);
  }

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!input.value.trim()) return;
    await call("save_key", { name: key.name, value: input.value }, `${key.name} saved`);
    input.value = "";
    load();
  });

  li.append(head, form);
  return li;
}

function renderStatus(state) {
  const pill = $("status");
  const error = $("server-error");
  const server = state.server;
  if (server.state === "running") {
    pill.textContent = `Running on port ${server.port}`;
    pill.className = "pill ok";
    error.hidden = true;
  } else if (server.state === "failed") {
    pill.textContent = "Not running";
    pill.className = "pill err";
    error.textContent = server.error;
    error.hidden = false;
  } else {
    pill.textContent = "Starting…";
    pill.className = "pill";
    error.hidden = true;
  }
}

async function load() {
  const state = await call("get_settings");
  $("version").textContent = `Version ${state.version}`;
  renderStatus(state);
  $("restart-banner").hidden = !state.restartNeeded;

  const keychain = $("keychain-error");
  if (state.keychainError) {
    keychain.textContent = `The system keychain is unavailable (${state.keychainError}). Keys in ${state.envFile} still work.`;
    keychain.hidden = false;
  } else {
    keychain.hidden = true;
  }

  const known = state.keys.filter((k) => k.known);
  const custom = state.keys.filter((k) => !k.known);
  $("known-keys").replaceChildren(...known.map(keyRow));
  if (custom.length) {
    $("custom-keys").replaceChildren(...custom.map(keyRow));
  } else {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = "None yet.";
    $("custom-keys").replaceChildren(empty);
  }

  $("autostart").checked = state.autostart;
  const portInput = $("port");
  if (document.activeElement !== portInput) {
    portInput.value = state.portSource === "default" ? "" : state.port;
  }
  const envOverride = state.portSource === "environment";
  portInput.disabled = envOverride;
  $("port-note").textContent = envOverride
    ? `Set by the TM_PORT environment variable (${state.port}).`
    : "Leave empty for the default (6655). Tailrmade expects 6655 unless you change it there too.";

  $("config-dir").textContent = state.configDir;
  $("config-dir").title = state.configDir;
  $("log-dir").textContent = state.logDir;
  $("log-dir").title = state.logDir;
  $("verbose-logging").checked = state.verboseLogging;
  $("frontend").textContent = state.frontend ? `Serving Tailrmade from ${state.frontend}` : "";
  $("frontend").hidden = !state.frontend;

  renderMcp(state.mcp);

  $("check-update").disabled = !state.updaterEnabled;
  if (!state.updaterEnabled) {
    $("update-status").textContent = "Not enabled in this build.";
  }
}

$("add-custom").addEventListener("submit", async (event) => {
  event.preventDefault();
  const name = $("custom-name").value.trim().toUpperCase();
  await call("save_key", { name, value: $("custom-value").value }, `${name} saved`);
  $("custom-name").value = "";
  $("custom-value").value = "";
  load();
});

$("port-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const raw = $("port").value.trim();
  const port = raw === "" ? null : Number(raw);
  await call("set_port", { port }, "Port saved");
  load();
});

$("autostart").addEventListener("change", async (event) => {
  try {
    await call("set_autostart", { enabled: event.target.checked });
  } finally {
    load();
  }
});

$("verbose-logging").addEventListener("change", async (event) => {
  try {
    await call("set_verbose_logging", { enabled: event.target.checked });
  } finally {
    load();
  }
});

$("restart").addEventListener("click", () => call("restart"));
$("open-config").addEventListener("click", () => call("open_config_dir"));
$("open-logs").addEventListener("click", () => call("open_logs_dir"));
$("open-tm").addEventListener("click", () => call("open_tailrmade"));

let pendingVersion = null;
$("check-update").addEventListener("click", async () => {
  const status = $("update-status");
  const button = $("check-update");
  button.disabled = true;
  if (pendingVersion) {
    status.textContent = "Downloading…";
    try {
      await call("install_update");
    } catch {
      button.disabled = false;
    }
    return;
  }
  status.textContent = "Checking…";
  try {
    pendingVersion = await invoke("check_update");
    status.textContent = pendingVersion
      ? `Version ${pendingVersion} is available.`
      : "You're up to date.";
    if (pendingVersion) button.textContent = `Install ${pendingVersion} and restart`;
  } catch (error) {
    status.textContent = String(error);
  }
  button.disabled = false;
});

window.addEventListener("focus", load);
load();

// ---------------------------------------------------------------------------
// AI agents (MCP)
// ---------------------------------------------------------------------------

const MASKED_TOKEN = "••••••••••••••••";
let mcp = null;
// Fetched as soon as the section shows: clipboard writes must happen right in
// the click handler, without waiting on the keychain.
let token = null;
let revealed = false;
let mcpClient = "claude-code";

function mcpUrl() {
  return `http://127.0.0.1:${mcp.port}/mcp`;
}

function claudeDesktopConfigPath() {
  if (mcp.os === "macos") return "~/Library/Application Support/Claude/claude_desktop_config.json";
  if (mcp.os === "windows") return "%APPDATA%\\Claude\\claude_desktop_config.json";
  return "claude_desktop_config.json";
}

function httpConfig(rootKey, token, extra = {}) {
  return JSON.stringify(
    {
      [rootKey]: {
        tailrmade: { ...extra, url: mcpUrl(), headers: { Authorization: `Bearer ${token}` } },
      },
    },
    null,
    2,
  );
}

function snippet(token) {
  switch (mcpClient) {
    case "claude-code":
      return {
        note: "Run in a terminal:",
        text: `claude mcp add --transport http tailrmade ${mcpUrl()} --header "Authorization: Bearer ${token}"`,
      };
    case "claude-desktop":
      return {
        note: `Add to ${claudeDesktopConfigPath()}, then restart Claude Desktop. It starts the companion's stdio bridge, which reads the token from the keychain itself.`,
        text: JSON.stringify(
          { mcpServers: { tailrmade: { command: mcp.stdioCommand, args: ["mcp-stdio"] } } },
          null,
          2,
        ),
      };
    case "vscode":
      return {
        note: "Add to .vscode/mcp.json in your workspace (or the user mcp.json):",
        text: httpConfig("servers", token, { type: "http" }),
      };
    case "cursor":
      return { note: "Add to ~/.cursor/mcp.json:", text: httpConfig("mcpServers", token) };
    case "lmstudio":
      return {
        note: "In LM Studio: Program → Install → Edit mcp.json, then add:",
        text: httpConfig("mcpServers", token),
      };
  }
}

function renderSnippet() {
  const { note, text } = snippet(revealed && token ? token : MASKED_TOKEN);
  $("mcp-snippet-note").textContent = note;
  $("mcp-snippet").textContent = text;
  for (const button of $("mcp-clients").querySelectorAll("button")) {
    button.classList.toggle("selected", button.dataset.client === mcpClient);
  }
}

function describeAge(seconds) {
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.round(seconds / 60)} min ago`;
  return `${Math.round(seconds / 3600)} h ago`;
}

function renderMcp(status) {
  mcp = status;
  $("mcp-enabled").checked = status.enabled;
  $("mcp-details").hidden = !status.enabled;
  if (status.enabled && !token) loadToken();

  const pill = $("mcp-pill");
  pill.hidden = !status.enabled;
  if (status.clientActive && status.lastClient) {
    pill.textContent = `Agent connected: ${status.lastClient.name}`;
    pill.className = "pill ok";
  } else {
    pill.textContent = status.tabs.length ? "Tailrmade connected" : "Waiting for Tailrmade";
    pill.className = "pill";
  }

  if (status.tabs.length) {
    $("mcp-tabs").replaceChildren(
      ...status.tabs.map((tab) => {
        const li = document.createElement("li");
        const head = document.createElement("div");
        head.className = "key-head";
        const title = document.createElement("div");
        title.className = "key-label";
        title.textContent = tab.title || "Untitled";
        const state = document.createElement("div");
        state.className = tab.active ? "key-source set" : "key-source";
        const since = describeAge(Math.max(0, Date.now() / 1000 - tab.connectedAt));
        state.textContent = tab.active ? `Receives tool calls · connected ${since}` : `Connected ${since}`;
        head.append(title, state);
        li.append(head);
        return li;
      }),
    );
  } else {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = "No Tailrmade tab. Open Tailrmade and turn on Agent access in the AI assistant settings.";
    $("mcp-tabs").replaceChildren(empty);
  }

  $("mcp-client").textContent = status.lastClient
    ? `Last MCP client: ${status.lastClient.name} ${status.lastClient.version} (${describeAge(status.lastClient.idleSecs)})`
    : "No MCP client has connected since the companion started.";
  renderSnippet();
}

async function refreshMcp() {
  try {
    renderMcp(await invoke("get_mcp_status"));
  } catch (error) {
    console.error(error);
  }
}

let tokenRequest = null;
function loadToken() {
  tokenRequest ||= call("get_mcp_token")
    .then((value) => {
      token = value;
      renderToken();
    })
    .finally(() => (tokenRequest = null));
  return tokenRequest;
}

function renderToken() {
  $("mcp-token").textContent = revealed && token ? token : MASKED_TOKEN;
  $("mcp-reveal").textContent = revealed ? "Hide" : "Show";
  renderSnippet();
}

async function copy(text, message) {
  try {
    await navigator.clipboard.writeText(text);
    toast(message);
  } catch (error) {
    toast(`Could not copy: ${error}`, true);
  }
}

$("mcp-enabled").addEventListener("change", async (event) => {
  try {
    await call(
      "set_mcp_enabled",
      { enabled: event.target.checked },
      event.target.checked ? "AI agents can connect" : "AI agents turned off",
    );
  } finally {
    refreshMcp();
  }
});

$("mcp-reveal").addEventListener("click", () => {
  revealed = !revealed;
  if (!token) loadToken();
  renderToken();
});

$("mcp-copy-token").addEventListener("click", () => {
  if (token) copy(token, "Token copied");
  else loadToken();
});

$("mcp-regenerate").addEventListener("click", async () => {
  if (!confirm("Regenerate the pairing token? Clients set up with the current token stop working until you update them.")) {
    return;
  }
  token = await call("regenerate_mcp_token", {}, "New token generated");
  renderToken();
});

$("mcp-copy-snippet").addEventListener("click", () => {
  if (token) copy(snippet(token).text, "Snippet copied");
  else loadToken();
});

$("mcp-clients").addEventListener("click", (event) => {
  const client = event.target.dataset?.client;
  if (!client) return;
  mcpClient = client;
  renderSnippet();
});

// Connected tabs and clients change while the window is open.
setInterval(() => {
  if (!document.hidden && mcp?.enabled) refreshMcp();
}, 3000);
