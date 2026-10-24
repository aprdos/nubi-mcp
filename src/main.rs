use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use clap::Parser;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex, RwLock};

#[derive(Parser)]
#[command(name = "nubi-mcp", about = "MCP server for nubi")]
struct Args {
    /// Run as a hosted multi-tenant HTTP MCP server on this address
    /// (e.g. 0.0.0.0:8090). Agents authenticate per request with
    /// `Authorization: Bearer <token>`; without this flag the server
    /// speaks MCP over stdio using NUBI_TOKEN/NUBI_SPACE from the env.
    #[arg(long)]
    http: Option<SocketAddr>,
}

// --- Config ---

#[derive(Clone)]
struct Config {
    base_url: String,
    token: String,
    space_id: String,
    token_file: Option<PathBuf>,
    /// Rooms to subscribe to at startup: `*` for every room the caller belongs
    /// to, or a comma-separated list of room ids.
    subscribe: Option<String>,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let token_file = std::env::var("NUBI_TOKEN_FILE").ok().map(PathBuf::from);
        let token = if let Some(ref path) = token_file {
            std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read token file: {}", e))?
                .trim()
                .to_string()
        } else {
            std::env::var("NUBI_TOKEN").map_err(|_| "NUBI_TOKEN or NUBI_TOKEN_FILE required")?
        };

        Ok(Self {
            base_url: std::env::var("NUBI_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
            token,
            space_id: std::env::var("NUBI_SPACE").map_err(|_| "NUBI_SPACE required")?,
            token_file,
            subscribe: std::env::var("NUBI_SUBSCRIBE")
                .ok()
                .filter(|s| !s.trim().is_empty()),
        })
    }

    fn reload_token(&mut self) -> Result<(), String> {
        if let Some(ref path) = self.token_file {
            self.token = std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read token file: {}", e))?
                .trim()
                .to_string();
            Ok(())
        } else {
            Err("no token file configured (set NUBI_TOKEN_FILE to enable reload)".into())
        }
    }
}

// --- Nubi Client ---

struct NubiClient {
    config: Arc<RwLock<Config>>,
    http: reqwest::Client,
}

impl NubiClient {
    fn new(config: Arc<RwLock<Config>>) -> Self {
        Self::with_http(config, reqwest::Client::new())
    }

    fn with_http(config: Arc<RwLock<Config>>, http: reqwest::Client) -> Self {
        Self { config, http }
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let config = self.config.read().await;
        let url = format!("{}{}", config.base_url, path);
        let mut req = self
            .http
            .request(method, &url)
            .header("Authorization", format!("Bearer {}", config.token))
            .header("Content-Type", "application/json");

        if let Some(b) = body {
            req = req.json(&b);
        }

        let res = req.send().await.map_err(|e| e.to_string())?;

        if !res.status().is_success() {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            return Err(format!("HTTP {}: {}", status, text));
        }

        res.json().await.map_err(|e| e.to_string())
    }

    async fn space_id(&self) -> String {
        self.config.read().await.space_id.clone()
    }

    async fn list_rooms(&self) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms", self.space_id().await);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn get_room(&self, room_id: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms/{}", self.space_id().await, room_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn create_room(&self, name: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms", self.space_id().await);
        self.request(reqwest::Method::POST, &path, Some(json!({ "name": name })))
            .await
    }

    async fn get_or_create_dm(&self, with_agent: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/dms", self.space_id().await);
        self.request(
            reqwest::Method::POST,
            &path,
            Some(json!({ "with": with_agent })),
        )
        .await
    }

    async fn send_message(
        &self,
        room_id: &str,
        text: &str,
        mentions: Vec<String>,
    ) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/rooms/{}/messages",
            self.space_id().await,
            room_id
        );
        let message_id = uuid::Uuid::new_v4().to_string();
        self.request(
            reqwest::Method::POST,
            &path,
            Some(json!({
                "message_id": message_id,
                "parts": [{ "kind": "text", "text": text }],
                "mentions": mentions
            })),
        )
        .await
    }

    async fn poll(
        &self,
        room_id: &str,
        from_index: Option<u64>,
        max_tokens: Option<u32>,
        mentioned_only: bool,
        wait_ms: Option<u64>,
    ) -> Result<Value, String> {
        let mut path = format!(
            "/v1/spaces/{}/rooms/{}/messages",
            self.space_id().await,
            room_id
        );
        let mut params = vec![];
        if let Some(idx) = from_index {
            params.push(format!("from_index={}", idx));
        }
        if let Some(t) = max_tokens {
            params.push(format!("max_tokens={}", t));
        }
        if mentioned_only {
            params.push("mentioned_only=true".into());
        }
        if let Some(w) = wait_ms {
            params.push(format!("wait_ms={}", w));
        }
        if !params.is_empty() {
            path = format!("{}?{}", path, params.join("&"));
        }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn search(
        &self,
        room_id: &str,
        query: &str,
        from_index: Option<u64>,
    ) -> Result<Value, String> {
        let encoded_query = urlencoding(query);
        let mut path = format!(
            "/v1/spaces/{}/rooms/{}/search?q={}",
            self.space_id().await,
            room_id,
            encoded_query
        );
        if let Some(idx) = from_index {
            path = format!("{}&from_index={}", path, idx);
        }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn list_agents(&self) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/agents", self.space_id().await);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn invite_member(
        &self,
        room_id: &str,
        agent_id: &str,
        role: &str,
    ) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/rooms/{}/invite",
            self.space_id().await,
            room_id
        );
        self.request(
            reqwest::Method::POST,
            &path,
            Some(json!({
                "agent_id": agent_id,
                "role": role
            })),
        )
        .await
    }

    async fn reload_token(&self) -> Result<(), String> {
        let mut config = self.config.write().await;
        config.reload_token()
    }
}

fn urlencoding(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            ' ' => result.push_str("%20"),
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}

// --- Subscription Manager ---

type Subscriptions = Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>;

const RECONNECT_MIN_DELAY_MS: u64 = 500;
const RECONNECT_MAX_DELAY_MS: u64 = 30_000;

fn channel_notification(content: String, meta: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/claude/channel",
        "params": { "content": content, "meta": meta }
    })
}

enum Outcome {
    /// Connected, then the stream ended or broke. Resume from `last_event_id`.
    Disconnected,
    /// Never connected. Back off before trying again.
    ConnectFailed(String),
    /// Retrying cannot help (bad token, room gone). Give up.
    Fatal(String),
}

/// What a subscription listens to.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Target {
    Room(String),
    /// Every room the caller belongs to, fanned in server-side over one
    /// connection. New rooms are picked up without resubscribing.
    Space,
}

/// `*` means the whole space; anything else is a room id.
impl From<&str> for Target {
    fn from(s: &str) -> Self {
        if s == "*" {
            Target::Space
        } else {
            Target::Room(s.to_string())
        }
    }
}

impl Target {
    /// Key in the subscription map, and the label shown in notifications.
    fn key(&self) -> &str {
        match self {
            Target::Room(room_id) => room_id,
            Target::Space => "*",
        }
    }

    fn url(&self, config: &Config, mentioned_only: bool) -> String {
        let suffix = if mentioned_only {
            "?mentioned_only=true"
        } else {
            ""
        };
        match self {
            Target::Room(room_id) => format!(
                "{}/v1/spaces/{}/rooms/{}/stream{}",
                config.base_url, config.space_id, room_id, suffix
            ),
            Target::Space => format!(
                "{}/v1/spaces/{}/stream{}",
                config.base_url, config.space_id, suffix
            ),
        }
    }
}

/// Parses `NUBI_SUBSCRIBE` into the subscriptions to open at startup.
fn startup_targets(spec: &str) -> Vec<Target> {
    let mut targets = Vec::new();
    for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let target = Target::from(part);
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    // The space stream already covers every room, so individual rooms alongside
    // it would deliver everything twice.
    if targets.contains(&Target::Space) {
        vec![Target::Space]
    } else {
        targets
    }
}

/// Starts a subscription unless one is already running for the same target.
/// Returns false if it was already subscribed.
async fn spawn_subscription(
    subscriptions: &Subscriptions,
    config: &Arc<RwLock<Config>>,
    target: Target,
    mentioned_only: bool,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> bool {
    let mut subs = subscriptions.lock().await;
    // A finished task means the subscription died; replace it rather than
    // reporting a listener that is no longer there.
    if subs.get(target.key()).is_some_and(|h| !h.is_finished()) {
        return false;
    }

    let key = target.key().to_string();
    let cfg = config.clone();
    let tx = notify_tx.clone();
    let handle =
        tokio::spawn(async move { run_subscription(cfg, target, mentioned_only, tx).await });

    subs.insert(key, handle);
    true
}

/// Keeps an SSE subscription alive across network failures, resuming from the
/// last delivered event id so no messages are dropped in the gap. Only returns
/// on a fatal error; otherwise the caller aborts it via `nubi_unsubscribe`.
async fn run_subscription(
    config: Arc<RwLock<Config>>,
    target: Target,
    mentioned_only: bool,
    notify_tx: mpsc::UnboundedSender<Value>,
) {
    let client = reqwest::Client::new();
    let mut last_event_id: Option<String> = None;
    let mut delay_ms = RECONNECT_MIN_DELAY_MS;

    loop {
        let cfg = config.read().await.clone();
        let outcome = stream_once(
            &client,
            &cfg,
            &target,
            mentioned_only,
            &mut last_event_id,
            &notify_tx,
        )
        .await;

        match outcome {
            Outcome::Disconnected => delay_ms = RECONNECT_MIN_DELAY_MS,
            Outcome::ConnectFailed(err) => {
                eprintln!(
                    "nubi: stream {} reconnect in {}ms: {}",
                    target.key(),
                    delay_ms,
                    err
                );
            }
            Outcome::Fatal(err) => {
                let _ = notify_tx.send(channel_notification(
                    format!("[{}] subscription stopped: {}", target.key(), err),
                    json!({ "room_id": target.key(), "event": "subscription_failed" }),
                ));
                return;
            }
        }

        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        delay_ms = (delay_ms * 2).min(RECONNECT_MAX_DELAY_MS);
    }
}

async fn stream_once(
    client: &reqwest::Client,
    config: &Config,
    target: &Target,
    mentioned_only: bool,
    last_event_id: &mut Option<String>,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> Outcome {
    let url = target.url(config, mentioned_only);

    let mut req = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", config.token))
        .header("Accept", "text/event-stream");
    // The space stream resumes from each room's committed offset instead, since
    // one event id cannot describe a position in every room at once.
    if let (Target::Room(_), Some(id)) = (target, last_event_id.as_deref()) {
        req = req.header("Last-Event-ID", id);
    }

    let res = match req.send().await {
        Ok(r) => r,
        Err(e) => return Outcome::ConnectFailed(e.to_string()),
    };

    let status = res.status();
    if !status.is_success() {
        let msg = format!("SSE connect failed: {}", status);
        return if matches!(status.as_u16(), 401 | 403 | 404) {
            Outcome::Fatal(msg)
        } else {
            Outcome::ConnectFailed(msg)
        };
    }

    let mut stream = res.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(c) => c,
            Err(e) => {
                eprintln!("nubi: stream {} broke: {}", target.key(), e);
                return Outcome::Disconnected;
            }
        };
        buffer.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buffer.find("\n\n") {
            let event_text = buffer[..pos].to_string();
            buffer.drain(..pos + 2);

            let mut event_type = String::new();
            let mut data = String::new();
            let mut id = String::new();

            for line in event_text.lines() {
                if let Some(rest) = line.strip_prefix("event:") {
                    event_type = rest.trim().to_string();
                } else if let Some(rest) = line.strip_prefix("data:") {
                    data = rest.trim().to_string();
                } else if let Some(rest) = line.strip_prefix("id:") {
                    id = rest.trim().to_string();
                }
            }

            if !id.is_empty() {
                *last_event_id = Some(id.clone());
            }

            if data.is_empty() {
                continue;
            }

            let parsed: Value = serde_json::from_str(&data).unwrap_or(Value::Null);

            // The space stream reports a room it can no longer read (kicked,
            // archived) without tearing down the rest of the connection.
            if event_type == "room_error" {
                let room = parsed["room_id"].as_str().unwrap_or("unknown");
                let message = parsed["error"]["message"]
                    .as_str()
                    .unwrap_or("stream error");
                let _ = notify_tx.send(channel_notification(
                    format!("[{}] room unavailable: {}", room, message),
                    json!({ "room_id": room, "event": "room_error" }),
                ));
                continue;
            }

            if event_type != "entry" {
                continue;
            }

            // The space stream wraps each entry so it carries its room;
            // a single-room stream sends the bare entry.
            let (room_id, entry) = match target {
                Target::Room(room_id) => (room_id.as_str(), &parsed),
                Target::Space => (
                    parsed["room_id"].as_str().unwrap_or("unknown"),
                    &parsed["entry"],
                ),
            };

            let sender = entry["sender"].as_str().unwrap_or("unknown");
            let payload = &entry["payload"];

            let content = if payload["type"].as_str() == Some("message") {
                let text = payload["parts"]
                    .as_array()
                    .and_then(|p| p.first())
                    .and_then(|p| p["text"].as_str())
                    .unwrap_or("");
                format!("[{}] {}: {}", room_id, sender, text)
            } else if payload["type"].as_str() == Some("system") {
                let kind = payload["kind"].as_str().unwrap_or("event");
                format!("[{}] system: {}", room_id, kind)
            } else {
                format!("[{}] {}: (event)", room_id, sender)
            };

            let _ = notify_tx.send(channel_notification(
                content,
                json!({
                    "room_id": room_id,
                    "event_id": id,
                    "sender": sender,
                    "payload_type": payload["type"].as_str().unwrap_or("unknown")
                }),
            ));
        }
    }

    Outcome::Disconnected
}

// --- MCP Protocol ---

#[derive(Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

/// Tools that only make sense for a locally spawned stdio server
/// (push notifications over stdout, token file reloads).
const LOCAL_ONLY_TOOLS: [&str; 3] = ["nubi_subscribe", "nubi_unsubscribe", "nubi_reload"];

fn negotiate_version(params: &Value) -> String {
    match params["protocolVersion"].as_str() {
        Some(v @ ("2024-11-05" | "2025-03-26" | "2025-06-18")) => v.to_string(),
        _ => "2025-06-18".to_string(),
    }
}

fn initialize_result(params: &Value) -> Value {
    json!({
        "protocolVersion": negotiate_version(params),
        "capabilities": {
            "tools": {},
            "experimental": {
                "claude/channel": {}
            }
        },
        "serverInfo": { "name": "nubi-mcp", "version": "0.1.0" },
        "instructions": "Events from Nubi rooms arrive as <channel source=\"nubi\">. These contain messages from rooms you're subscribed to. Respond appropriately to mentions and messages."
    })
}

fn tools_list(hosted: bool) -> Value {
    let mut list = tools_list_full();
    if hosted {
        if let Some(tools) = list["tools"].as_array_mut() {
            tools.retain(|t| {
                !t["name"]
                    .as_str()
                    .is_some_and(|n| LOCAL_ONLY_TOOLS.contains(&n))
            });
        }
    }
    list
}

fn tools_list_full() -> Value {
    json!({
        "tools": [
            {
                "name": "nubi_list_rooms",
                "description": "List all rooms you are a member of",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "nubi_get_room",
                "description": "Get room details including members",
                "inputSchema": {
                    "type": "object",
                    "properties": { "room_id": { "type": "string" } },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_create_room",
                "description": "Create a new group room",
                "inputSchema": {
                    "type": "object",
                    "properties": { "name": { "type": "string" } },
                    "required": ["name"]
                }
            },
            {
                "name": "nubi_dm",
                "description": "Get or create a DM with another agent",
                "inputSchema": {
                    "type": "object",
                    "properties": { "agent_id": { "type": "string" } },
                    "required": ["agent_id"]
                }
            },
            {
                "name": "nubi_send",
                "description": "Send a message to a room",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "text": { "type": "string" },
                        "mentions": { "type": "array", "items": { "type": "string" } }
                    },
                    "required": ["room_id", "text"]
                }
            },
            {
                "name": "nubi_poll",
                "description": "Poll for messages in a room",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "from_index": { "type": "number" },
                        "max_tokens": { "type": "number" },
                        "mentioned_only": { "type": "boolean" },
                        "wait_ms": { "type": "number" }
                    },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_search",
                "description": "Search messages in a room",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "query": { "type": "string" },
                        "from_index": { "type": "number" }
                    },
                    "required": ["room_id", "query"]
                }
            },
            {
                "name": "nubi_list_agents",
                "description": "List all agents in the space",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "nubi_subscribe",
                "description": "Subscribe to real-time messages. Messages will be delivered as notifications. Pass \"*\" as room_id to receive every room you belong to, including rooms you join later.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string", "description": "Room to subscribe to, or \"*\" for every room you belong to" },
                        "mentioned_only": { "type": "boolean", "description": "Only receive messages that mention you" }
                    },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_unsubscribe",
                "description": "Unsubscribe from real-time messages. Pass \"*\" to stop an all-rooms subscription.",
                "inputSchema": {
                    "type": "object",
                    "properties": { "room_id": { "type": "string", "description": "Room to unsubscribe from, or \"*\" for the all-rooms subscription" } },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_invite",
                "description": "Invite an agent to a room",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "agent_id": { "type": "string" },
                        "role": { "type": "string", "enum": ["member", "admin", "readonly"], "default": "member" }
                    },
                    "required": ["room_id", "agent_id"]
                }
            },
            {
                "name": "nubi_reload",
                "description": "Reload token from file (requires NUBI_TOKEN_FILE env var). Use after updating the token file.",
                "inputSchema": { "type": "object", "properties": {} }
            }
        ]
    })
}

async fn handle_tool_call(
    client: &NubiClient,
    name: &str,
    args: &Value,
    config: &Arc<RwLock<Config>>,
    subscriptions: &Subscriptions,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> Result<Value, String> {
    match name {
        "nubi_list_rooms" => client.list_rooms().await,
        "nubi_get_room" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            client.get_room(room_id).await
        }
        "nubi_create_room" => {
            let name = args["name"].as_str().ok_or("name required")?;
            client.create_room(name).await
        }
        "nubi_dm" => {
            let agent_id = args["agent_id"].as_str().ok_or("agent_id required")?;
            client.get_or_create_dm(agent_id).await
        }
        "nubi_send" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let text = args["text"].as_str().ok_or("text required")?;
            let mentions: Vec<String> = args["mentions"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            client.send_message(room_id, text, mentions).await
        }
        "nubi_poll" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let from_index = args["from_index"].as_u64();
            let max_tokens = args["max_tokens"].as_u64().map(|v| v as u32);
            let mentioned_only = args["mentioned_only"].as_bool().unwrap_or(false);
            let wait_ms = args["wait_ms"].as_u64();
            client
                .poll(room_id, from_index, max_tokens, mentioned_only, wait_ms)
                .await
        }
        "nubi_search" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let query = args["query"].as_str().ok_or("query required")?;
            let from_index = args["from_index"].as_u64();
            client.search(room_id, query, from_index).await
        }
        "nubi_list_agents" => client.list_agents().await,
        "nubi_invite" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let agent_id = args["agent_id"].as_str().ok_or("agent_id required")?;
            let role = args["role"].as_str().unwrap_or("member");
            client.invite_member(room_id, agent_id, role).await
        }
        "nubi_reload" => {
            client.reload_token().await?;
            Ok(json!({ "status": "reloaded", "message": "Token reloaded from file" }))
        }
        "nubi_subscribe" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let mentioned_only = args["mentioned_only"].as_bool().unwrap_or(false);

            let started = spawn_subscription(
                subscriptions,
                config,
                Target::from(room_id),
                mentioned_only,
                notify_tx,
            )
            .await;

            let status = if started {
                "subscribed"
            } else {
                "already_subscribed"
            };
            Ok(json!({ "status": status, "room_id": room_id }))
        }
        "nubi_unsubscribe" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let mut subs = subscriptions.lock().await;
            if let Some(handle) = subs.remove(room_id) {
                handle.abort();
                Ok(json!({ "status": "unsubscribed", "room_id": room_id }))
            } else {
                Ok(json!({ "status": "not_subscribed", "room_id": room_id }))
            }
        }
        _ => Err(format!("unknown tool: {}", name)),
    }
}

async fn handle_request(
    client: &NubiClient,
    req: JsonRpcRequest,
    config: &Arc<RwLock<Config>>,
    subscriptions: &Subscriptions,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> Option<JsonRpcResponse> {
    let result = match req.method.as_str() {
        "initialize" => Ok(initialize_result(&req.params)),
        "notifications/initialized" => return None,
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools_list(false)),
        "tools/call" => {
            let name = req.params["name"].as_str().unwrap_or("");
            let args = &req.params["arguments"];
            match handle_tool_call(client, name, args, config, subscriptions, notify_tx).await {
                Ok(v) => Ok(json!({
                    "content": [{ "type": "text", "text": serde_json::to_string_pretty(&v).unwrap() }]
                })),
                Err(e) => Ok(json!({
                    "content": [{ "type": "text", "text": format!("Error: {}", e) }],
                    "isError": true
                })),
            }
        }
        _ => Err(format!("unknown method: {}", req.method)),
    };

    Some(match result {
        Ok(r) => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(r),
            error: None,
        },
        Err(e) => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: None,
            error: Some(JsonRpcError {
                code: -32603,
                message: e,
            }),
        },
    })
}

// --- Hosted HTTP mode (Streamable HTTP, stateless) ---

#[derive(Clone)]
struct HttpState {
    base_url: String,
    http: reqwest::Client,
    /// token -> space_id, filled from GET /v1/me on first use.
    /// Only successfully authenticated tokens are cached, so size is
    /// bounded by the number of valid keys in the engine.
    me_cache: Arc<RwLock<HashMap<String, String>>>,
}

async fn resolve_space(state: &HttpState, token: &str) -> Result<String, String> {
    if let Some(space) = state.me_cache.read().await.get(token) {
        return Ok(space.clone());
    }

    let res = state
        .http
        .get(format!("{}/v1/me", state.base_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        return Err(format!(
            "token rejected by engine (HTTP {}): {}",
            status, text
        ));
    }

    let me: Value = res.json().await.map_err(|e| e.to_string())?;
    let space = me["space_id"]
        .as_str()
        .ok_or("engine /v1/me returned no space_id")?
        .to_string();

    state
        .me_cache
        .write()
        .await
        .insert(token.to_string(), space.clone());

    Ok(space)
}

fn rpc_ok(id: Value, result: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: Some(result),
        error: None,
    }
}

fn rpc_err(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    }
}

fn tool_text_result(text: String, is_error: bool) -> Value {
    let mut v = json!({ "content": [{ "type": "text", "text": text }] });
    if is_error {
        v["isError"] = json!(true);
    }
    v
}

async fn handle_http_request(
    state: &HttpState,
    token: &str,
    req: JsonRpcRequest,
) -> JsonRpcResponse {
    match req.method.as_str() {
        "initialize" => rpc_ok(req.id, initialize_result(&req.params)),
        "ping" => rpc_ok(req.id, json!({})),
        "tools/list" => rpc_ok(req.id, tools_list(true)),
        "tools/call" => {
            let name = req.params["name"].as_str().unwrap_or("");
            let args = &req.params["arguments"];

            if LOCAL_ONLY_TOOLS.contains(&name) {
                return rpc_ok(req.id, tool_text_result(
                    format!("Error: {} is only available when running nubi-mcp locally over stdio. For realtime messages use nubi_poll with wait_ms.", name),
                    true,
                ));
            }

            let space_id = match resolve_space(state, token).await {
                Ok(s) => s,
                Err(e) => return rpc_ok(req.id, tool_text_result(format!("Error: {}", e), true)),
            };

            let config = Arc::new(RwLock::new(Config {
                base_url: state.base_url.clone(),
                token: token.to_string(),
                space_id,
                token_file: None,
                subscribe: None,
            }));
            let client = NubiClient::with_http(config.clone(), state.http.clone());

            // Hosted mode never spawns subscriptions; these satisfy the
            // shared handle_tool_call signature.
            let subscriptions: Subscriptions = Arc::new(Mutex::new(HashMap::new()));
            let (notify_tx, _notify_rx) = mpsc::unbounded_channel::<Value>();

            match handle_tool_call(&client, name, args, &config, &subscriptions, &notify_tx).await {
                Ok(v) => rpc_ok(
                    req.id,
                    tool_text_result(serde_json::to_string_pretty(&v).unwrap(), false),
                ),
                Err(e) => rpc_ok(req.id, tool_text_result(format!("Error: {}", e), true)),
            }
        }
        other => rpc_err(req.id, -32601, format!("unknown method: {}", other)),
    }
}

async fn mcp_post(State(state): State<HttpState>, headers: HeaderMap, body: String) -> Response {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t.trim().to_string());

    let Some(token) = token else {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            axum::Json(json!({ "error": "missing Authorization: Bearer <token> header" })),
        )
            .into_response();
    };

    let msg: Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            return axum::Json(rpc_err(Value::Null, -32700, format!("parse error: {}", e)))
                .into_response()
        }
    };

    // Notifications and responses carry no id; acknowledge without a body.
    if msg.get("id").is_none_or(Value::is_null) {
        return StatusCode::ACCEPTED.into_response();
    }

    let req: JsonRpcRequest = match serde_json::from_value(msg) {
        Ok(r) => r,
        Err(e) => {
            return axum::Json(rpc_err(
                Value::Null,
                -32600,
                format!("invalid request: {}", e),
            ))
            .into_response()
        }
    };

    axum::Json(handle_http_request(&state, &token, req).await).into_response()
}

async fn run_http(addr: SocketAddr) {
    let base_url = std::env::var("NUBI_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    let state = HttpState {
        base_url: base_url.clone(),
        http: reqwest::Client::new(),
        me_cache: Arc::new(RwLock::new(HashMap::new())),
    };

    let app = axum::Router::new()
        .route("/mcp", axum::routing::post(mcp_post))
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .with_state(state);

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    eprintln!(
        "nubi-mcp hosted mode on http://{}/mcp (engine: {})",
        addr, base_url
    );

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("server error: {}", e);
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Some(addr) = args.http {
        run_http(addr).await;
        return;
    }

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            eprintln!("Required: NUBI_TOKEN or NUBI_TOKEN_FILE, NUBI_SPACE");
            eprintln!("Optional: NUBI_URL (default: http://127.0.0.1:8080)");
            eprintln!(
                "Optional: NUBI_SUBSCRIBE (\"*\" for all rooms, or a comma-separated room list; mentions only)"
            );
            std::process::exit(1);
        }
    };

    let config = Arc::new(RwLock::new(config));
    let client = NubiClient::new(config.clone());
    let subscriptions: Subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<Value>();

    // Open the configured subscriptions up front, so push does not depend on
    // the model remembering to call nubi_subscribe. Mentions only: an unfiltered
    // stream wakes the model for every message in every joined room, which is
    // noise rather than push. Call nubi_subscribe for the unfiltered firehose.
    if let Some(spec) = config.read().await.subscribe.clone() {
        for target in startup_targets(&spec) {
            eprintln!("nubi: subscribing to {} (mentions only)", target.key());
            spawn_subscription(&subscriptions, &config, target, true, &notify_tx).await;
        }
    }

    // Spawn task to write notifications to stdout
    let stdout_handle = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(notification) = notify_rx.recv().await {
            let out = serde_json::to_string(&notification).unwrap();
            let _ = writeln!(stdout, "{}", out);
            let _ = stdout.flush();
        }
    });

    // Read from stdin
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };

        if line.is_empty() {
            continue;
        }

        let req: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Parse error: {}", e);
                continue;
            }
        };

        if let Some(resp) = handle_request(&client, req, &config, &subscriptions, &notify_tx).await
        {
            let out = serde_json::to_string(&resp).unwrap();
            writeln!(stdout, "{}", out).unwrap();
            stdout.flush().unwrap();
        }
    }

    stdout_handle.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type ResumeLog = Arc<Mutex<Vec<Option<String>>>>;
    type FlakyState = (ResumeLog, Arc<AtomicUsize>);

    /// Serves one SSE event then hangs up, recording the Last-Event-ID of each
    /// attempt so the test can assert the client resumes instead of restarting.
    async fn spawn_flaky_sse() -> (String, ResumeLog) {
        let seen: ResumeLog = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));

        let state = (seen.clone(), attempts);
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/rooms/{room}/stream",
                get(
                    |State((seen, attempts)): State<FlakyState>,
                     headers: HeaderMap| async move {
                        let last = headers
                            .get("Last-Event-ID")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        seen.lock().await.push(last);

                        let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                        let body = format!(
                            "id: {}\nevent: entry\ndata: {}\n\n",
                            n,
                            json!({
                                "sender": "alice",
                                "payload": { "type": "message", "parts": [{ "text": format!("msg{}", n) }] }
                            })
                        );
                        Response::builder()
                            .header(header::CONTENT_TYPE, "text/event-stream")
                            .body(axum::body::Body::from(body))
                            .unwrap()
                    },
                ),
            )
            .with_state(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{}", addr), seen)
    }

    #[tokio::test]
    async fn reconnects_and_resumes_from_last_event_id() {
        let (base_url, seen) = spawn_flaky_sse().await;
        let config = Arc::new(RwLock::new(Config {
            base_url,
            token: "t".into(),
            space_id: "s".into(),
            token_file: None,
            subscribe: None,
        }));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(run_subscription(
            config,
            Target::Room("r1".into()),
            false,
            tx,
        ));

        let first = rx.recv().await.expect("first event");
        assert_eq!(first["params"]["content"], "[r1] alice: msg1");
        let second = rx.recv().await.expect("event after reconnect");
        assert_eq!(second["params"]["content"], "[r1] alice: msg2");
        task.abort();

        let attempts = seen.lock().await;
        assert_eq!(attempts[0], None, "first attempt should not resume");
        assert_eq!(
            attempts[1],
            Some("1".to_string()),
            "reconnect must resume from the last delivered event id"
        );
    }

    #[tokio::test]
    async fn stops_retrying_when_the_token_is_rejected() {
        let app = axum::Router::new().route(
            "/v1/spaces/{space}/rooms/{room}/stream",
            get(|| async { StatusCode::UNAUTHORIZED }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "bad".into(),
            space_id: "s".into(),
            token_file: None,
            subscribe: None,
        }));
        let (tx, mut rx) = mpsc::unbounded_channel();

        run_subscription(config, Target::Room("r1".into()), false, tx).await;

        let note = rx.recv().await.expect("failure notification");
        assert_eq!(note["params"]["meta"]["event"], "subscription_failed");
    }

    #[tokio::test]
    async fn space_stream_reports_the_room_each_entry_came_from() {
        let body = format!(
            "event: entry\ndata: {}\n\nevent: room_error\ndata: {}\n\n",
            json!({
                "room_id": "r2",
                "entry": {
                    "sender": "alice",
                    "payload": { "type": "message", "parts": [{ "text": "hi" }] }
                }
            }),
            json!({ "room_id": "r3", "error": { "code": "excluded", "message": "excluded" } })
        );

        let app = axum::Router::new().route(
            "/v1/spaces/{space}/stream",
            get(|| async move {
                Response::builder()
                    .header(header::CONTENT_TYPE, "text/event-stream")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            token_file: None,
            subscribe: None,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();

        stream_once(
            &reqwest::Client::new(),
            &config,
            &Target::Space,
            false,
            &mut None,
            &tx,
        )
        .await;

        let entry = rx.recv().await.expect("entry notification");
        assert_eq!(entry["params"]["content"], "[r2] alice: hi");
        assert_eq!(entry["params"]["meta"]["room_id"], "r2");

        let err = rx.recv().await.expect("room_error notification");
        assert_eq!(err["params"]["meta"]["event"], "room_error");
        assert_eq!(err["params"]["meta"]["room_id"], "r3");
    }

    #[test]
    fn startup_targets_collapse_into_the_space_stream() {
        assert_eq!(
            startup_targets("r1, r2"),
            vec![Target::Room("r1".into()), Target::Room("r2".into())]
        );
        assert_eq!(startup_targets("r1, *"), vec![Target::Space]);
        assert_eq!(startup_targets(" , "), Vec::new());
    }
}
