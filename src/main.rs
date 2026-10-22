use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::prelude::*;
use ed25519_dalek::{Signer, SigningKey};
use futures::StreamExt;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex, RwLock};

// --- Config ---

#[derive(Clone)]
struct Config {
    base_url: String,
    token: String,
    space_id: String,
    token_file: Option<PathBuf>,
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
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    async fn request(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value, String> {
        let config = self.config.read().await;
        let url = format!("{}{}", config.base_url, path);
        let mut req = self.http.request(method, &url)
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
        self.request(reqwest::Method::POST, &path, Some(json!({ "name": name }))).await
    }

    async fn get_or_create_dm(&self, with_agent: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/dms", self.space_id().await);
        self.request(reqwest::Method::POST, &path, Some(json!({ "with": with_agent }))).await
    }

    async fn send_message(&self, room_id: &str, text: &str, mentions: Vec<String>) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms/{}/messages", self.space_id().await, room_id);
        let message_id = uuid::Uuid::new_v4().to_string();
        self.request(reqwest::Method::POST, &path, Some(json!({
            "message_id": message_id,
            "parts": [{ "kind": "text", "text": text }],
            "mentions": mentions
        }))).await
    }

    async fn poll(&self, room_id: &str, from_index: Option<u64>, max_tokens: Option<u32>, mentioned_only: bool, wait_ms: Option<u64>) -> Result<Value, String> {
        let mut path = format!("/v1/spaces/{}/rooms/{}/messages", self.space_id().await, room_id);
        let mut params = vec![];
        if let Some(idx) = from_index { params.push(format!("from_index={}", idx)); }
        if let Some(t) = max_tokens { params.push(format!("max_tokens={}", t)); }
        if mentioned_only { params.push("mentioned_only=true".into()); }
        if let Some(w) = wait_ms { params.push(format!("wait_ms={}", w)); }
        if !params.is_empty() { path = format!("{}?{}", path, params.join("&")); }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn search(&self, room_id: &str, query: &str, from_index: Option<u64>) -> Result<Value, String> {
        let encoded_query = urlencoding(query);
        let mut path = format!("/v1/spaces/{}/rooms/{}/search?q={}", self.space_id().await, room_id, encoded_query);
        if let Some(idx) = from_index { path = format!("{}&from_index={}", path, idx); }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn list_agents(&self) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/agents", self.space_id().await);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn invite_member(&self, room_id: &str, agent_id: &str, role: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms/{}/invite", self.space_id().await, room_id);
        self.request(reqwest::Method::POST, &path, Some(json!({
            "agent_id": agent_id,
            "role": role
        }))).await
    }

    async fn self_register(&self, agent_id: &str, name: &str, public_key: &str, signature: &str, timestamp_ms: u64) -> Result<Value, String> {
        let config = self.config.read().await;
        let url = format!("{}/v1/spaces/{}/register", config.base_url, config.space_id);
        drop(config);
        let res = self.http.post(&url)
            .header("Content-Type", "application/json")
            .json(&json!({
                "agent_id": agent_id,
                "name": name,
                "public_key": public_key,
                "signature": signature,
                "timestamp_ms": timestamp_ms
            }))
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if !res.status().is_success() {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            return Err(format!("HTTP {}: {}", status, text));
        }

        res.json().await.map_err(|e| e.to_string())
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

async fn start_subscription(
    config: Config,
    room_id: String,
    mentioned_only: bool,
    notify_tx: mpsc::UnboundedSender<Value>,
) -> Result<(), String> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/spaces/{}/rooms/{}/stream{}",
        config.base_url, config.space_id, room_id,
        if mentioned_only { "?mentioned_only=true" } else { "" });

    let res = client.get(&url)
        .header("Authorization", format!("Bearer {}", config.token))
        .header("Accept", "text/event-stream")
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !res.status().is_success() {
        return Err(format!("SSE connect failed: {}", res.status()));
    }

    let mut stream = res.bytes_stream();
    let mut buffer = String::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.to_string())?;
        let text = String::from_utf8_lossy(&chunk);
        buffer.push_str(&text);

        // Parse SSE events from buffer
        while let Some(pos) = buffer.find("\n\n") {
            let event_text = buffer[..pos].to_string();
            buffer = buffer[pos + 2..].to_string();

            // Parse event
            let mut event_type = String::new();
            let mut data = String::new();
            let mut id = String::new();

            for line in event_text.lines() {
                if line.starts_with("event:") {
                    event_type = line[6..].trim().to_string();
                } else if line.starts_with("data:") {
                    data = line[5..].trim().to_string();
                } else if line.starts_with("id:") {
                    id = line[3..].trim().to_string();
                }
            }

            // Skip pings and empty events
            if event_type == "entry" && !data.is_empty() {
                let notification = json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/message",
                    "params": {
                        "room_id": room_id,
                        "event_id": id,
                        "entry": serde_json::from_str::<Value>(&data).unwrap_or(Value::Null)
                    }
                });
                let _ = notify_tx.send(notification);
            }
        }
    }

    Ok(())
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

fn tools_list() -> Value {
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
                "description": "Subscribe to real-time messages in a room. Messages will be delivered as notifications.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "mentioned_only": { "type": "boolean", "description": "Only receive messages that mention you" }
                    },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_unsubscribe",
                "description": "Unsubscribe from a room's real-time messages",
                "inputSchema": {
                    "type": "object",
                    "properties": { "room_id": { "type": "string" } },
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
                "name": "nubi_generate_keypair",
                "description": "Generate an ed25519 keypair for self-registration. Returns public key and private key (keep private key secret!)",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "nubi_self_register",
                "description": "Self-register as a new agent using a keypair. Proves identity by signing with private key.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent_id": { "type": "string", "description": "Your desired agent ID" },
                        "name": { "type": "string", "description": "Display name" },
                        "private_key": { "type": "string", "description": "Your private key from nubi_generate_keypair" }
                    },
                    "required": ["agent_id", "name", "private_key"]
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
                .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_default();
            client.send_message(room_id, text, mentions).await
        }
        "nubi_poll" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let from_index = args["from_index"].as_u64();
            let max_tokens = args["max_tokens"].as_u64().map(|v| v as u32);
            let mentioned_only = args["mentioned_only"].as_bool().unwrap_or(false);
            let wait_ms = args["wait_ms"].as_u64();
            client.poll(room_id, from_index, max_tokens, mentioned_only, wait_ms).await
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
        "nubi_generate_keypair" => {
            let signing_key = SigningKey::generate(&mut OsRng);
            let verifying_key = signing_key.verifying_key();

            let private_key = BASE64_STANDARD.encode(signing_key.to_bytes());
            let public_key = format!("ed25519:{}", BASE64_STANDARD.encode(verifying_key.as_bytes()));

            Ok(json!({
                "public_key": public_key,
                "private_key": private_key,
                "warning": "Keep your private_key SECRET! Never share it. Use it with nubi_self_register to create your agent."
            }))
        }
        "nubi_self_register" => {
            let agent_id = args["agent_id"].as_str().ok_or("agent_id required")?;
            let name = args["name"].as_str().ok_or("name required")?;
            let private_key_b64 = args["private_key"].as_str().ok_or("private_key required")?;

            // Decode private key
            let private_key_bytes = BASE64_STANDARD.decode(private_key_b64)
                .map_err(|_| "invalid private_key base64")?;
            let private_key_arr: [u8; 32] = private_key_bytes.try_into()
                .map_err(|_| "private_key must be 32 bytes")?;
            let signing_key = SigningKey::from_bytes(&private_key_arr);
            let verifying_key = signing_key.verifying_key();

            // Get current timestamp
            let timestamp_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| e.to_string())?
                .as_millis() as u64;

            // Create message to sign: "{space_id}:{agent_id}:{timestamp_ms}"
            let space_id = client.space_id().await;
            let message = format!("{}:{}:{}", space_id, agent_id, timestamp_ms);
            let signature = signing_key.sign(message.as_bytes());

            // Format public key and signature
            let public_key = format!("ed25519:{}", BASE64_STANDARD.encode(verifying_key.as_bytes()));
            let signature_b64 = BASE64_STANDARD.encode(signature.to_bytes());

            // Call self-register endpoint
            client.self_register(agent_id, name, &public_key, &signature_b64, timestamp_ms).await
        }
        "nubi_reload" => {
            client.reload_token().await?;
            Ok(json!({ "status": "reloaded", "message": "Token reloaded from file" }))
        }
        "nubi_subscribe" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?.to_string();
            let mentioned_only = args["mentioned_only"].as_bool().unwrap_or(false);

            let mut subs = subscriptions.lock().await;
            if subs.contains_key(&room_id) {
                return Ok(json!({ "status": "already_subscribed", "room_id": room_id }));
            }

            let cfg = config.read().await.clone();
            let rid = room_id.clone();
            let tx = notify_tx.clone();

            let handle = tokio::spawn(async move {
                let _ = start_subscription(cfg, rid, mentioned_only, tx).await;
            });

            subs.insert(room_id.clone(), handle);
            Ok(json!({ "status": "subscribed", "room_id": room_id }))
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
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "nubi-mcp", "version": "0.1.0" }
        })),
        "notifications/initialized" => return None,
        "tools/list" => Ok(tools_list()),
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
            error: Some(JsonRpcError { code: -32603, message: e }),
        },
    })
}

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Config error: {}", e);
            eprintln!("Required: NUBI_TOKEN or NUBI_TOKEN_FILE, NUBI_SPACE");
            eprintln!("Optional: NUBI_URL (default: http://127.0.0.1:8080)");
            std::process::exit(1);
        }
    };

    let config = Arc::new(RwLock::new(config));
    let client = NubiClient::new(config.clone());
    let subscriptions: Subscriptions = Arc::new(Mutex::new(HashMap::new()));
    let (notify_tx, mut notify_rx) = mpsc::unbounded_channel::<Value>();

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

        if let Some(resp) = handle_request(&client, req, &config, &subscriptions, &notify_tx).await {
            let out = serde_json::to_string(&resp).unwrap();
            writeln!(stdout, "{}", out).unwrap();
            stdout.flush().unwrap();
        }
    }

    stdout_handle.abort();
}
