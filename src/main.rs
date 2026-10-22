use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// --- Config ---

#[derive(Clone)]
struct Config {
    base_url: String,
    token: String,
    space_id: String,
    agent_id: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            base_url: std::env::var("NUBI_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".into()),
            token: std::env::var("NUBI_TOKEN").map_err(|_| "NUBI_TOKEN required")?,
            space_id: std::env::var("NUBI_SPACE").map_err(|_| "NUBI_SPACE required")?,
            agent_id: std::env::var("NUBI_AGENT").map_err(|_| "NUBI_AGENT required")?,
        })
    }
}

// --- Nubi Client ---

struct NubiClient {
    config: Config,
    http: reqwest::Client,
}

impl NubiClient {
    fn new(config: Config) -> Self {
        Self {
            config,
            http: reqwest::Client::new(),
        }
    }

    async fn request(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> Result<Value, String> {
        let url = format!("{}{}", self.config.base_url, path);
        let mut req = self.http.request(method, &url)
            .header("Authorization", format!("Bearer {}", self.config.token))
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

    async fn list_rooms(&self) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms", self.config.space_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn get_room(&self, room_id: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms/{}", self.config.space_id, room_id);
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn create_room(&self, name: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms", self.config.space_id);
        self.request(reqwest::Method::POST, &path, Some(json!({ "name": name }))).await
    }

    async fn get_or_create_dm(&self, with_agent: &str) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/dms", self.config.space_id);
        self.request(reqwest::Method::POST, &path, Some(json!({ "with": with_agent }))).await
    }

    async fn send_message(&self, room_id: &str, text: &str, mentions: Vec<String>) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms/{}/messages", self.config.space_id, room_id);
        let message_id = uuid::Uuid::new_v4().to_string();
        self.request(reqwest::Method::POST, &path, Some(json!({
            "message_id": message_id,
            "parts": [{ "kind": "text", "text": text }],
            "mentions": mentions
        }))).await
    }

    async fn poll(&self, room_id: &str, from_index: Option<u64>, max_tokens: Option<u32>, mentioned_only: bool, wait_ms: Option<u64>) -> Result<Value, String> {
        let mut path = format!("/v1/spaces/{}/rooms/{}/messages", self.config.space_id, room_id);
        let mut params = vec![];
        if let Some(idx) = from_index { params.push(format!("from_index={}", idx)); }
        if let Some(t) = max_tokens { params.push(format!("max_tokens={}", t)); }
        if mentioned_only { params.push("mentioned_only=true".into()); }
        if let Some(w) = wait_ms { params.push(format!("wait_ms={}", w)); }
        if !params.is_empty() { path = format!("{}?{}", path, params.join("&")); }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn search(&self, room_id: &str, query: &str, from_index: Option<u64>) -> Result<Value, String> {
        let encoded_query = urlencoding(&query);
        let mut path = format!("/v1/spaces/{}/rooms/{}/search?q={}", self.config.space_id, room_id, encoded_query);
        if let Some(idx) = from_index { path = format!("{}&from_index={}", path, idx); }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn list_agents(&self) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/agents", self.config.space_id);
        self.request(reqwest::Method::GET, &path, None).await
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
            }
        ]
    })
}

async fn handle_tool_call(client: &NubiClient, name: &str, args: &Value) -> Result<Value, String> {
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
        _ => Err(format!("unknown tool: {}", name)),
    }
}

async fn handle_request(client: &NubiClient, req: JsonRpcRequest) -> Option<JsonRpcResponse> {
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
            match handle_tool_call(client, name, args).await {
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
            eprintln!("Required: NUBI_TOKEN, NUBI_SPACE, NUBI_AGENT");
            eprintln!("Optional: NUBI_URL (default: http://127.0.0.1:8080)");
            std::process::exit(1);
        }
    };

    let client = NubiClient::new(config);
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

        if let Some(resp) = handle_request(&client, req).await {
            let out = serde_json::to_string(&resp).unwrap();
            writeln!(stdout, "{}", out).unwrap();
            stdout.flush().unwrap();
        }
    }
}
