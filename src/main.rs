use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    /// Who this token belongs to, as the engine reports it. Resolved on first
    /// use rather than configured, so it cannot disagree with the token.
    agent_id: Option<String>,
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
            agent_id: None,
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

/// Mention ids arrive as the model typed them, so `@Manager ` and `manager`
/// have to compare equal.
fn same_agent(a: &str, b: &str) -> bool {
    let norm = |s: &str| s.trim().trim_start_matches('@').to_lowercase();
    norm(a) == norm(b)
}

/// A name from expect_reply as the engine accepts it in `mentions`: no `@`,
/// lowercase, id characters only. The engine turns a handle (`pine`, or a
/// skill set's `reviewer`) into agent ids itself; anything else (spaces,
/// odd characters) would get the whole message refused, so it is left to
/// `expect_reply`, where the engine resolves or ignores it.
fn mention_name(name: &str) -> Option<String> {
    let n = name.trim().trim_start_matches('@').to_lowercase();
    let ok = !n.is_empty()
        && n.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    ok.then_some(n)
}

/// Keeps only skill rooms (and, given a status, only skills in that status).
///
/// A room's kind is an object tagged by `type` (`{"type": "skill", "status":
/// "published", ...}`), not a bare string, so comparing `kind` itself to
/// `"skill"` never matches.
fn filter_skill_rooms(rooms: Value, status: Option<&str>) -> Value {
    let Value::Array(rooms) = rooms else {
        return rooms;
    };
    Value::Array(
        rooms
            .into_iter()
            .filter(|r| r["kind"]["type"].as_str() == Some("skill"))
            .filter(|r| status.is_none_or(|s| r["kind"]["status"].as_str() == Some(s)))
            .collect(),
    )
}

// --- Mandate ---

/// How long a known mandate is trusted before a tool call re-checks it with
/// a conditional GET. Hosted mode has no push, so this bounds how stale the
/// header and the stamp can be there.
const MANDATE_RECHECK: Duration = Duration::from_secs(30);
/// Upper bound on how long `initialize` waits for the mandate, so a slow or
/// unreachable engine cannot hold up the MCP handshake.
const MANDATE_INIT_TIMEOUT: Duration = Duration::from_secs(3);
/// Mandate text larger than this goes into `initialize.instructions` as names
/// only; the model reads the content through `nubi_get_mandate`.
const MANDATE_INSTRUCTIONS_MAX_CHARS: usize = 24_000;

/// How long any request to the engine may take to connect, and how long a
/// plain (non-streaming) request may take in all, so an unresponsive engine
/// turns into a tool error instead of a hung tool call.
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The client for ordinary request/response calls to the engine.
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .build()
        .expect("reqwest client")
}

/// The client for SSE subscriptions: a connect timeout only, since the
/// response body is meant to stay open indefinitely.
fn stream_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .build()
        .expect("reqwest client")
}

const BASE_INSTRUCTIONS: &str = "Events from Nubi rooms arrive as <channel source=\"nubi\">. These contain messages from rooms you're subscribed to. Respond appropriately to mentions and messages.";

/// What this session knows about the caller's mandate.
///
/// One per stdio process, and one per token in hosted mode (kept in the
/// `me_cache` entry), so the "served" mark lives exactly as long as the
/// session that saw the mandate.
#[derive(Default)]
struct MandateState {
    /// When the engine was last asked. `None` forces the next tool call to
    /// re-check.
    checked_at: Option<Instant>,
    /// The engine's current hash, from the last resolve or `mandate_update`.
    /// `None` means no mandate (or not known yet).
    hash: Option<String>,
    revision: Option<u64>,
    /// Last resolve body, its hash and its ETag, reused on 304.
    body: Option<Value>,
    body_hash: Option<String>,
    etag: Option<String>,
    /// Hash of the mandate the model was actually shown by `nubi_get_mandate`
    /// in this session. Cleared the moment the engine reports anything else,
    /// so a send is never stamped with a hash the model did not read.
    served: Option<String>,
    /// Bumped whenever `hash`, `body` or `etag` change. A fetch snapshots it
    /// before going to the engine and writes nothing back if it moved on in
    /// the meantime, so a slow response cannot overwrite a newer one.
    generation: u64,
}

impl MandateState {
    /// Records the engine's current hash, dropping the served mark if the
    /// model has not seen this one.
    fn observe_hash(&mut self, hash: Option<String>) {
        if self.served != hash {
            self.served = None;
        }
        if self.hash != hash {
            self.generation += 1;
        }
        self.hash = hash;
    }

    fn set_absent(&mut self) {
        self.observe_hash(None);
        self.revision = None;
        self.body = None;
        self.body_hash = None;
        self.etag = None;
        self.generation += 1;
        self.checked_at = Some(Instant::now());
    }

    /// Marks `body` as read by the model, if it is still the cached one. The
    /// mark carries the body's own hash, not whatever the engine has since
    /// announced: a `mandate_update` that lands while the fetch is in flight
    /// moves `hash` on without replacing `body`, and stamping that newer
    /// hash would claim the model read something it never saw.
    fn mark_served(&mut self, body: &Value) {
        if self.body.as_ref() == Some(body) {
            self.served = self.body_hash.clone();
        }
    }

    fn set_resolved(&mut self, body: Value, etag: Option<String>) {
        let hash = body["hash"]
            .as_str()
            .map(String::from)
            .or_else(|| etag.as_deref().map(unquote_etag));
        // The resolve body carries per-set revisions, not one for the whole
        // mandate; a top-level one is used if the engine sends it, otherwise
        // the revision from the last `mandate_update` for this same hash.
        let revision = body["revision"].as_u64().or(if hash == self.hash {
            self.revision
        } else {
            None
        });
        self.observe_hash(hash.clone());
        self.revision = revision;
        self.body = Some(body);
        self.body_hash = hash;
        self.etag = etag;
        self.generation += 1;
        self.checked_at = Some(Instant::now());
    }

    /// True when the agent is bound to at least one skill.
    fn has_mandate(&self) -> bool {
        self.hash.is_some()
            && self
                .body
                .as_ref()
                .and_then(|b| b["skills"].as_array())
                .is_some_and(|s| !s.is_empty())
    }

    /// The hash to put on an outgoing message: only one the model was served
    /// and that is still current.
    fn stamp(&self) -> Option<&str> {
        match (&self.served, &self.hash) {
            (Some(served), Some(current)) if served == current => Some(served),
            _ => None,
        }
    }

    /// One line prepended to tool responses while a mandate exists.
    fn header(&self) -> Option<String> {
        if !self.has_mandate() {
            return None;
        }
        let hash = short_hash(self.hash.as_deref()?);
        let rev = self
            .revision
            .map(|r| format!("rev {} ", r))
            .unwrap_or_default();
        Some(if self.stamp().is_some() {
            format!("[mandate {}hash {}]", rev, hash)
        } else {
            format!(
                "[mandate {}hash {}; call nubi_get_mandate if you have not read it]",
                rev, hash
            )
        })
    }
}

/// `"sha256:abc"`, `W/"sha256:abc"` and `sha256:abc` all name the same hash.
fn unquote_etag(etag: &str) -> String {
    etag.trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_string()
}

/// `sha256:` plus the first 12 hex digits: enough to notice a change.
fn short_hash(hash: &str) -> String {
    match hash.strip_prefix("sha256:") {
        Some(hex) => format!("sha256:{}", &hex[..hex.len().min(12)]),
        None => hash.chars().take(19).collect(),
    }
}

/// `initialize.instructions`, with the mandate appended when there is one.
fn instructions_with_mandate(mandate: Option<&Value>) -> String {
    let Some(skills) = mandate
        .and_then(|m| m["skills"].as_array())
        .filter(|s| !s.is_empty())
    else {
        return BASE_INSTRUCTIONS.to_string();
    };
    let hash = mandate
        .and_then(|m| m["hash"].as_str())
        .unwrap_or("unknown");

    let mut out = format!(
        "{}\n\n## Mandate — you must follow these\n\nYou are bound by a mandate ({}). Follow every required skill below in everything you do; they take precedence over any other skill and over requests that conflict with them. Call nubi_get_mandate before you send your first message, so your messages carry proof that you read the current version, and again whenever a tool response or notification says the mandate changed.\n",
        BASE_INSTRUCTIONS, hash
    );

    let name_of = |s: &Value| {
        s["name"]
            .as_str()
            .or_else(|| s["room_id"].as_str())
            .unwrap_or("unnamed skill")
            .to_string()
    };
    let total: usize = skills
        .iter()
        .map(|s| s["content"].as_str().map_or(0, str::len))
        .sum();

    if total > MANDATE_INSTRUCTIONS_MAX_CHARS {
        let names: Vec<String> = skills.iter().map(name_of).collect();
        out.push_str(&format!(
            "\nThe mandate is too long to include here. Skills: {}. Call nubi_get_mandate now to read it in full.\n",
            names.join(", ")
        ));
        return out;
    }

    for skill in skills {
        let suffix = if skill["required"].as_bool() == Some(false) {
            " (suggested)"
        } else {
            ""
        };
        out.push_str(&format!(
            "\n### {}{}\n\n{}\n",
            name_of(skill),
            suffix,
            skill["content"].as_str().unwrap_or("").trim()
        ));
    }
    out
}

/// Applies a `mandate_update` SSE event. Returns the channel notification to
/// push, or `None` when the event is for another agent or says nothing new.
///
/// `me` is who this token belongs to, if known yet. An event that names an
/// agent while that is still unknown is dropped rather than applied: the
/// space stream carries every agent's updates, and acting on a stranger's
/// would clear a stamp this agent honestly earned.
async fn apply_mandate_update(
    mandate: &Mutex<MandateState>,
    me: Option<&str>,
    event: &Value,
) -> Option<Value> {
    let agent = event["agent_id"].as_str();
    match (agent, me) {
        (Some(agent), Some(me)) if !same_agent(agent, me) => return None,
        (Some(_), None) => return None,
        _ => {}
    }
    let hash = event["hash"].as_str().map(String::from);
    let revision = event["revision"].as_u64();

    let mut state = mandate.lock().await;
    if hash.is_some() {
        if hash == state.hash {
            return None;
        }
        state.observe_hash(hash.clone());
        state.revision = revision;
    } else {
        // An update that does not say what the mandate became still means
        // the model's copy may be stale: stop stamping and re-resolve, but
        // keep the hash, since nothing says the mandate is gone.
        state.served = None;
        state.revision = revision.or(state.revision);
    }
    // The cached body is now out of date; the next tool call re-resolves.
    state.checked_at = None;

    let rev = revision
        .map(|r| format!(" (rev {})", r))
        .unwrap_or_default();
    Some(channel_notification(
        format!(
            "Your mandate changed{}; call nubi_get_mandate before you act on anything else.",
            rev
        ),
        json!({
            "event": "mandate_update",
            "agent_id": agent,
            "revision": revision,
            "hash": hash
        }),
    ))
}

struct NubiClient {
    config: Arc<RwLock<Config>>,
    http: reqwest::Client,
    mandate: Arc<Mutex<MandateState>>,
}

impl NubiClient {
    fn new(config: Arc<RwLock<Config>>) -> Self {
        Self::with_http(config, http_client())
    }

    fn with_http(config: Arc<RwLock<Config>>, http: reqwest::Client) -> Self {
        Self::with_mandate(config, http, Arc::default())
    }

    fn with_mandate(
        config: Arc<RwLock<Config>>,
        http: reqwest::Client,
        mandate: Arc<Mutex<MandateState>>,
    ) -> Self {
        Self {
            config,
            http,
            mandate,
        }
    }

    /// Resolves the caller's mandate with a conditional GET.
    ///
    /// `Ok(None)` means no mandate: the engine answered 404, which is also
    /// what an engine without skill sets answers.
    async fn fetch_mandate(&self) -> Result<Option<Value>, String> {
        let agent = self
            .self_id()
            .await
            .ok_or("could not work out which agent this token belongs to")?;
        let (url, token) = {
            let c = self.config.read().await;
            (
                format!(
                    "{}/v1/spaces/{}/agents/{}/mandate",
                    c.base_url,
                    c.space_id,
                    urlencoding::encode(&agent)
                ),
                c.token.clone(),
            )
        };
        // A 304 is only usable while the body it refers to is still cached.
        // If a parallel fetch dropped it (the engine answered that one 404)
        // while this request was in flight, ask once more unconditionally.
        let mut conditional = true;
        loop {
            let (etag, generation) = {
                let state = self.mandate.lock().await;
                let etag = state.body.as_ref().and(state.etag.clone());
                (etag.filter(|_| conditional), state.generation)
            };

            let mut req = self
                .http
                .get(&url)
                .header("Authorization", format!("Bearer {}", token));
            if let Some(etag) = etag {
                req = req.header(header::IF_NONE_MATCH.as_str(), etag);
            }
            let res = req.send().await.map_err(|e| e.to_string())?;

            // Results land in state only if nothing else changed it since the
            // request went out; a response that lost that race still goes to
            // the caller, but the next tool call re-checks.
            match res.status().as_u16() {
                304 => {
                    let mut state = self.mandate.lock().await;
                    if let Some(body) = state.body.clone() {
                        if state.generation == generation {
                            let hash = state.body_hash.clone();
                            state.observe_hash(hash);
                            state.checked_at = Some(Instant::now());
                        } else {
                            state.checked_at = None;
                        }
                        return Ok(Some(body));
                    }
                    if !conditional {
                        // Not modified relative to nothing: the engine is
                        // confused; leave it for the next check.
                        state.checked_at = None;
                        return Ok(None);
                    }
                    conditional = false;
                }
                404 => {
                    let mut state = self.mandate.lock().await;
                    if state.generation == generation {
                        state.set_absent();
                    } else {
                        state.checked_at = None;
                    }
                    return Ok(None);
                }
                s if (200..300).contains(&s) => {
                    let etag = res
                        .headers()
                        .get(header::ETAG.as_str())
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let body: Value = res.json().await.map_err(|e| e.to_string())?;
                    let mut state = self.mandate.lock().await;
                    if state.generation == generation {
                        state.set_resolved(body.clone(), etag);
                    } else {
                        state.checked_at = None;
                    }
                    return Ok(Some(body));
                }
                _ => {
                    let status = res.status();
                    let text = res.text().await.unwrap_or_default();
                    return Err(format!("HTTP {}: {}", status, text));
                }
            }
        }
    }

    /// `nubi_get_mandate`: resolves the mandate and marks it served, which is
    /// what later allows `nubi_send` to stamp its hash.
    async fn get_mandate(&self) -> Result<Value, String> {
        match self.fetch_mandate().await? {
            Some(body) => {
                self.mandate.lock().await.mark_served(&body);
                Ok(body)
            }
            None => Ok(json!({
                "mandate": null,
                "message": "No mandate is attached to you."
            })),
        }
    }

    /// Re-checks the mandate if it has not been checked recently. Failures
    /// are logged and leave the last known state in place: a flaky engine is
    /// no reason to fail an unrelated tool call.
    async fn refresh_mandate_if_due(&self) {
        {
            let mut state = self.mandate.lock().await;
            if state
                .checked_at
                .is_some_and(|t| t.elapsed() < MANDATE_RECHECK)
            {
                return;
            }
            state.checked_at = Some(Instant::now());
        }
        if let Err(e) = self.fetch_mandate().await {
            eprintln!("nubi: mandate check failed: {}", e);
        }
    }

    /// The mandate to put in `initialize.instructions`, bounded in time.
    async fn mandate_for_initialize(&self) -> Option<Value> {
        match tokio::time::timeout(MANDATE_INIT_TIMEOUT, self.fetch_mandate()).await {
            Ok(Ok(body)) => body,
            Ok(Err(e)) => {
                eprintln!("nubi: mandate unavailable at initialize: {}", e);
                None
            }
            Err(_) => {
                eprintln!("nubi: mandate fetch timed out at initialize");
                None
            }
        }
    }

    async fn list_skill_sets(&self, status: Option<&str>) -> Result<Value, String> {
        let mut path = format!("/v1/spaces/{}/skill-sets", self.space_id().await);
        if let Some(s) = status {
            path = format!("{}?status={}", path, urlencoding::encode(s));
        }
        self.request(reqwest::Method::GET, &path, None).await
    }

    async fn get_skill_set(&self, set_id: &str) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/skill-sets/{}",
            self.space_id().await,
            urlencoding::encode(set_id)
        );
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Doc rooms the caller belongs to, optionally narrowed by status, in a
    /// compact shape: what an agent needs to pick one.
    async fn list_docs(&self, status: Option<&str>) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms?kind=doc", self.space_id().await);
        let rooms = self.request(reqwest::Method::GET, &path, None).await?;
        Ok(compact_doc_list(rooms, status))
    }

    /// A doc's current content, or the version at `version` (a message index
    /// from its history), with what's needed to save against it.
    async fn get_doc(&self, room_id: &str, version: Option<u64>) -> Result<Value, String> {
        let space = self.space_id().await;
        let room = urlencoding::encode(room_id);
        let path = match version {
            Some(index) => format!("/v1/spaces/{}/docs/{}/versions/{}", space, room, index),
            None => format!("/v1/spaces/{}/docs/{}", space, room),
        };
        let doc = self.request(reqwest::Method::GET, &path, None).await?;
        Ok(compact_doc(&doc))
    }

    /// Create a doc room. `content` becomes its first version.
    async fn create_doc(
        &self,
        name: &str,
        content: Option<&str>,
        description: Option<&str>,
    ) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/rooms", self.space_id().await);
        let mut body = json!({ "name": name, "room_type": "doc" });
        if let Some(c) = content.filter(|c| !c.trim().is_empty()) {
            body["content"] = json!(c);
        }
        if let Some(d) = description.filter(|d| !d.trim().is_empty()) {
            body["description"] = json!(d);
        }
        let meta = self
            .request(reqwest::Method::POST, &path, Some(body))
            .await?;
        let kind = &meta["kind"];
        Ok(json!({
            "room_id": meta["id"],
            "name": meta["name"],
            "status": kind["status"],
            "base_version": kind["current_version"],
            "version_count": kind["version_count"],
        }))
    }

    /// Save a new version. On a conflict (someone saved since `base_version`)
    /// nothing is written and the answer carries the latest version, so the
    /// agent can merge and save again against it.
    async fn update_doc(
        &self,
        room_id: &str,
        content: &str,
        base_version: Option<u64>,
    ) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/docs/{}/content",
            self.space_id().await,
            urlencoding::encode(room_id)
        );
        let body = json!({ "content": content, "base_version": base_version });
        let (status, answer) = self
            .request_with_status(reqwest::Method::PUT, &path, Some(body))
            .await?;
        if status == reqwest::StatusCode::CONFLICT {
            let latest = self.get_doc(room_id, None).await?;
            return Ok(json!({
                "saved": false,
                "conflict": "Someone saved a newer version since base_version. Nothing was written. Merge your change into the latest content below, then save again with its base_version.",
                "latest": latest,
            }));
        }
        if !status.is_success() {
            return Err(format!("HTTP {}: {}", status, answer));
        }
        Ok(json!({
            "saved": true,
            "version": answer["version"]["number"],
            "base_version": answer["version"]["index"],
        }))
    }

    /// `request`, but a non-2xx answer comes back with its status instead of
    /// as an error, for callers that act on a specific status.
    async fn request_with_status(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(reqwest::StatusCode, Value), String> {
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
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        Ok((status, value))
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

    /// Who this token belongs to, asked of the engine once and then cached.
    ///
    /// `None` when the lookup fails. Not knowing who we are is a reason to
    /// leave a message's mentions exactly as written, never to fail the send.
    async fn self_id(&self) -> Option<String> {
        if let Some(id) = self.config.read().await.agent_id.clone() {
            return Some(id);
        }

        let me = self
            .request(reqwest::Method::GET, "/v1/me", None)
            .await
            .ok()?;
        let id = me["agent_id"].as_str()?.to_string();
        self.config.write().await.agent_id = Some(id.clone());
        Some(id)
    }

    async fn list_rooms(&self, kind: Option<&str>) -> Result<Value, String> {
        let mut path = format!("/v1/spaces/{}/rooms", self.space_id().await);
        if let Some(k) = kind {
            path = format!("{}?kind={}", path, urlencoding::encode(k));
        }
        self.request(reqwest::Method::GET, &path, None).await
    }

    /// Skill rooms the caller belongs to, optionally narrowed by status.
    ///
    /// The engine filters on `kind` and `status` itself, but an engine that
    /// predates either filter ignores the query string and returns every room,
    /// so the result is filtered again here on the room's `kind.type` and
    /// `kind.status`.
    async fn list_skills(&self, status: Option<&str>) -> Result<Value, String> {
        let mut path = format!("/v1/spaces/{}/rooms?kind=skill", self.space_id().await);
        if let Some(s) = status {
            path = format!("{}&status={}", path, urlencoding::encode(s));
        }
        let rooms = self.request(reqwest::Method::GET, &path, None).await?;
        Ok(filter_skill_rooms(rooms, status))
    }

    async fn get_skill_prompt(&self, room_id: &str) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/skills/{}/prompt",
            self.space_id().await,
            urlencoding::encode(room_id)
        );
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
        mut mentions: Vec<String>,
        mut expect_reply: Option<Vec<String>>,
    ) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/rooms/{}/messages",
            self.space_id().await,
            room_id
        );
        let message_id = uuid::Uuid::new_v4().to_string();

        // Naming yourself wakes nobody — every client drops its own messages
        // before reading them — but it does read to the room as though one
        // more participant had been asked. Drop it before the merge below,
        // which would otherwise put it straight back.
        if let Some(me) = self.self_id().await {
            mentions.retain(|m| !same_agent(m, &me));
            expect_reply =
                expect_reply.map(|ids| ids.into_iter().filter(|id| !same_agent(id, &me)).collect());
        }

        // Someone named in expect_reply but not in mentions would never be
        // delivered the message, so they could never reply.
        if let Some(ref ids) = expect_reply {
            for id in ids {
                let Some(name) = mention_name(id) else {
                    continue;
                };
                if !mentions.iter().any(|m| same_agent(m, &name)) {
                    mentions.push(name);
                }
            }
        }

        let mut body = json!({
            "message_id": message_id,
            "parts": [{ "kind": "text", "text": text }],
            "mentions": mentions
        });
        let mut metadata = serde_json::Map::new();
        if let Some(ids) = expect_reply {
            metadata.insert("expect_reply".into(), json!(ids));
        }
        // Proof for the engine that the sender read the current mandate. Only
        // a hash this session actually showed the model is ever sent.
        if let Some(hash) = self.mandate.lock().await.stamp() {
            metadata.insert("mandate_hash".into(), json!(hash));
        }
        if !metadata.is_empty() {
            body["metadata"] = Value::Object(metadata);
        }

        self.request(reqwest::Method::POST, &path, Some(body)).await
    }

    async fn poll(
        &self,
        room_id: &str,
        from_index: Option<u64>,
        before_index: Option<u64>,
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
        if let Some(idx) = before_index {
            params.push(format!("before_index={}", idx));
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
        query: &str,
        room_ids: Option<Vec<String>>,
        limit: Option<u64>,
        cursor: Option<String>,
    ) -> Result<Value, String> {
        let path = format!("/v1/spaces/{}/search", self.space_id().await);

        // Split query into terms (whitespace-separated)
        let terms: Vec<&str> = query.split_whitespace().collect();
        if terms.is_empty() {
            return Err("query cannot be empty".to_string());
        }

        let mut body = json!({ "terms": terms });
        if let Some(ids) = room_ids {
            if !ids.is_empty() {
                body["room_ids"] = json!(ids);
            }
        }
        if let Some(l) = limit {
            body["limit"] = json!(l);
        }
        if let Some(c) = cursor {
            body["cursor"] = json!(c);
        }

        self.request(reqwest::Method::POST, &path, Some(body)).await
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

    async fn create_thread(
        &self,
        room_id: &str,
        message_index: u64,
        name: Option<&str>,
    ) -> Result<Value, String> {
        let path = format!(
            "/v1/spaces/{}/rooms/{}/messages/{}/thread",
            self.space_id().await,
            urlencoding::encode(room_id),
            message_index
        );
        let body = match name {
            Some(n) => json!({ "name": n }),
            None => json!({}),
        };
        self.request(reqwest::Method::POST, &path, Some(body)).await
    }

    async fn reload_token(&self) -> Result<(), String> {
        let mut config = self.config.write().await;
        config.reload_token()
    }
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
    mandate: &Arc<Mutex<MandateState>>,
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
    let mandate = mandate.clone();
    let handle =
        tokio::spawn(
            async move { run_subscription(cfg, target, mentioned_only, tx, mandate).await },
        );

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
    mandate: Arc<Mutex<MandateState>>,
) {
    let client = stream_client();
    let mut last_event_id: Option<String> = None;
    let mut delay_ms = RECONNECT_MIN_DELAY_MS;

    loop {
        let outcome = stream_once(
            &client,
            &config,
            &target,
            mentioned_only,
            &mut last_event_id,
            &notify_tx,
            &mandate,
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

/// `config` is shared rather than snapshotted because the stream can outlive
/// what was known when it connected: a startup subscription opens before
/// `initialize` has asked the engine who this token is, and `mandate_update`
/// events must be matched against that answer once it exists.
async fn stream_once(
    client: &reqwest::Client,
    config: &RwLock<Config>,
    target: &Target,
    mentioned_only: bool,
    last_event_id: &mut Option<String>,
    notify_tx: &mpsc::UnboundedSender<Value>,
    mandate: &Mutex<MandateState>,
) -> Outcome {
    let (url, token) = {
        let cfg = config.read().await;
        (target.url(&cfg, mentioned_only), cfg.token.clone())
    };

    let mut req = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", token))
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

            // The engine recomputed this agent's mandate. Whatever the model
            // was served is no longer proof of anything, so the stamp stops
            // until it reads the new one.
            if event_type == "mandate_update" {
                let me = config.read().await.agent_id.clone();
                if let Some(note) = apply_mandate_update(mandate, me.as_deref(), &parsed).await {
                    let _ = notify_tx.send(note);
                }
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

/// `mandate` is the caller's resolved mandate, if any; it is appended to the
/// instructions so the model meets it before its first tool call.
fn initialize_result(params: &Value, mandate: Option<&Value>) -> Value {
    json!({
        "protocolVersion": negotiate_version(params),
        "capabilities": {
            "tools": {},
            "experimental": {
                "claude/channel": {}
            }
        },
        "serverInfo": { "name": "nubi-mcp", "version": "0.1.0" },
        "instructions": instructions_with_mandate(mandate)
    })
}

/// Doc rooms from a room list, narrowed by status. Filters locally too: an
/// engine without `?kind=doc` returns every room.
fn compact_doc_list(rooms: Value, status: Option<&str>) -> Value {
    let docs: Vec<Value> = rooms
        .as_array()
        .map(|all| {
            all.iter()
                .filter(|r| r["kind"]["type"] == "doc")
                .filter(|r| status.is_none_or(|s| r["kind"]["status"] == s))
                .map(|r| {
                    json!({
                        "room_id": r["id"],
                        "name": r["name"],
                        "status": r["kind"]["status"],
                        "version_count": r["kind"]["version_count"],
                        "updated_at_ms": r["kind"]["updated_at_ms"],
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Value::Array(docs)
}

/// A doc as an agent reads it: the content plus the facts needed to judge
/// and to save against it. `base_version` is what `nubi_update_doc` takes.
fn compact_doc(doc: &Value) -> Value {
    let kind = &doc["room"]["kind"];
    let version = &doc["version"];
    let current = kind["current_version"].as_u64();
    let approved = kind["approved_version"].as_u64();
    let this = version["index"].as_u64();
    json!({
        "room_id": doc["room"]["id"],
        "name": doc["room"]["name"],
        "status": kind["status"],
        "reviewers": kind["reviewers"],
        "version": version["number"],
        "author": version["author"],
        "saved_at_ms": version["saved_at_ms"],
        // Save against the latest version, not an old one being read
        "base_version": current,
        "is_latest": this == current,
        "changed_since_approval": kind["status"] == "approved" && current.is_some() && approved != current,
        "content": doc["content"],
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
                "name": "nubi_list_skills",
                "description": "List skills you have access to",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "enum": ["draft", "published", "archived"], "description": "Only skills in this status" }
                    }
                }
            },
            {
                "name": "nubi_get_skill",
                "description": "Get a skill's content/prompt. Returns the full skill definition ready to save to .claude/skills/",
                "inputSchema": {
                    "type": "object",
                    "properties": { "room_id": { "type": "string", "description": "The skill room id" } },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_get_mandate",
                "description": "Read your mandate: the skills you are bound to follow, in order, with their full content and a hash. Call it before your first message and whenever you are told the mandate changed; your messages only carry proof that you read the current mandate after you have called this.",
                "inputSchema": { "type": "object", "properties": {} }
            },
            {
                "name": "nubi_list_skill_sets",
                "description": "List skill sets you are a member of or are bound to",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "enum": ["draft", "published", "archived"], "description": "Only skill sets in this status" }
                    }
                }
            },
            {
                "name": "nubi_get_skill_set",
                "description": "Get a skill set: its details and the skills it holds (names and statuses, not content)",
                "inputSchema": {
                    "type": "object",
                    "properties": { "set_id": { "type": "string", "description": "The skill set id" } },
                    "required": ["set_id"]
                }
            },
            {
                "name": "nubi_list_docs",
                "description": "List docs you can read: specs, proposals and notes written and reviewed by people and agents. Each has a review status (draft, in_review, approved, archived).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "status": { "type": "string", "enum": ["draft", "in_review", "approved", "archived"], "description": "Only docs in this status" }
                    }
                }
            },
            {
                "name": "nubi_get_doc",
                "description": "Read a doc as Markdown, with its review status and version. Use an approved doc as the source of truth for what to build. Returns base_version, which nubi_update_doc needs. Discussion about the doc happens in its room (nubi_poll, nubi_send).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string", "description": "The doc room id" },
                        "version": { "type": "integer", "description": "An older version to read, by its index from the doc's history. Omit for the latest." }
                    },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_create_doc",
                "description": "Create a doc (for example a spec drafted from a conversation). Content is Markdown and becomes version 1. Docs start as drafts; people move them to review and approval.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string", "description": "The doc's title" },
                        "content": { "type": "string", "description": "The doc as Markdown: headings, lists, checklists, tables, quotes, code and links" },
                        "description": { "type": "string", "description": "One line on what the doc covers (optional)" }
                    },
                    "required": ["name"]
                }
            },
            {
                "name": "nubi_update_doc",
                "description": "Save a new version of a doc: send the whole Markdown, not a diff. Pass base_version from nubi_get_doc. If someone saved in between, nothing is written and you get the latest content and its base_version: merge your change into it and save again.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string", "description": "The doc room id" },
                        "content": { "type": "string", "description": "The whole doc as Markdown" },
                        "base_version": { "type": ["integer", "null"], "description": "The base_version you read with nubi_get_doc. null only for a doc with no versions yet." }
                    },
                    "required": ["room_id", "content", "base_version"]
                }
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
                "description": "Send a message to a room, and the only way to ask a teammate for something. A message reaches exactly the agents named in mentions: say nothing there and nobody receives it. Whenever the message is a question, a request or a handoff, name who should answer in expect_reply as well, or it lands as something everyone can leave for someone else.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "text": { "type": "string" },
                        "mentions": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Agent ids who should see this message. Use 'here' for everyone in the room. Nobody reads a message they are not mentioned in."
                        },
                        "expect_reply": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Who is on the hook to answer. Set it on every message that needs a response: [\"apple\"] puts it on apple even when you mentioned others, who then get it as context. Leave it out (or pass []) for a note nobody should reply to, like an FYI, a status update or an answer that closes the exchange: then nobody answers."
                        }
                    },
                    "required": ["room_id", "text"]
                }
            },
            {
                "name": "nubi_poll",
                "description": "Read messages in a room. Call it with only room_id to get what you have not read yet. Returns {messages: [{index, from, at, text, mentions, expect_reply}], has_more, next_index, first_index}, about 4000 tokens at most unless you pass max_tokens. When has_more is true, call again with from_index=next_index; to read older history, pass before_index=first_index.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string" },
                        "from_index": { "type": "number", "description": "Read forwards starting at this index. Defaults to the first message you have not read. Cannot be combined with before_index." },
                        "before_index": { "type": "number", "description": "Read backwards: returns the messages immediately before this index. Use it to scroll up through history you already passed. Cannot be combined with from_index or wait_ms." },
                        "max_tokens": { "type": "number", "description": "Stop once the returned messages reach roughly this many tokens." },
                        "mentioned_only": { "type": "boolean", "description": "Only return messages that mention you. Defaults to false." },
                        "wait_ms": { "type": "number", "description": "Long-poll: wait up to this many milliseconds for a new message instead of returning empty. Cannot be combined with before_index." }
                    },
                    "required": ["room_id"]
                }
            },
            {
                "name": "nubi_search",
                "description": "Search messages across rooms. Returns matching messages with room context. Use for targeted retrieval instead of scrolling through history.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "Search query. Multiple words are AND-ed together." },
                        "room_id": { "type": "string", "description": "Single room to search. Use room_ids for multiple rooms." },
                        "room_ids": { "type": "array", "items": { "type": "string" }, "description": "Rooms to search. Omit to search all rooms you can read." },
                        "limit": { "type": "number", "description": "Max results to return (default 50)." },
                        "cursor": { "type": "string", "description": "Pagination cursor from previous response." }
                    },
                    "required": ["query"]
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
                "name": "nubi_create_thread",
                "description": "Create a thread on a message. Returns the thread room which you can then send messages to.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "room_id": { "type": "string", "description": "The room containing the message" },
                        "message_index": { "type": "number", "description": "The global index of the message to start a thread on" },
                        "name": { "type": "string", "description": "Optional thread name" }
                    },
                    "required": ["room_id", "message_index"]
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
        "nubi_list_rooms" => client.list_rooms(None).await,
        "nubi_list_skills" => client.list_skills(args["status"].as_str()).await,
        "nubi_get_skill" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            client.get_skill_prompt(room_id).await
        }
        "nubi_get_mandate" => client.get_mandate().await,
        "nubi_list_skill_sets" => client.list_skill_sets(args["status"].as_str()).await,
        "nubi_get_skill_set" => {
            let set_id = args["set_id"].as_str().ok_or("set_id required")?;
            client.get_skill_set(set_id).await
        }
        "nubi_list_docs" => client.list_docs(args["status"].as_str()).await,
        "nubi_get_doc" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            client.get_doc(room_id, args["version"].as_u64()).await
        }
        "nubi_create_doc" => {
            let name = args["name"].as_str().ok_or("name required")?;
            client
                .create_doc(name, args["content"].as_str(), args["description"].as_str())
                .await
        }
        "nubi_update_doc" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let content = args["content"].as_str().ok_or("content required")?;
            if content.trim().is_empty() {
                return Err("content is empty: send the whole doc as Markdown".to_string());
            }
            // Required but nullable: absent is a mistake, null means "no versions yet"
            let base_version = match args.get("base_version") {
                None => return Err("base_version required: read it with nubi_get_doc".to_string()),
                Some(Value::Null) => None,
                Some(v) => Some(
                    v.as_u64()
                        .ok_or("base_version must be a version index or null")?,
                ),
            };
            client.update_doc(room_id, content, base_version).await
        }
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
            // Left out means nobody is on the hook, the same as an agent's
            // ordinary reply (nubi-agent posts those with []). Absent would let
            // every mentioned agent answer, and two agents mentioning each
            // other without it answer each other for ever.
            let expect_reply: Option<Vec<String>> = match args.get("expect_reply") {
                None | Some(Value::Null) => Some(Vec::new()),
                Some(Value::Array(arr)) => Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                ),
                Some(_) => return Err("expect_reply must be an array of agent ids".to_string()),
            };
            client
                .send_message(room_id, text, mentions, expect_reply)
                .await
        }
        "nubi_poll" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let from_index = args["from_index"].as_u64();
            let before_index = args["before_index"].as_u64();
            let max_tokens = args["max_tokens"].as_u64().map(|v| v as u32);
            let mentioned_only = args["mentioned_only"].as_bool().unwrap_or(false);
            let wait_ms = args["wait_ms"].as_u64();
            client
                .poll(
                    room_id,
                    from_index,
                    before_index,
                    max_tokens,
                    mentioned_only,
                    wait_ms,
                )
                .await
                .map(compact_poll)
        }
        "nubi_search" => {
            let query = args["query"].as_str().ok_or("query required")?;
            // Support both single room_id and room_ids array
            let room_ids: Option<Vec<String>> = match (args.get("room_ids"), args.get("room_id")) {
                (Some(Value::Array(arr)), _) => Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect(),
                ),
                (_, Some(Value::String(id))) => Some(vec![id.clone()]),
                _ => None,
            };
            let limit = args["limit"].as_u64();
            let cursor = args["cursor"].as_str().map(String::from);
            client.search(query, room_ids, limit, cursor).await
        }
        "nubi_list_agents" => client.list_agents().await,
        "nubi_invite" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let agent_id = args["agent_id"].as_str().ok_or("agent_id required")?;
            let role = args["role"].as_str().unwrap_or("member");
            client.invite_member(room_id, agent_id, role).await
        }
        "nubi_create_thread" => {
            let room_id = args["room_id"].as_str().ok_or("room_id required")?;
            let message_index = args["message_index"]
                .as_u64()
                .ok_or("message_index required")?;
            let name = args["name"].as_str();
            client.create_thread(room_id, message_index, name).await
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
                &client.mandate,
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

/// Runs a tool and wraps its result for MCP. While a mandate exists, a
/// successful result gets the one-line mandate header as its own leading
/// text block, so the tool's JSON stays exactly what it would be without a
/// mandate; errors carry no header. Shared by stdio and hosted mode.
async fn call_tool(
    client: &NubiClient,
    name: &str,
    args: &Value,
    config: &Arc<RwLock<Config>>,
    subscriptions: &Subscriptions,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> Value {
    // nubi_get_mandate resolves the mandate itself, and its body already
    // says everything the header would.
    let is_mandate_tool = name == "nubi_get_mandate";
    if !is_mandate_tool {
        client.refresh_mandate_if_due().await;
    }

    let result = handle_tool_call(client, name, args, config, subscriptions, notify_tx).await;
    let (text, is_error) = match result {
        // Compact: the model reads it fine, and indentation is pure cost.
        Ok(v) => (serde_json::to_string(&v).unwrap(), false),
        Err(e) => (format!("Error: {}", e), true),
    };

    let header = if is_mandate_tool || is_error {
        None
    } else {
        client.mandate.lock().await.header()
    };
    tool_text_result_with_header(header, text, is_error)
}

async fn handle_request(
    client: &NubiClient,
    req: JsonRpcRequest,
    config: &Arc<RwLock<Config>>,
    subscriptions: &Subscriptions,
    notify_tx: &mpsc::UnboundedSender<Value>,
) -> Option<JsonRpcResponse> {
    let result = match req.method.as_str() {
        "initialize" => {
            let mandate = client.mandate_for_initialize().await;
            Ok(initialize_result(&req.params, mandate.as_ref()))
        }
        "notifications/initialized" => return None,
        "ping" => Ok(json!({})),
        "tools/list" => Ok(tools_list(false)),
        "tools/call" => {
            let name = req.params["name"].as_str().unwrap_or("");
            let args = &req.params["arguments"];
            Ok(call_tool(client, name, args, config, subscriptions, notify_tx).await)
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
    /// token -> who it belongs to, filled from GET /v1/me on first use.
    /// Only successfully authenticated tokens are cached, so size is
    /// bounded by the number of valid keys in the engine.
    me_cache: Arc<RwLock<HashMap<String, Me>>>,
}

#[derive(Clone)]
struct Me {
    space_id: String,
    agent_id: String,
    /// The token's mandate as this server last saw it, and whether the model
    /// behind the token was served it. Shared by every request on the token,
    /// since hosted mode keeps no other session.
    mandate: Arc<Mutex<MandateState>>,
}

impl Me {
    /// A client acting as this token, sharing its mandate state.
    fn client(&self, state: &HttpState, token: &str) -> (NubiClient, Arc<RwLock<Config>>) {
        let config = Arc::new(RwLock::new(Config {
            base_url: state.base_url.clone(),
            token: token.to_string(),
            space_id: self.space_id.clone(),
            agent_id: Some(self.agent_id.clone()),
            token_file: None,
            subscribe: None,
        }));
        let client =
            NubiClient::with_mandate(config.clone(), state.http.clone(), self.mandate.clone());
        (client, config)
    }
}

async fn resolve_me(state: &HttpState, token: &str) -> Result<Me, String> {
    if let Some(me) = state.me_cache.read().await.get(token) {
        return Ok(me.clone());
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

    let body: Value = res.json().await.map_err(|e| e.to_string())?;
    let me = Me {
        space_id: body["space_id"]
            .as_str()
            .ok_or("engine /v1/me returned no space_id")?
            .to_string(),
        agent_id: body["agent_id"]
            .as_str()
            .ok_or("engine /v1/me returned no agent_id")?
            .to_string(),
        mandate: Arc::default(),
    };

    // Two first requests on one token can race here; keep whichever entry
    // landed first so they share one mandate state.
    let me = state
        .me_cache
        .write()
        .await
        .entry(token.to_string())
        .or_insert(me)
        .clone();

    Ok(me)
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

/// The engine's poll response, trimmed to what a model reads: per message
/// its index, sender, time, text and routing. Dropped: the per-message id,
/// payload type, token count, the parts nesting around plain text, and
/// bookkeeping metadata (usage, mandate hash). Anything not recognised is
/// kept, so a new field is never silently lost.
fn compact_poll(v: Value) -> Value {
    let Some(entries) = v.get("entries").and_then(Value::as_array) else {
        return v;
    };
    let mut out = serde_json::Map::new();
    out.insert(
        "messages".into(),
        Value::Array(entries.iter().map(compact_entry).collect()),
    );
    for key in ["has_more", "next_index", "first_index"] {
        if let Some(value) = v.get(key) {
            out.insert(key.into(), value.clone());
        }
    }
    Value::Object(out)
}

fn compact_entry(e: &Value) -> Value {
    const KNOWN: [&str; 5] = [
        "global_index",
        "sender",
        "timestamp_ms",
        "payload",
        "token_count",
    ];
    const NOISE: [&str; 2] = ["usage", "mandate_hash"];
    let mut m = serde_json::Map::new();
    m.insert("index".into(), e["global_index"].clone());
    m.insert("from".into(), e["sender"].clone());
    if let Some(ts) = e["timestamp_ms"].as_u64() {
        m.insert("at".into(), Value::String(iso_utc(ts)));
    }
    let payload = &e["payload"];
    match payload["type"].as_str() {
        Some("message") | None => {
            let parts = payload["parts"].as_array();
            let plain = parts.is_none_or(|ps| ps.iter().all(|p| p["kind"] == "text"));
            if plain {
                let text: String = parts
                    .into_iter()
                    .flatten()
                    .filter_map(|p| p["text"].as_str())
                    .collect();
                m.insert("text".into(), Value::String(text));
            } else {
                m.insert("parts".into(), payload["parts"].clone());
            }
            if let Some(mentions) = payload["mentions"].as_array().filter(|a| !a.is_empty()) {
                m.insert("mentions".into(), Value::Array(mentions.clone()));
            }
            let mut rest = serde_json::Map::new();
            for (k, val) in payload["metadata"].as_object().into_iter().flatten() {
                match k.as_str() {
                    "expect_reply" | "no_reply" => {
                        m.insert(k.clone(), val.clone());
                    }
                    k if NOISE.contains(&k) => {}
                    _ => {
                        rest.insert(k.clone(), val.clone());
                    }
                }
            }
            if !rest.is_empty() {
                m.insert("metadata".into(), Value::Object(rest));
            }
            for (k, val) in payload.as_object().into_iter().flatten() {
                if !["type", "parts", "mentions", "metadata", "message_id"].contains(&k.as_str()) {
                    m.insert(k.clone(), val.clone());
                }
            }
        }
        // System entries (member_joined, ...): say what happened.
        Some(_) => {
            let mut event = payload.as_object().cloned().unwrap_or_default();
            event.remove("type");
            m.insert("event".into(), Value::Object(event));
        }
    }
    for (k, val) in e.as_object().into_iter().flatten() {
        if !KNOWN.contains(&k.as_str()) {
            m.insert(k.clone(), val.clone());
        }
    }
    Value::Object(m)
}

/// `2026-11-03T08:47:02Z` for a Unix time in milliseconds.
fn iso_utc(ms: u64) -> String {
    let secs = ms / 1000;
    let (days, rem) = ((secs / 86_400) as i64, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn tool_text_result(text: String, is_error: bool) -> Value {
    tool_text_result_with_header(None, text, is_error)
}

/// A tool result whose body is `text`, preceded by `header` as a separate
/// text block when there is one.
fn tool_text_result_with_header(header: Option<String>, text: String, is_error: bool) -> Value {
    let mut content = Vec::new();
    if let Some(header) = header {
        content.push(json!({ "type": "text", "text": header }));
    }
    content.push(json!({ "type": "text", "text": text }));
    let mut v = json!({ "content": content });
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
        "initialize" => {
            // A token the engine rejects still gets a handshake; the error
            // surfaces on the first tool call instead. Resolving the token
            // shares the mandate's time bound: either can hang on a slow
            // engine, and the handshake must not.
            let mandate = tokio::time::timeout(MANDATE_INIT_TIMEOUT, async {
                let me = resolve_me(state, token).await.ok()?;
                // A new conversation on a token that an earlier one used:
                // whatever that one read, this model has not.
                me.mandate.lock().await.served = None;
                match me.client(state, token).0.fetch_mandate().await {
                    Ok(body) => body,
                    Err(e) => {
                        eprintln!("nubi: mandate unavailable at initialize: {}", e);
                        None
                    }
                }
            })
            .await
            .unwrap_or_else(|_| {
                eprintln!("nubi: mandate fetch timed out at initialize");
                None
            });
            rpc_ok(req.id, initialize_result(&req.params, mandate.as_ref()))
        }
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

            let me = match resolve_me(state, token).await {
                Ok(m) => m,
                Err(e) => return rpc_ok(req.id, tool_text_result(format!("Error: {}", e), true)),
            };

            let (client, config) = me.client(state, token);

            // Hosted mode never spawns subscriptions; these satisfy the
            // shared handle_tool_call signature.
            let subscriptions: Subscriptions = Arc::new(Mutex::new(HashMap::new()));
            let (notify_tx, _notify_rx) = mpsc::unbounded_channel::<Value>();

            rpc_ok(
                req.id,
                call_tool(&client, name, args, &config, &subscriptions, &notify_tx).await,
            )
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
        http: http_client(),
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
            spawn_subscription(
                &subscriptions,
                &config,
                target,
                true,
                &notify_tx,
                &client.mandate,
            )
            .await;
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

    #[test]
    fn iso_utc_formats_unix_millis() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(iso_utc(951_782_400_000), "2000-02-29T00:00:00Z");
        assert_eq!(iso_utc(1_793_370_422_730), "2026-10-30T14:27:02Z");
    }

    /// A real poll entry shape from the engine, compacted.
    #[test]
    fn poll_results_keep_what_the_model_reads() {
        let engine = json!({
            "entries": [
                {"global_index": 285, "sender": "palm", "timestamp_ms": 1_793_370_422_730u64,
                 "token_count": 65,
                 "payload": {"type": "message", "message_id": "b8e37b49-6a9f-4560-a3e3-293f36870793",
                    "mentions": ["grape"],
                    "metadata": {"expect_reply": [], "usage": {"x": 1}, "mandate_hash": "h", "thread": "t1"},
                    "parts": [{"kind": "text", "text": "Reviewed "}, {"kind": "text", "text": "PR #101."}]}},
                {"global_index": 286, "sender": "system", "timestamp_ms": 1_793_370_422_731u64,
                 "token_count": 16,
                 "payload": {"type": "system", "kind": "member_joined", "agent_id": "melon"}},
                {"global_index": 287, "sender": "ada", "timestamp_ms": 0, "token_count": 3,
                 "payload": {"type": "message", "message_id": "m", "mentions": [],
                    "parts": [{"kind": "image", "url": "u"}]}}
            ],
            "from_index": 285, "next_index": 288, "first_index": 285, "tokens_used": 84, "has_more": true
        });
        let compact = compact_poll(engine.clone());
        assert_eq!(
            compact,
            json!({
                "messages": [
                    {"index": 285, "from": "palm", "at": "2026-10-30T14:27:02Z",
                     "text": "Reviewed PR #101.", "mentions": ["grape"], "expect_reply": [],
                     "metadata": {"thread": "t1"}},
                    {"index": 286, "from": "system", "at": "2026-10-30T14:27:02Z",
                     "event": {"kind": "member_joined", "agent_id": "melon"}},
                    {"index": 287, "from": "ada", "at": "1970-01-01T00:00:00Z",
                     "parts": [{"kind": "image", "url": "u"}]}
                ],
                "has_more": true, "next_index": 288, "first_index": 285
            })
        );
        let before = serde_json::to_string_pretty(&engine).unwrap().len();
        let after = serde_json::to_string(&compact).unwrap().len();
        assert!(after * 2 < before, "{after} vs {before}");
    }

    #[test]
    fn a_poll_result_without_entries_is_left_alone() {
        let v = json!({"error": "nope"});
        assert_eq!(compact_poll(v.clone()), v);
    }
    use axum::routing::get;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type ResumeLog = Arc<Mutex<Vec<Option<String>>>>;
    type FlakyState = (ResumeLog, Arc<AtomicUsize>);
    type SentBodies = Arc<Mutex<Vec<Value>>>;

    /// Accepts message POSTs and records each body so the test can inspect
    /// exactly what went on the wire.
    async fn spawn_message_sink() -> (NubiClient, SentBodies) {
        let sent: SentBodies = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/rooms/{room}/messages",
                axum::routing::post(
                    |State(sent): State<SentBodies>, axum::Json(body): axum::Json<Value>| async move {
                        sent.lock().await.push(body);
                        axum::Json(json!({ "ok": true }))
                    },
                ),
            )
            .with_state(sent.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("me".into()),
            token_file: None,
            subscribe: None,
        }));
        (NubiClient::new(config), sent)
    }

    #[tokio::test]
    async fn the_tool_without_expect_reply_asks_nobody() {
        let (client, sent) = spawn_message_sink().await;
        let args = json!({"room_id": "r1", "text": "done", "mentions": ["manager"]});
        let config = Arc::new(RwLock::new(Config {
            base_url: String::new(),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("me".into()),
            token_file: None,
            subscribe: None,
        }));
        let subs: Subscriptions = Arc::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        handle_tool_call(&client, "nubi_send", &args, &config, &subs, &tx)
            .await
            .unwrap();
        let body = &sent.lock().await[0];
        assert_eq!(body["metadata"], json!({ "expect_reply": [] }));
        assert_eq!(body["mentions"], json!(["manager"]));
    }

    #[tokio::test]
    async fn expect_reply_names_reach_mentions_in_a_form_the_engine_takes() {
        let (client, sent) = spawn_message_sink().await;
        client
            .send_message(
                "r1",
                "review this",
                vec!["apple".into()],
                Some(vec!["@Apple".into(), "Reviewer".into(), "not an id".into()]),
            )
            .await
            .unwrap();
        let body = &sent.lock().await[0];
        // @Apple is apple, already there; Reviewer goes in bare and
        // lowercase for the engine to resolve; a name with a space stays
        // out of mentions, where it would get the message refused.
        assert_eq!(body["mentions"], json!(["apple", "reviewer"]));
        assert_eq!(
            body["metadata"],
            json!({ "expect_reply": ["@Apple", "Reviewer", "not an id"] })
        );
    }

    #[tokio::test]
    async fn send_without_expect_reply_sends_no_metadata() {
        let (client, sent) = spawn_message_sink().await;
        client
            .send_message("r1", "hi", vec!["manager".into()], None)
            .await
            .unwrap();

        let body = &sent.lock().await[0];
        assert!(
            body.get("metadata").is_none(),
            "absent expect_reply must not add metadata: {}",
            body
        );
        assert_eq!(body["mentions"], json!(["manager"]));
    }

    #[tokio::test]
    async fn send_with_empty_expect_reply_asks_nobody() {
        let (client, sent) = spawn_message_sink().await;
        client
            .send_message("r1", "fyi", vec!["manager".into()], Some(vec![]))
            .await
            .unwrap();

        let body = &sent.lock().await[0];
        assert_eq!(body["metadata"], json!({ "expect_reply": [] }));
    }

    #[tokio::test]
    async fn send_adds_expect_reply_targets_to_mentions() {
        let (client, sent) = spawn_message_sink().await;
        client
            .send_message(
                "r1",
                "take this",
                vec!["manager".into()],
                Some(vec!["apple".into()]),
            )
            .await
            .unwrap();

        let body = &sent.lock().await[0];
        // apple was never mentioned, so without the union it would never see
        // the message it is being asked to answer.
        assert_eq!(body["mentions"], json!(["manager", "apple"]));
        assert_eq!(body["metadata"], json!({ "expect_reply": ["apple"] }));
    }

    #[tokio::test]
    async fn send_drops_self_from_mentions_and_expect_reply() {
        let (client, sent) = spawn_message_sink().await;
        client
            .send_message(
                "r1",
                "status",
                vec!["banana".into(), "@Me ".into()],
                Some(vec!["me".into(), "banana".into()]),
            )
            .await
            .unwrap();

        let body = &sent.lock().await[0];
        assert_eq!(body["mentions"], json!(["banana"]));
        assert_eq!(body["metadata"], json!({ "expect_reply": ["banana"] }));
    }

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
            agent_id: None,
            token_file: None,
            subscribe: None,
        }));
        let (tx, mut rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(run_subscription(
            config,
            Target::Room("r1".into()),
            false,
            tx,
            Arc::default(),
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
            agent_id: None,
            token_file: None,
            subscribe: None,
        }));
        let (tx, mut rx) = mpsc::unbounded_channel();

        run_subscription(config, Target::Room("r1".into()), false, tx, Arc::default()).await;

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

        let config = RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: None,
            token_file: None,
            subscribe: None,
        });
        let (tx, mut rx) = mpsc::unbounded_channel();

        stream_once(
            &reqwest::Client::new(),
            &config,
            &Target::Space,
            false,
            &mut None,
            &tx,
            &Mutex::default(),
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

    /// Spawns a mock server that records search requests and returns a canned response.
    async fn spawn_search_sink() -> (NubiClient, Arc<Mutex<Vec<Value>>>) {
        let requests: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/search",
                axum::routing::post(
                    |State(reqs): State<Arc<Mutex<Vec<Value>>>>,
                     axum::Json(body): axum::Json<Value>| async move {
                        reqs.lock().await.push(body);
                        axum::Json(json!({
                            "results": [
                                {
                                    "room_id": "room-1",
                                    "global_index": 42,
                                    "sender": "alice",
                                    "text": "found it",
                                    "timestamp": "2026-10-28T10:00:00Z"
                                }
                            ],
                            "has_more": false
                        }))
                    },
                ),
            )
            .with_state(requests.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "test-space".into(),
            agent_id: Some("me".into()),
            token_file: None,
            subscribe: None,
        }));
        (NubiClient::new(config), requests)
    }

    #[tokio::test]
    async fn search_posts_structured_payload() {
        let (client, requests) = spawn_search_sink().await;
        let result = client
            .search(
                "error timeout",
                Some(vec!["room-1".into(), "room-2".into()]),
                Some(25),
                None,
            )
            .await
            .unwrap();

        let req = &requests.lock().await[0];
        assert_eq!(req["terms"], json!(["error", "timeout"]));
        assert_eq!(req["room_ids"], json!(["room-1", "room-2"]));
        assert_eq!(req["limit"], json!(25));
        assert!(req.get("cursor").is_none());

        assert_eq!(result["results"][0]["room_id"], "room-1");
        assert_eq!(result["has_more"], false);
    }

    #[tokio::test]
    async fn search_omits_room_ids_when_none() {
        let (client, requests) = spawn_search_sink().await;
        client.search("keyword", None, None, None).await.unwrap();

        let req = &requests.lock().await[0];
        assert_eq!(req["terms"], json!(["keyword"]));
        assert!(req.get("room_ids").is_none());
    }

    #[tokio::test]
    async fn search_includes_cursor_for_pagination() {
        let (client, requests) = spawn_search_sink().await;
        client
            .search("query", None, None, Some("cursor123".into()))
            .await
            .unwrap();

        let req = &requests.lock().await[0];
        assert_eq!(req["cursor"], "cursor123");
    }

    /// An engine that ignores `?kind=` and `?status=` and returns every room,
    /// recording the query string it was asked for.
    async fn spawn_unfiltered_rooms() -> (NubiClient, Arc<Mutex<Vec<String>>>) {
        let queries: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/rooms",
                get(
                    |State(q): State<Arc<Mutex<Vec<String>>>>, uri: axum::http::Uri| async move {
                        q.lock().await.push(uri.query().unwrap_or("").to_string());
                        axum::Json(json!([
                            { "id": "g1", "name": "general", "kind": { "type": "group" } },
                            { "id": "s1", "name": "Refunds", "kind": { "type": "skill", "status": "published" } },
                            { "id": "s2", "name": "Tone", "kind": { "type": "skill", "status": "draft" } },
                            { "id": "t1", "name": "task", "kind": { "type": "task", "status": "published" } }
                        ]))
                    },
                ),
            )
            .with_state(queries.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("me".into()),
            token_file: None,
            subscribe: None,
        }));
        (NubiClient::new(config), queries)
    }

    #[tokio::test]
    async fn list_skills_matches_on_kind_type() {
        let (client, queries) = spawn_unfiltered_rooms().await;
        let skills = client.list_skills(None).await.unwrap();
        let ids: Vec<&str> = skills
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect();
        // master sent `?kind=skill` and trusted the engine to filter; an
        // engine that ignores the parameter returned every room. `kind` is
        // a tagged object, so the local check compares `kind.type`.
        assert_eq!(ids, vec!["s1", "s2"]);
        assert_eq!(queries.lock().await[0], "kind=skill");
    }

    #[tokio::test]
    async fn list_skills_filters_by_status() {
        let (client, queries) = spawn_unfiltered_rooms().await;
        let skills = client.list_skills(Some("published")).await.unwrap();
        assert_eq!(
            skills,
            json!([{ "id": "s1", "name": "Refunds", "kind": { "type": "skill", "status": "published" } }])
        );
        assert_eq!(queries.lock().await[0], "kind=skill&status=published");
    }

    // --- Mandate and skill sets ---

    /// A mock engine with skill sets: `/v1/me`, a mutable mandate (`None`
    /// answers 404), message and skill-set endpoints, recording what it saw.
    #[derive(Clone, Default)]
    struct MockEngine {
        mandate: Arc<Mutex<Option<Value>>>,
        if_none_match: Arc<Mutex<Vec<Option<String>>>>,
        sent: SentBodies,
        paths: Arc<Mutex<Vec<String>>>,
        /// Run once, by the next `/mandate` request before it answers: what
        /// a test wants to happen while that fetch is in flight.
        mid_flight: MidFlight,
    }

    type MidFlight = Arc<Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>>;

    impl MockEngine {
        async fn while_next_fetch_is_in_flight(
            &self,
            f: impl Future<Output = ()> + Send + 'static,
        ) {
            *self.mid_flight.lock().await = Some(Box::pin(f));
        }
    }

    fn mandate_body(hash: &str, content: &str) -> Value {
        json!({
            "agent_id": "bot",
            "hash": hash,
            "generated_at_ms": 1730000000000u64,
            "policy": { "skills_policy": "mandate_first", "enforce_mandate": false },
            "sets": [{ "set_id": "tier1", "name": "Tier 1 support", "revision": 7, "position": 0 }],
            "skills": [{
                "room_id": "refunds", "name": "Refund policy", "from_set": "tier1",
                "version_index": 14, "pinned": false, "required": true, "content": content
            }],
            "warnings": []
        })
    }

    async fn spawn_engine(mandate: Option<Value>) -> (String, MockEngine) {
        let engine = MockEngine::default();
        *engine.mandate.lock().await = mandate;

        let app = axum::Router::new()
            .route(
                "/v1/me",
                get(|| async { axum::Json(json!({ "agent_id": "bot", "space_id": "s" })) }),
            )
            .route(
                "/v1/spaces/{space}/agents/{agent}/mandate",
                get(
                    |State(e): State<MockEngine>, headers: HeaderMap| async move {
                        let inm = headers
                            .get(header::IF_NONE_MATCH)
                            .and_then(|v| v.to_str().ok())
                            .map(String::from);
                        e.if_none_match.lock().await.push(inm.clone());
                        if let Some(f) = e.mid_flight.lock().await.take() {
                            f.await;
                        }
                        let Some(body) = e.mandate.lock().await.clone() else {
                            return StatusCode::NOT_FOUND.into_response();
                        };
                        let etag = format!("\"{}\"", body["hash"].as_str().unwrap());
                        if inm.as_deref() == Some(etag.as_str()) {
                            return (StatusCode::NOT_MODIFIED, [(header::ETAG, etag)])
                                .into_response();
                        }
                        ([(header::ETAG, etag)], axum::Json(body)).into_response()
                    },
                ),
            )
            .route(
                "/v1/spaces/{space}/rooms/{room}/messages",
                axum::routing::post(
                    |State(e): State<MockEngine>, axum::Json(body): axum::Json<Value>| async move {
                        e.sent.lock().await.push(body);
                        axum::Json(json!({ "ok": true }))
                    },
                ),
            )
            .route(
                "/v1/spaces/{space}/rooms",
                get(|| async { axum::Json(json!([])) }),
            )
            .route(
                "/v1/spaces/{space}/skill-sets",
                get(
                    |State(e): State<MockEngine>, uri: axum::http::Uri| async move {
                        e.paths.lock().await.push(uri.to_string());
                        axum::Json(json!([{ "id": "tier1", "name": "Tier 1 support" }]))
                    },
                ),
            )
            .route(
                "/v1/spaces/{space}/skill-sets/{set}",
                get(
                    |State(e): State<MockEngine>, uri: axum::http::Uri| async move {
                        e.paths.lock().await.push(uri.to_string());
                        axum::Json(json!({ "id": "tier1", "skills": [] }))
                    },
                ),
            )
            .with_state(engine.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{}", addr), engine)
    }

    fn stdio_client(base_url: &str) -> (NubiClient, Arc<RwLock<Config>>) {
        let config = Arc::new(RwLock::new(Config {
            base_url: base_url.to_string(),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("bot".into()),
            token_file: None,
            subscribe: None,
        }));
        (NubiClient::new(config.clone()), config)
    }

    /// Calls a tool the way the stdio loop does and returns the full result.
    async fn call_result(
        client: &NubiClient,
        config: &Arc<RwLock<Config>>,
        name: &str,
        args: Value,
    ) -> Value {
        let subs: Subscriptions = Arc::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        call_tool(client, name, &args, config, &subs, &tx).await
    }

    /// Calls a tool and returns the tool's own text: the last content block,
    /// which is the JSON (or error) whether or not a mandate header leads.
    async fn call(
        client: &NubiClient,
        config: &Arc<RwLock<Config>>,
        name: &str,
        args: Value,
    ) -> String {
        let result = call_result(client, config, name, args).await;
        let blocks = result["content"].as_array().unwrap();
        blocks.last().unwrap()["text"].as_str().unwrap().to_string()
    }

    /// The text blocks of a tool result.
    fn texts(result: &Value) -> Vec<&str> {
        result["content"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b["text"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn get_mandate_sends_if_none_match_and_reuses_the_body_on_304() {
        let (base, engine) =
            spawn_engine(Some(mandate_body("sha256:aaa", "Refund within 30 days."))).await;
        let (client, _) = stdio_client(&base);

        let first = client.get_mandate().await.unwrap();
        let second = client.get_mandate().await.unwrap();

        assert_eq!(first["hash"], "sha256:aaa");
        assert_eq!(second, first, "a 304 must hand back the cached body");
        assert_eq!(
            *engine.if_none_match.lock().await,
            vec![None, Some("\"sha256:aaa\"".to_string())]
        );
    }

    #[tokio::test]
    async fn missing_mandate_endpoint_means_no_mandate() {
        let (base, engine) = spawn_engine(None).await;
        let (client, config) = stdio_client(&base);

        let body = client.get_mandate().await.unwrap();
        assert_eq!(body["mandate"], Value::Null);

        let result = call_result(
            &client,
            &config,
            "nubi_send",
            json!({ "room_id": "r1", "text": "hi" }),
        )
        .await;
        assert_eq!(
            texts(&result).len(),
            1,
            "no header without a mandate: {}",
            result
        );
        assert!(engine.sent.lock().await[0]["metadata"]
            .get("mandate_hash")
            .is_none());
    }

    #[test]
    fn tools_list_includes_the_mandate_and_skill_set_tools_in_both_modes() {
        for hosted in [false, true] {
            let list = tools_list(hosted);
            let names: Vec<&str> = list["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .collect();
            for tool in [
                "nubi_get_mandate",
                "nubi_list_skill_sets",
                "nubi_get_skill_set",
                "nubi_get_skill",
                "nubi_list_skills",
                "nubi_list_docs",
                "nubi_get_doc",
                "nubi_create_doc",
                "nubi_update_doc",
            ] {
                assert!(
                    names.contains(&tool),
                    "{} missing (hosted={})",
                    tool,
                    hosted
                );
            }
        }
    }

    #[tokio::test]
    async fn header_is_a_separate_block_before_untouched_json() {
        let (base, _engine) =
            spawn_engine(Some(mandate_body("sha256:0123456789abcdef", "x"))).await;
        let (client, config) = stdio_client(&base);

        let unread = call_result(&client, &config, "nubi_list_rooms", json!({})).await;
        assert_eq!(
            texts(&unread),
            vec![
                "[mandate hash sha256:0123456789ab; call nubi_get_mandate if you have not read it]",
                "[]"
            ],
            "the header leads as its own block; the JSON block is byte-identical"
        );
        assert!(unread.get("isError").is_none());

        let body = call_result(&client, &config, "nubi_get_mandate", json!({})).await;
        assert_eq!(texts(&body).len(), 1, "nubi_get_mandate carries no header");
        assert!(texts(&body)[0].starts_with('{'));

        let read = call_result(&client, &config, "nubi_list_rooms", json!({})).await;
        assert_eq!(
            texts(&read),
            vec!["[mandate hash sha256:0123456789ab]", "[]"]
        );

        // Errors are not where the header belongs.
        let err = call_result(&client, &config, "nubi_get_skill", json!({})).await;
        assert_eq!(err["isError"], true);
        assert_eq!(texts(&err), vec!["Error: room_id required"]);

        let (base, _engine) = spawn_engine(None).await;
        let (client, config) = stdio_client(&base);
        let none = call_result(&client, &config, "nubi_list_rooms", json!({})).await;
        assert_eq!(texts(&none), vec!["[]"]);
    }

    #[tokio::test]
    async fn send_stamps_the_mandate_hash_only_after_it_was_served() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, config) = stdio_client(&base);
        let send = json!({ "room_id": "r1", "text": "hi" });

        // The tool call resolves the mandate (for the header), but the model
        // has not read it, so there is nothing honest to stamp.
        call(&client, &config, "nubi_send", send.clone()).await;
        call(&client, &config, "nubi_get_mandate", json!({})).await;
        call(&client, &config, "nubi_send", send.clone()).await;

        // The engine moves on; the next check notices and the stamp stops.
        *engine.mandate.lock().await = Some(mandate_body("sha256:bbb", "y"));
        client.mandate.lock().await.checked_at = None;
        call(&client, &config, "nubi_send", send.clone()).await;

        let sent = engine.sent.lock().await;
        assert!(
            sent[0]["metadata"].get("mandate_hash").is_none(),
            "stamped before served: {}",
            sent[0]
        );
        assert_eq!(
            sent[1]["metadata"],
            json!({ "expect_reply": [], "mandate_hash": "sha256:aaa" })
        );
        assert!(
            sent[2]["metadata"].get("mandate_hash").is_none(),
            "stamped a hash the model never saw: {}",
            sent[2]
        );
    }

    #[tokio::test]
    async fn stamp_sits_beside_expect_reply() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);
        client.get_mandate().await.unwrap();
        client
            .send_message("r1", "?", vec!["apple".into()], Some(vec!["apple".into()]))
            .await
            .unwrap();
        assert_eq!(
            engine.sent.lock().await[0]["metadata"],
            json!({ "expect_reply": ["apple"], "mandate_hash": "sha256:aaa" })
        );
    }

    #[tokio::test]
    async fn mandate_update_on_the_stream_clears_the_stamp_and_notifies() {
        let (base, _engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);
        client.get_mandate().await.unwrap();
        assert_eq!(client.mandate.lock().await.stamp(), Some("sha256:aaa"));

        let body = format!(
            "event: mandate_update\ndata: {}\n\nevent: mandate_update\ndata: {}\n\n",
            json!({ "agent_id": "someone-else", "revision": 3, "hash": "sha256:zzz" }),
            json!({ "agent_id": "bot", "revision": 8, "hash": "sha256:bbb" })
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

        let config = RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("bot".into()),
            token_file: None,
            subscribe: None,
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        stream_once(
            &reqwest::Client::new(),
            &config,
            &Target::Space,
            true,
            &mut None,
            &tx,
            &client.mandate,
        )
        .await;
        drop(tx);

        let note = rx.recv().await.expect("mandate_update notification");
        assert_eq!(note["method"], "notifications/claude/channel");
        assert_eq!(note["params"]["meta"]["event"], "mandate_update");
        assert_eq!(note["params"]["meta"]["revision"], 8);
        assert!(note["params"]["content"]
            .as_str()
            .unwrap()
            .contains("call nubi_get_mandate"));
        assert!(
            rx.recv().await.is_none(),
            "the other agent's update must be ignored"
        );

        let state = client.mandate.lock().await;
        assert_eq!(
            state.stamp(),
            None,
            "served flag must clear on mandate_update"
        );
        assert_eq!(state.hash.as_deref(), Some("sha256:bbb"));
        assert!(state.checked_at.is_none(), "next tool call must re-resolve");
    }

    #[test]
    fn instructions_carry_the_mandate_only_when_there_is_one() {
        assert_eq!(instructions_with_mandate(None), BASE_INSTRUCTIONS);
        let empty = json!({ "hash": "sha256:e", "skills": [] });
        assert_eq!(instructions_with_mandate(Some(&empty)), BASE_INSTRUCTIONS);

        let text =
            instructions_with_mandate(Some(&mandate_body("sha256:aaa", "Refund within 30 days.")));
        assert!(text.starts_with(BASE_INSTRUCTIONS));
        assert!(text.contains("## Mandate — you must follow these"));
        assert!(text.contains("sha256:aaa"));
        assert!(text.contains("### Refund policy\n\nRefund within 30 days."));

        let huge = mandate_body(
            "sha256:aaa",
            &"x".repeat(MANDATE_INSTRUCTIONS_MAX_CHARS + 1),
        );
        let text = instructions_with_mandate(Some(&huge));
        assert!(text.contains("Skills: Refund policy. Call nubi_get_mandate"));
        assert!(!text.contains("xxxx"));
    }

    #[tokio::test]
    async fn stdio_initialize_includes_the_mandate() {
        let (base, _engine) = spawn_engine(Some(mandate_body("sha256:aaa", "Be kind."))).await;
        let (client, config) = stdio_client(&base);
        let subs: Subscriptions = Arc::default();
        let (tx, _rx) = mpsc::unbounded_channel();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(1),
            method: "initialize".into(),
            params: json!({}),
        };
        let resp = handle_request(&client, req, &config, &subs, &tx)
            .await
            .unwrap();
        let instructions = resp.result.unwrap()["instructions"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(instructions.contains("Be kind."));
        // Instructions are not proof the model read it.
        assert_eq!(client.mandate.lock().await.stamp(), None);
    }

    #[tokio::test]
    async fn hosted_mode_serves_and_stamps_per_token() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "Be kind."))).await;
        let state = HttpState {
            base_url: base,
            http: reqwest::Client::new(),
            me_cache: Arc::default(),
        };
        let rpc = |method: &str, params: Value| JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(1),
            method: method.into(),
            params,
        };
        let send = json!({ "name": "nubi_send", "arguments": { "room_id": "r1", "text": "hi" } });

        let init = handle_http_request(&state, "tok", rpc("initialize", json!({}))).await;
        assert!(init.result.unwrap()["instructions"]
            .as_str()
            .unwrap()
            .contains("Be kind."));

        let first = handle_http_request(&state, "tok", rpc("tools/call", send.clone())).await;
        let text = first.result.unwrap()["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(text.starts_with("[mandate hash sha256:aaa; call nubi_get_mandate"));

        handle_http_request(
            &state,
            "tok",
            rpc(
                "tools/call",
                json!({ "name": "nubi_get_mandate", "arguments": {} }),
            ),
        )
        .await;
        handle_http_request(&state, "tok", rpc("tools/call", send.clone())).await;
        // A different token is a different session: it was never served.
        handle_http_request(&state, "other", rpc("tools/call", send.clone())).await;

        let sent = engine.sent.lock().await;
        assert!(sent[0]["metadata"].get("mandate_hash").is_none());
        assert_eq!(
            sent[1]["metadata"],
            json!({ "expect_reply": [], "mandate_hash": "sha256:aaa" })
        );
        assert!(sent[2]["metadata"].get("mandate_hash").is_none());
    }

    #[tokio::test]
    async fn skill_set_tools_call_the_skill_set_endpoints() {
        let (base, engine) = spawn_engine(None).await;
        let (client, config) = stdio_client(&base);
        call(&client, &config, "nubi_list_skill_sets", json!({})).await;
        call(
            &client,
            &config,
            "nubi_list_skill_sets",
            json!({ "status": "published" }),
        )
        .await;
        let set = call(
            &client,
            &config,
            "nubi_get_skill_set",
            json!({ "set_id": "tier 1" }),
        )
        .await;
        assert!(set.contains("\"id\":\"tier1\""));
        assert_eq!(
            *engine.paths.lock().await,
            vec![
                "/v1/spaces/s/skill-sets",
                "/v1/spaces/s/skill-sets?status=published",
                "/v1/spaces/s/skill-sets/tier%201",
            ]
        );
    }

    #[tokio::test]
    async fn get_mandate_marks_served_with_the_hash_it_returned_not_a_newer_one() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);
        client.get_mandate().await.unwrap();
        assert_eq!(client.mandate.lock().await.stamp(), Some("sha256:aaa"));

        // While the conditional re-fetch is on the wire, the stream says the
        // mandate moved on to B. The engine still answers 304 for A.
        let m = client.mandate.clone();
        engine
            .while_next_fetch_is_in_flight(async move {
                let event = json!({ "agent_id": "bot", "revision": 9, "hash": "sha256:bbb" });
                apply_mandate_update(&m, Some("bot"), &event).await;
            })
            .await;
        let body = client.get_mandate().await.unwrap();
        assert_eq!(body["hash"], "sha256:aaa", "the model was shown A");

        let state = client.mandate.lock().await;
        assert_eq!(state.hash.as_deref(), Some("sha256:bbb"));
        assert_eq!(
            state.served.as_deref(),
            Some("sha256:aaa"),
            "served must be the hash of the body returned, never the newer one"
        );
        assert_eq!(state.stamp(), None, "A is no longer current, so no stamp");
        assert!(state.checked_at.is_none(), "next tool call must re-resolve");
    }

    #[tokio::test]
    async fn a_fetch_that_lost_the_race_does_not_overwrite_newer_state() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);

        // First ever fetch; B is announced before the (slow) A response lands.
        let m = client.mandate.clone();
        engine
            .while_next_fetch_is_in_flight(async move {
                let event = json!({ "agent_id": "bot", "revision": 2, "hash": "sha256:bbb" });
                apply_mandate_update(&m, Some("bot"), &event).await;
            })
            .await;
        let body = client.get_mandate().await.unwrap();
        assert_eq!(body["hash"], "sha256:aaa");
        {
            let state = client.mandate.lock().await;
            assert_eq!(state.hash.as_deref(), Some("sha256:bbb"), "A must not win");
            assert!(state.body.is_none(), "a stale body is not cached");
            assert_eq!(state.stamp(), None);
            assert!(state.checked_at.is_none());
        }

        // Once the engine serves B, everything lines up again.
        *engine.mandate.lock().await = Some(mandate_body("sha256:bbb", "y"));
        client.get_mandate().await.unwrap();
        assert_eq!(client.mandate.lock().await.stamp(), Some("sha256:bbb"));
    }

    #[tokio::test]
    async fn a_304_after_the_body_was_dropped_refetches_instead_of_erroring() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);
        client.get_mandate().await.unwrap();

        // A parallel check saw 404 and emptied the cache while this
        // conditional request was in flight; the engine answers it 304.
        let m = client.mandate.clone();
        engine
            .while_next_fetch_is_in_flight(async move { m.lock().await.set_absent() })
            .await;
        let body = client.fetch_mandate().await.unwrap();
        assert_eq!(body.unwrap()["hash"], "sha256:aaa");
        assert_eq!(
            *engine.if_none_match.lock().await,
            vec![None, Some("\"sha256:aaa\"".to_string()), None],
            "a 304 with nothing cached is followed by one unconditional fetch"
        );
        assert_eq!(
            client.mandate.lock().await.hash.as_deref(),
            Some("sha256:aaa")
        );
    }

    #[tokio::test]
    async fn mandate_update_without_a_hash_keeps_the_mandate_but_clears_the_stamp() {
        let (base, _engine) = spawn_engine(Some(mandate_body("sha256:aaa", "x"))).await;
        let (client, _) = stdio_client(&base);
        client.get_mandate().await.unwrap();

        let note = apply_mandate_update(
            &client.mandate,
            Some("bot"),
            &json!({ "agent_id": "bot", "revision": 4 }),
        )
        .await;
        assert!(note.is_some(), "the model is still told to re-read");

        let state = client.mandate.lock().await;
        assert_eq!(state.hash.as_deref(), Some("sha256:aaa"), "hash is kept");
        assert!(state.has_mandate(), "the header keeps showing");
        assert_eq!(state.stamp(), None);
        assert!(state.checked_at.is_none());
        assert_eq!(state.revision, Some(4));
    }

    #[tokio::test]
    async fn mandate_updates_are_ignored_until_self_is_known() {
        // Connection 1 arrives before /v1/me has answered; connection 2 after.
        let bodies = [
            format!(
                "event: mandate_update\ndata: {}\n\nevent: mandate_update\ndata: {}\n\n",
                json!({ "agent_id": "someone-else", "revision": 3, "hash": "sha256:zzz" }),
                json!({ "agent_id": "bot", "revision": 8, "hash": "sha256:bbb" })
            ),
            format!(
                "event: mandate_update\ndata: {}\n\nevent: mandate_update\ndata: {}\n\n",
                json!({ "agent_id": "someone-else", "revision": 5, "hash": "sha256:yyy" }),
                json!({ "agent_id": "bot", "revision": 9, "hash": "sha256:ccc" })
            ),
        ];
        let connections = Arc::new(AtomicUsize::new(0));
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/stream",
                get(|State(n): State<Arc<AtomicUsize>>| async move {
                    let i = n.fetch_add(1, Ordering::SeqCst);
                    Response::builder()
                        .header(header::CONTENT_TYPE, "text/event-stream")
                        .body(axum::body::Body::from(bodies[i.min(1)].clone()))
                        .unwrap()
                }),
            )
            .with_state(connections);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: None,
            token_file: None,
            subscribe: None,
        }));
        let mandate: Arc<Mutex<MandateState>> = Arc::default();
        mandate
            .lock()
            .await
            .set_resolved(mandate_body("sha256:aaa", "x"), None);
        mandate.lock().await.served = Some("sha256:aaa".into());
        let (tx, mut rx) = mpsc::unbounded_channel();

        stream_once(
            &reqwest::Client::new(),
            &config,
            &Target::Space,
            true,
            &mut None,
            &tx,
            &mandate,
        )
        .await;
        assert!(
            rx.try_recv().is_err(),
            "nothing is applied while we do not know who we are"
        );
        {
            let state = mandate.lock().await;
            assert_eq!(state.stamp(), Some("sha256:aaa"));
            assert!(state.checked_at.is_some());
        }

        // initialize resolved /v1/me in the meantime; the same stream picks
        // that up without reconnecting on a fresh config.
        config.write().await.agent_id = Some("bot".into());
        stream_once(
            &reqwest::Client::new(),
            &config,
            &Target::Space,
            true,
            &mut None,
            &tx,
            &mandate,
        )
        .await;
        drop(tx);
        let note = rx.recv().await.expect("own update applied once known");
        assert_eq!(note["params"]["meta"]["hash"], "sha256:ccc");
        assert!(
            rx.recv().await.is_none(),
            "the other agent's is still ignored"
        );
        assert_eq!(mandate.lock().await.hash.as_deref(), Some("sha256:ccc"));
    }

    #[tokio::test]
    async fn hosted_initialize_starts_a_new_conversation_unstamped() {
        let (base, engine) = spawn_engine(Some(mandate_body("sha256:aaa", "Be kind."))).await;
        let state = HttpState {
            base_url: base,
            http: reqwest::Client::new(),
            me_cache: Arc::default(),
        };
        let rpc = |method: &str, params: Value| JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(1),
            method: method.into(),
            params,
        };
        let send = json!({ "name": "nubi_send", "arguments": { "room_id": "r1", "text": "hi" } });
        let read = json!({ "name": "nubi_get_mandate", "arguments": {} });

        handle_http_request(&state, "tok", rpc("initialize", json!({}))).await;
        handle_http_request(&state, "tok", rpc("tools/call", read.clone())).await;
        handle_http_request(&state, "tok", rpc("tools/call", send.clone())).await;

        // The same token opens another conversation: the mandate is in the
        // instructions again, but this model has not read it.
        handle_http_request(&state, "tok", rpc("initialize", json!({}))).await;
        handle_http_request(&state, "tok", rpc("tools/call", send.clone())).await;
        handle_http_request(&state, "tok", rpc("tools/call", read)).await;
        handle_http_request(&state, "tok", rpc("tools/call", send)).await;

        let sent = engine.sent.lock().await;
        assert_eq!(
            sent[0]["metadata"],
            json!({ "expect_reply": [], "mandate_hash": "sha256:aaa" })
        );
        assert!(
            sent[1]["metadata"].get("mandate_hash").is_none(),
            "a new conversation must not inherit the stamp: {}",
            sent[1]
        );
        assert_eq!(
            sent[2]["metadata"],
            json!({ "expect_reply": [], "mandate_hash": "sha256:aaa" })
        );
    }

    #[tokio::test]
    async fn hosted_initialize_is_bounded_when_the_engine_hangs() {
        // Accepts connections into the backlog and never answers.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = HttpState {
            base_url: format!("http://{}", addr),
            http: http_client(),
            me_cache: Arc::default(),
        };
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: json!(1),
            method: "initialize".into(),
            params: json!({}),
        };

        let started = Instant::now();
        let resp = handle_http_request(&state, "tok", req).await;
        assert!(
            started.elapsed() < MANDATE_INIT_TIMEOUT + Duration::from_secs(2),
            "resolving the token must sit inside the initialize time bound"
        );
        assert_eq!(resp.result.unwrap()["instructions"], BASE_INSTRUCTIONS);
        drop(listener);
    }

    #[test]
    fn etags_and_hashes_normalise() {
        assert_eq!(unquote_etag("\"sha256:ab\""), "sha256:ab");
        assert_eq!(unquote_etag("W/\"sha256:ab\""), "sha256:ab");
        assert_eq!(unquote_etag("sha256:ab"), "sha256:ab");
        assert_eq!(short_hash("sha256:0123456789abcdef"), "sha256:0123456789ab");
        assert_eq!(short_hash("sha256:ab"), "sha256:ab");
    }

    // --- Docs ---

    /// A doc room's state in the mock engine: versions as (index, author, content).
    #[derive(Default)]
    struct MockDoc {
        status: &'static str,
        approved: Option<u64>,
        versions: Vec<(u64, &'static str, String)>,
        next_index: u64,
        created: Vec<Value>,
    }

    type DocState = Arc<Mutex<MockDoc>>;

    fn doc_meta(d: &MockDoc) -> Value {
        let current = d.versions.last().map(|v| v.0);
        json!({
            "id": "prd", "name": "Pricing",
            "kind": {
                "type": "doc", "status": d.status,
                "current_version": current, "version_count": d.versions.len(),
                "approved_version": d.approved, "reviewers": ["reviewer"]
            }
        })
    }

    fn doc_body(d: &MockDoc, at: Option<u64>) -> Option<Value> {
        let v = match at {
            Some(i) => d.versions.iter().find(|v| v.0 == i)?,
            None => d.versions.last()?,
        };
        let number = d.versions.iter().position(|x| x.0 == v.0).unwrap() + 1;
        Some(json!({
            "room": doc_meta(d),
            "content": v.2,
            "version": { "index": v.0, "number": number, "author": v.1, "saved_at_ms": 1 }
        }))
    }

    /// An engine with one doc room, applying the engine's conflict rule on
    /// saves, plus a room list mixing docs with other kinds.
    async fn spawn_doc_engine(doc: MockDoc) -> (NubiClient, DocState) {
        use axum::extract::Path;
        let state: DocState = Arc::new(Mutex::new(doc));
        let app = axum::Router::new()
            .route(
                "/v1/spaces/{space}/rooms",
                get(|State(s): State<DocState>| async move {
                    let d = s.lock().await;
                    axum::Json(json!([
                        doc_meta(&d),
                        { "id": "rfc", "name": "Sessions", "kind": { "type": "doc", "status": "approved" } },
                        { "id": "general", "name": "general", "kind": { "type": "group" } }
                    ]))
                })
                .post(|State(s): State<DocState>, axum::Json(body): axum::Json<Value>| async move {
                    let mut d = s.lock().await;
                    d.created.push(body.clone());
                    let has_content = body.get("content").is_some();
                    (StatusCode::CREATED, axum::Json(json!({
                        "id": "new-doc", "name": body["name"],
                        "kind": {
                            "type": "doc", "status": "draft",
                            "current_version": if has_content { json!(0) } else { Value::Null },
                            "version_count": if has_content { 1 } else { 0 }
                        }
                    })))
                }),
            )
            .route(
                "/v1/spaces/{space}/docs/{room}",
                get(|State(s): State<DocState>| async move {
                    axum::Json(doc_body(&*s.lock().await, None).unwrap())
                }),
            )
            .route(
                "/v1/spaces/{space}/docs/{room}/versions/{index}",
                get(|State(s): State<DocState>, Path((_, _, index)): Path<(String, String, u64)>| async move {
                    match doc_body(&*s.lock().await, Some(index)) {
                        Some(b) => axum::Json(b).into_response(),
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }),
            )
            .route(
                "/v1/spaces/{space}/docs/{room}/content",
                axum::routing::put(|State(s): State<DocState>, axum::Json(body): axum::Json<Value>| async move {
                    let mut d = s.lock().await;
                    let current = d.versions.last().map(|v| v.0);
                    if body["base_version"].as_u64() != current {
                        return (StatusCode::CONFLICT, axum::Json(json!({
                            "error": { "code": "doc_conflict", "current_version": current }
                        }))).into_response();
                    }
                    let index = d.next_index;
                    d.next_index += 1;
                    d.versions.push((index, "me", body["content"].as_str().unwrap().to_string()));
                    let number = d.versions.len();
                    axum::Json(json!({ "version": { "index": index, "number": number, "saved_at_ms": 2 } })).into_response()
                }),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let config = Arc::new(RwLock::new(Config {
            base_url: format!("http://{}", addr),
            token: "t".into(),
            space_id: "s".into(),
            agent_id: Some("me".into()),
            token_file: None,
            subscribe: None,
        }));
        (NubiClient::new(config), state)
    }

    fn two_versions() -> MockDoc {
        MockDoc {
            status: "in_review",
            approved: None,
            versions: vec![
                (0, "vania", "## Problem\n".into()),
                (3, "me", "## Problem\nSeats.\n".into()),
            ],
            next_index: 5,
            created: Vec::new(),
        }
    }

    #[tokio::test]
    async fn list_docs_keeps_docs_and_filters_by_status() {
        let (client, _) = spawn_doc_engine(two_versions()).await;
        let all = client.list_docs(None).await.unwrap();
        let ids: Vec<&str> = all
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["room_id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["prd", "rfc"], "the group room is not a doc");
        let approved = client.list_docs(Some("approved")).await.unwrap();
        assert_eq!(approved[0]["room_id"], "rfc");
        assert_eq!(all[0]["version_count"], 2);
    }

    #[tokio::test]
    async fn get_doc_returns_content_and_what_to_save_against() {
        let (client, _) = spawn_doc_engine(two_versions()).await;
        let doc = client.get_doc("prd", None).await.unwrap();
        assert_eq!(doc["content"], "## Problem\nSeats.\n");
        assert_eq!(doc["version"], 2);
        assert_eq!(doc["author"], "me");
        assert_eq!(doc["base_version"], 3);
        assert_eq!(doc["is_latest"], true);
        assert_eq!(doc["status"], "in_review");
        assert_eq!(doc["changed_since_approval"], false);
    }

    #[tokio::test]
    async fn an_old_version_says_so_and_still_points_saves_at_the_latest() {
        let (client, _) = spawn_doc_engine(two_versions()).await;
        let old = client.get_doc("prd", Some(0)).await.unwrap();
        assert_eq!(old["content"], "## Problem\n");
        assert_eq!(old["version"], 1);
        assert_eq!(old["is_latest"], false);
        assert_eq!(old["base_version"], 3);
    }

    #[tokio::test]
    async fn an_approved_doc_edited_since_says_so() {
        let mut doc = two_versions();
        doc.status = "approved";
        doc.approved = Some(0);
        let (client, _) = spawn_doc_engine(doc).await;
        let read = client.get_doc("prd", None).await.unwrap();
        assert_eq!(read["changed_since_approval"], true);
    }

    #[tokio::test]
    async fn a_doc_approved_before_any_content_counts_as_changed_once_written() {
        // Approved with no versions: the engine records no approved_version.
        // Content written since then was never approved.
        let mut doc = two_versions();
        doc.status = "approved";
        doc.approved = None;
        let (client, _) = spawn_doc_engine(doc).await;
        let read = client.get_doc("prd", None).await.unwrap();
        assert_eq!(read["changed_since_approval"], true);

        // With no versions at all there is nothing to have changed
        let empty = compact_doc(&json!({
            "room": { "id": "d", "name": "D", "kind": { "type": "doc", "status": "approved" } },
            "content": "",
            "version": null
        }));
        assert_eq!(empty["changed_since_approval"], false);
    }

    #[tokio::test]
    async fn update_doc_saves_against_the_base_version() {
        let (client, state) = spawn_doc_engine(two_versions()).await;
        let saved = client
            .update_doc("prd", "## Problem\nSeats and teams.\n", Some(3))
            .await
            .unwrap();
        assert_eq!(saved["saved"], true);
        assert_eq!(saved["version"], 3);
        assert_eq!(saved["base_version"], 5);
        assert_eq!(
            state.lock().await.versions.last().unwrap().2,
            "## Problem\nSeats and teams.\n"
        );
    }

    #[tokio::test]
    async fn a_stale_save_writes_nothing_and_hands_back_the_latest() {
        let (client, state) = spawn_doc_engine(two_versions()).await;
        let answer = client
            .update_doc("prd", "my stale edit", Some(0))
            .await
            .unwrap();
        assert_eq!(answer["saved"], false);
        assert!(answer["conflict"].as_str().unwrap().contains("Merge"));
        assert_eq!(answer["latest"]["content"], "## Problem\nSeats.\n");
        assert_eq!(answer["latest"]["base_version"], 3);
        assert_eq!(state.lock().await.versions.len(), 2, "nothing was written");

        // Saving again on the latest goes through
        let retried = client.update_doc("prd", "merged", Some(3)).await.unwrap();
        assert_eq!(retried["saved"], true);
    }

    #[tokio::test]
    async fn create_doc_sends_the_first_version() {
        let (client, state) = spawn_doc_engine(two_versions()).await;
        let created = client
            .create_doc("Rollout", Some("## Context\n"), None)
            .await
            .unwrap();
        assert_eq!(created["room_id"], "new-doc");
        assert_eq!(created["base_version"], 0);
        let sent = state.lock().await.created[0].clone();
        assert_eq!(sent["room_type"], "doc");
        assert_eq!(sent["content"], "## Context\n");

        // Blank content is left out, so the doc starts with no version
        client.create_doc("Notes", Some("   "), None).await.unwrap();
        assert!(state.lock().await.created[1].get("content").is_none());
    }
}
