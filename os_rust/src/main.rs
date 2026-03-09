use anyhow::{Context, Result};
use axum::{
    extract::{State, Json},
    response::IntoResponse,
    routing::post,
    Router,
};
use clap::Parser;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Write;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_appender::rolling;
use tracing_subscriber::{fmt, EnvFilter, prelude::*};
use tower_http::cors::CorsLayer;

const DEFAULT_LOG: &str = "/tmp/scheduler.log"; // Matches Python Dashboard
const DEFAULT_PID: &str = "/var/run/green_scheduler.pid";
const CARBON_API_URL: &str = "http://127.0.0.1:5000/intensity"; // local mock API like main_code.c

#[derive(Parser, Debug)]
#[command(author, version, about = "Carbon-aware task scheduler (Rust + Axum)")]
struct Args {
    #[arg(short = 'f', long = "foreground")]
    foreground: bool,

    #[arg(short = 'p', long = "port", default_value_t = 8080)]
    port: u16,

    #[arg(short = 'l', long = "log", default_value = DEFAULT_LOG)]
    log: String,

    #[arg(long = "pid", default_value = DEFAULT_PID)]
    pid: String,

    #[arg(short = 'i', long = "interval", default_value_t = 300)]
    interval: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TaskConfig {
    command: String,
    #[serde(default = "default_urgency")]
    urgency: String,
    #[serde(default)]
    deadline_hours: u64,
    submitted_at: Option<u64>,
}

fn default_urgency() -> String {
    "low".to_string()
}

#[derive(Debug, Clone)]
struct Task {
    command: String,
    urgency: String,
    deadline: u64,
    submitted_at: u64,
    started: bool,
    delayed: bool,
    pid: Option<u32>,
}

#[derive(Clone)]
struct AppState {
    tasks: Arc<RwLock<Vec<Task>>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Setup logging (file or stdout)
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.log)
        .unwrap();

    let fmt_layer = fmt::layer()
        .with_writer(std::sync::Arc::new(log_file))
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_ansi(false);

    tracing_subscriber::registry().with(env_filter).with(fmt_layer).init();

    info!("Starting green scheduler on port {} (logs to {})", args.port, args.log);

    if !args.foreground {
        if let Err(e) = write_pid(&args.pid) {
            warn!("Failed to write pid file {}: {}", args.pid, e);
        }
    }

    let client = Client::builder()
        .user_agent("green_scheduler/0.2 (rust-axum)")
        .timeout(Duration::from_secs(10))
        .build()?;

    let state = AppState {
        tasks: Arc::new(RwLock::new(Vec::new())),
    };

    let pid_path = args.pid.clone();
    ctrlc::set_handler(move || {
        let _ = fs::remove_file(&pid_path);
        info!("Termination signal received. PID file removed and exiting.");
        std::process::exit(0);
    })?;

    // Spawn the scheduler loop in the background
    let scheduler_state = state.clone();
    let poll_interval = args.interval;
    tokio::spawn(async move {
        run_scheduler(scheduler_state, client, poll_interval).await;
    });

    // Start Axum REST API Server
    let app = Router::new()
        .route("/add_tasks", post(handle_add_tasks))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handle_add_tasks(
    State(state): State<AppState>,
    Json(payload): Json<Vec<TaskConfig>>,
) -> impl IntoResponse {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut new_tasks = Vec::new();

    for t in payload.into_iter() {
        let submitted = t.submitted_at.unwrap_or(now);
        let deadline = submitted + t.deadline_hours * 3600;
        
        new_tasks.push(Task {
            command: t.command,
            urgency: t.urgency,
            deadline,
            submitted_at: submitted,
            started: false,
            delayed: false,
            pid: None,
        });
    }

    // Sort heavily heavily urgent first
    new_tasks.sort_by_key(|t| {
        match t.urgency.as_str() {
            "high" => 0,
            "medium" => 1,
            _ => 2,
        }
    });

    let count = new_tasks.len();
    {
        let mut tasks = state.tasks.write().await;
        for t in new_tasks {
            // Very simple deduplication (don't add immediate duplicates)
            let exists = tasks.iter().any(|existing| existing.command == t.command && existing.submitted_at == t.submitted_at);
            if !exists {
                tasks.push(t);
            }
        }
    }

    info!("Received {} tasks via POST /add_tasks", count);
    "Tasks accepted"
}

// Emulates the exact log string format the Python dashboard needs
fn timestamp_log_str() -> String {
    let now = chrono::Local::now();
    format!("[{}]", now.format("%Y-%m-%d %H:%M:%S"))
}

async fn run_scheduler(state: AppState, client: Client, interval_secs: u64) {
    let mut last_fetch = 0;
    let mut intensity = String::from("unknown");
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Check if we need to fetch intensity
        if now - last_fetch >= interval_secs || intensity == "unknown" {
            intensity = match fetch_carbon_intensity(&client).await {
                Ok(s) => s,
                Err(e) => {
                    let ts = timestamp_log_str();
                    error!("{} [ERROR] Carbon API request failed: {}", ts, e);
                    String::from("unknown")
                }
            };
            
            let ts = timestamp_log_str();
            info!("{} [INFO] Carbon Intensity Level: {} | Forecast: -1 gCO2/kWh", ts, intensity);
            last_fetch = now;
        }

        let mut tasks_to_launch = Vec::new();
        let high_carbon = intensity == "high" || intensity == "very high";

        {
            let mut guard = state.tasks.write().await;
            for (idx, t) in guard.iter_mut().enumerate() {
                if t.started {
                    continue;
                }

                let urgent = t.urgency == "high";
                let deadline_passed = now >= t.deadline;

                if urgent {
                    t.started = true;
                    tasks_to_launch.push((idx, t.clone()));
                } else if high_carbon && !deadline_passed {
                    if !t.delayed {
                        t.delayed = true;
                        let ts = timestamp_log_str();
                        info!("{} [INFO] Deferred due to high carbon: {}", ts, t.command);
                    }
                } else {
                    t.started = true;
                    tasks_to_launch.push((idx, t.clone()));
                }
            }
        }

        // Launch tasks concurrently
        for (idx, task) in tasks_to_launch {
            let state_clone = state.tasks.clone();
            tokio::spawn(async move {
                let ts = timestamp_log_str();
                // Match Python parser expectation: [TASK] Launched: <cmd> | PID: <pid> | Delayed: yes/no
                

                
                let mut parts = shlex::split(&task.command)
                    .unwrap_or_else(|| task.command.split_whitespace().map(|s| s.to_string()).collect());
                
                if parts.is_empty() {
                    return;
                }

                let prog = parts.remove(0);
                let mut cmd = Command::new(&prog);
                if !parts.is_empty() {
                    cmd.args(parts);
                }

                if task.urgency == "low" {
                    unsafe {
                        cmd.pre_exec(|| {
                            libc::nice(10);
                            Ok(())
                        });
                    }
                }

                cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());

                match cmd.spawn() {
                    Ok(mut child) => {
                        let child_pid = child.id().unwrap_or(0);
                        info!("{} [TASK] Launched: {} | PID: {} | Delayed: {}", 
                              ts, task.command, child_pid, if task.delayed { "yes" } else { "no" });
                        
                        // Update pid in state
                        {
                            let mut guard = state_clone.write().await;
                            if let Some(t) = guard.get_mut(idx) {
                                t.pid = Some(child_pid);
                            }
                        }

                        // Wait for completion
                        let _ = child.wait().await;

                        let end_time = SystemTime::now();
                        let completed_ts = timestamp_log_str();
                        
                        // Precise delay calculation for dashboard
                        let delay_secs = end_time.duration_since(UNIX_EPOCH).unwrap().as_secs() - task.submitted_at;
                        
                        // Output exactly what Python requires
                        // "[2026-03-09 12:34:56] [TASK] Completed: sleep 10 | PID: 12345 | Delay: 23 sec"
                        info!("{} [TASK] Completed: {} | PID: {} | Delay: {} sec", 
                              completed_ts, task.command, child_pid, delay_secs);
                    }
                    Err(e) => {
                        error!("{} [ERROR] Failed to launch {}: {}", ts, task.command, e);
                    }
                }
            });
        }

        sleep(Duration::from_millis(500)).await;
    }
}

async fn fetch_carbon_intensity(client: &Client) -> Result<String> {
    let resp = client.get(CARBON_API_URL).send().await.context("request error")?;
    let json: serde_json::Value = resp.json().await.context("json parse")?;

    if let Some(index) = json
        .get("data")
        .and_then(|d| d.get(0))
        .and_then(|e| e.get("intensity"))
        .and_then(|i| i.get("index"))
        .and_then(|s| s.as_str())
    {
        return Ok(index.to_string());
    }

    Ok(String::from("unknown"))
}

fn write_pid(path: &str) -> Result<()> {
    let pid = std::process::id();
    let mut f = File::create(path).context("create pid file")?;
    writeln!(f, "{}", pid).context("writing pid file")?;
    Ok(())
}
