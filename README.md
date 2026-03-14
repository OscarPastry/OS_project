# 🌿 OS_project — Carbon-Aware Green Task Scheduler

A **carbon-aware task scheduler** that delays non-urgent computational tasks when grid electricity carbon intensity is high, and runs them when it's low — reducing the carbon footprint of computation.

Originally written in C, the project has been fully rewritten using a modern **Rust (Axum/Tokio)** backend with **Python** tooling for visualization and simulation.

## 🏗️ Architecture

1. **Scheduler Daemon (Rust):** An asynchronous daemon that exposes a REST API (`POST /add_tasks`). It controls OS-level task execution duration via polling carbon API metrics.
2. **Carbon API Mock (Python):** A local Flask server that simulates changing grid intensity ("low", "moderate", "high", "very high") every 2 minutes.
3. **Live Dashboard (Python):** A real-time `matplotlib` visualization tracking delays, carbon emissions saved, and tasks processed.
4. **CLI Tool (Python):** A `submit_tasks.py` script to easily send tasks to the REST backend.

---

## 🚀 How to Run the Project

### Prerequisites
- [Rust & Cargo](https://rustup.rs/) (Edition 2021)
- Python 3+

### 1. Setup Python Environment
To avoid system package conflicts, it's highly recommended to use a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install flask requests pandas matplotlib numpy
```

### 2. Start the Carbon Intensity Mock API
In a new terminal (with the `.venv` activated):
```bash
python3 mock_carbon_api.py
```
*(This mock API cycles grid states on `127.0.0.1:5000`)*

### 3. Start the Rust Scheduler Daemon
In a new terminal:
```bash
cd os_rust
cargo build --release

# Run in foreground polling the API every 5 seconds
./target/release/os_project --foreground --interval 5
```
*(Logs are output dynamically and also saved to `/tmp/scheduler.log`)*

### 4. Open the Live Analytics Dashboard
In a new terminal (with the `.venv` activated):
```bash
python3 live_dashboard.py
```
*(This parses `/tmp/scheduler.log` to plot real-time metrics)*

### 5. Submit Tasks
You can easily submit tasks using the included CLI component in another terminal:
```bash
source .venv/bin/activate

# High-urgency tasks execute immediately
python3 submit_tasks.py "echo 'hello world'" --urgency high

# Low-urgency tasks execute immediately IF carbon intensity is low,
# or are delayed until intensity drops (or until the deadline passes).
python3 submit_tasks.py "sleep 5" --urgency low --deadline 12
```

---

## 📂 Legacy Code
The original C implementations (using `libmicrohttpd` and raw `waitpid` pthreads) have been archived in the `legacy_c_code/` directory for reference.
