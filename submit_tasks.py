#!/usr/bin/env python3
import argparse
import requests
import json
import sys
import time

def submit_task(url, command, urgency, deadline_hours):
    payload = [{
        "command": command,
        "urgency": urgency,
        "deadline_hours": deadline_hours,
        "submitted_at": int(time.time())
    }]

    try:
        print(f"Submitting task to {url}...")
        print(json.dumps(payload, indent=2))
        
        response = requests.post(
            f"{url}/add_tasks",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code in (200, 202):
            print("✅ Task successfully submitted!")
        else:
            print(f"❌ Failed to submit task. Server responded with {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to the scheduler daemon at {url}")
        print(f"Ensure the Rust scheduler is running. Details: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Submit tasks to the Green Scheduler Rust Daemon")
    parser.add_argument("command", help="The command to execute (e.g. 'sleep 10')")
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="URL of the scheduler daemon REST API")
    parser.add_argument("--urgency", choices=["low", "medium", "high"], default="low", help="Task urgency (default: low)")
    parser.add_argument("--deadline", type=int, default=24, help="Deadline in hours before task must run regardless of carbon intensity (default: 24)")

    args = parser.parse_args()

    submit_task(args.url, args.command, args.urgency, args.deadline)

if __name__ == "__main__":
    main()
