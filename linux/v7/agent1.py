#!/usr/bin/env python3
# -------------------------------------------------------------------------
# Unified Airflow Agent (TMUX-only, parallel-safe, config.xml-driven)
# -------------------------------------------------------------------------

import os
import subprocess
import time
import ipaddress
import threading
import xml.etree.ElementTree as ET

from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# -------------------------------------------------------------------------
# CONSTANTS
# -------------------------------------------------------------------------

BASE_DIR = "/opt/airflow_agent"
JOB_DIR = f"{BASE_DIR}/jobs"
CONFIG_FILE = f"{BASE_DIR}/config.xml"
TMUX_BIN = "/usr/bin/tmux"

os.makedirs(JOB_DIR, exist_ok=True)

# -------------------------------------------------------------------------
# RUNTIME CONFIG (populated from config.xml)
# -------------------------------------------------------------------------

CONFIG = {
    "token": None,
    "allowed_ips": [],
    "command_blacklist": [],
    "rate_limit_window": 60,
    "rate_limit_max": 120,
}

RATE_TRACKER = {}   # { ip: [timestamps] }

# -------------------------------------------------------------------------
# FASTAPI INIT
# -------------------------------------------------------------------------

app = FastAPI(title="Unified Airflow Agent (Config-enabled)")


# -------------------------------------------------------------------------
# CONFIG LOADER
# -------------------------------------------------------------------------

def load_config():
    global CONFIG

    if not os.path.exists(CONFIG_FILE):
        print("[CONFIG] No config.xml found. Using defaults.")
        return

    try:
        tree = ET.parse(CONFIG_FILE)
        root = tree.getroot()

        # TOKEN
        token = root.findtext("token")
        if token:
            CONFIG["token"] = token.strip()

        # ALLOWED IPs + CIDR
        CONFIG["allowed_ips"] = []
        aip = root.find("allowed_ips")
        if aip is not None:
            for node in aip:
                CONFIG["allowed_ips"].append(node.text.strip())

        # COMMAND BLACKLIST
        CONFIG["command_blacklist"] = []
        bl = root.find("command_blacklist")
        if bl is not None:
            for cmd in bl:
                CONFIG["command_blacklist"].append(cmd.text.strip())

        # RATE-LIMIT
        rl = root.find("rate_limit")
        if rl is not None:
            ws = rl.findtext("window_seconds")
            mx = rl.findtext("max_requests")
            CONFIG["rate_limit_window"] = int(ws) if ws else 60
            CONFIG["rate_limit_max"] = int(mx) if mx else 120

        print("[CONFIG] Loaded config.xml successfully")

    except Exception as e:
        print(f"[CONFIG] ERROR loading config.xml: {e}")


def config_auto_reload():
    """Reload config.xml every 30 seconds."""
    while True:
        time.sleep(30)
        load_config()


# Start background config reload
threading.Thread(target=config_auto_reload, daemon=True).start()
load_config()


# -------------------------------------------------------------------------
# SECURITY HELPERS
# -------------------------------------------------------------------------

def ip_allowed(ip: str) -> bool:
    """Validate IP against allow-list with CIDR support."""
    if not CONFIG["allowed_ips"]:
        return True  # allow all if not configured

    try:
        ip_obj = ipaddress.ip_address(ip)
    except:
        return False

    for entry in CONFIG["allowed_ips"]:
        entry = entry.strip()
        if "/" in entry:
            # CIDR
            try:
                if ip_obj in ipaddress.ip_network(entry, strict=False):
                    return True
            except:
                pass
        else:
            # Single IP
            if ip == entry:
                return True

    return False


def is_blocked_command(cmd: str) -> bool:
    for bad in CONFIG["command_blacklist"]:
        if bad.lower() in cmd.lower():
            return True
    return False


def rate_limited(ip: str) -> bool:
    """Simple in-memory rate limiting."""
    now = time.time()
    window = CONFIG["rate_limit_window"]
    maxreq = CONFIG["rate_limit_max"]

    RATE_TRACKER.setdefault(ip, [])
    RATE_TRACKER[ip] = [t for t in RATE_TRACKER[ip] if now - t < window]

    if len(RATE_TRACKER[ip]) >= maxreq:
        return True

    RATE_TRACKER[ip].append(now)
    return False


async def enforce_security(request: Request, payload_command: Optional[str] = None):
    # IP CHECK
    client_ip = request.client.host
    if not ip_allowed(client_ip):
        return JSONResponse({"error": "IP_DENIED", "ip": client_ip}, status_code=403)

    # RATE LIMIT
    if rate_limited(client_ip):
        return JSONResponse({"error": "RATE_LIMIT"}, status_code=429)

    # TOKEN CHECK
    if CONFIG["token"]:
        header = request.headers.get("X-Agent-Token")
        if header != CONFIG["token"]:
            return JSONResponse({"error": "INVALID_TOKEN"}, status_code=403)

    # BLACKLIST CHECK
    if payload_command and is_blocked_command(payload_command):
        return JSONResponse({"error": "COMMAND_BLOCKED"}, status_code=403)

    return None


# -------------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------------

def write(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(content))


def read(path, default=""):
    if os.path.exists(path):
        return open(path).read()
    return default


def tmux_session_name(job_id):
    return f"agent_{job_id}"


def is_tmux_alive(job_id):
    session = tmux_session_name(job_id)
    return subprocess.call(
        [TMUX_BIN, "has-session", "-t", session],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    ) == 0


# -------------------------------------------------------------------------
# REQUEST MODEL
# -------------------------------------------------------------------------

class JobRequest(BaseModel):
    command: str
    run_as_user: Optional[str] = None
    job_id: str
    skip_if_running: bool = True
    fire_and_forget: bool = False
    env: Dict[str, str] = {}
    timeout_seconds: Optional[int] = None


# -------------------------------------------------------------------------
# JOB EXECUTION
# -------------------------------------------------------------------------

def build_script(job_id: str, payload: JobRequest):

    job_path = f"{JOB_DIR}/{job_id}"
    os.makedirs(job_path, exist_ok=True)

    stdout = f"{job_path}/stdout.log"
    stderr = f"{job_path}/stderr.log"
    exit_file = f"{job_path}/exit"
    status = f"{job_path}/status"

    write(status, "starting")

    if payload.run_as_user:
        escaped = payload.command.replace("'", "'\"'\"'")
        run_line = f"su - {payload.run_as_user} -c '{escaped}'"
    else:
        run_line = payload.command

    script = f"{job_path}/run.sh"
    with open(script, "w") as f:
        f.write(f"""#!/bin/bash
set -o pipefail
{run_line} >> "{stdout}" 2>> "{stderr}"
RC=$?
echo $RC > "{exit_file}"
echo finished > "{status}"
""")

    os.chmod(script, 0o755)
    return script


def run_job(job_id: str, payload: JobRequest):
    script = build_script(job_id, payload)
    session = tmux_session_name(job_id)

    subprocess.call([
        TMUX_BIN, "new-session", "-d",
        "-s", session,
        "bash", "-lc", f"'{script}'"
    ])

    write(f"{JOB_DIR}/{job_id}/status", "running")


# -------------------------------------------------------------------------
# API
# -------------------------------------------------------------------------

@app.post("/ping")
async def ping(req: Request):
    sec = await enforce_security(req)
    if sec:
        return sec
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run(req: Request, payload: JobRequest):
    sec = await enforce_security(req, payload.command)
    if sec:
        return sec

    job_id = payload.job_id

    # parallel-safe dedup
    if payload.skip_if_running and is_tmux_alive(job_id):
        return {"job_id": job_id, "status": "already_running"}

    run_job(job_id, payload)
    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def status(req: Request, job_id: str):
    sec = await enforce_security(req)
    if sec:
        return sec

    job_path = f"{JOB_DIR}/{job_id}"

    status = read(f"{job_path}/status", "unknown").strip()
    exit_code = read(f"{job_path}/exit")

    if exit_code:
        try:
            exit_code = int(exit_code)
        except:
            exit_code = None

    return {
        "job_id": job_id,
        "status": status,
        "return_code": exit_code,
        "stdout": read(f"{job_path}/stdout.log", ""),
        "stderr": read(f"{job_path}/stderr.log", ""),
    }


@app.post("/cancel/{job_id}")
async def cancel(req: Request, job_id: str):
    sec = await enforce_security(req)
    if sec:
        return sec

    session = tmux_session_name(job_id)
    subprocess.call([TMUX_BIN, "kill-session", "-t", session])

    write(f"{JOB_DIR}/{job_id}/status", "cancelled")
    return {"job_id": job_id, "status": "cancelled"}


@app.on_event("startup")
def startup():
    print(f"[AGENT] Started. BASE_DIR={BASE_DIR}, JOB_DIR={JOB_DIR}, TMUX={TMUX_BIN}")
