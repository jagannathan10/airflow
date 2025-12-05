#!/usr/bin/env python3
import os
import uuid
import subprocess
import time
import ipaddress
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import threading

# ============================================================================
# CONFIG
# ============================================================================

BASE_DIR = "/opt/airflow_agent"
CONFIG_FILE = os.path.join(BASE_DIR, "config.xml")
JOB_DIR = os.path.join(BASE_DIR, "jobs")
os.makedirs(JOB_DIR, exist_ok=True)

CONFIG = {
    "token": None,
    "listen_host": "0.0.0.0",
    "listen_port": 18443,
    "allowed_ips": [],         # single IP entries
    "allowed_cidrs": [],       # CIDR ranges
    "command_blacklist": [],
    "rate_limit_window": 60,
    "rate_limit_max": 200,
    "tls_cert": None,
    "tls_key": None,
}

RATE_STATE = {}         # rate limit in-memory
CONFIG_MTIME = None     # config reload tracking

app = FastAPI(title="Unified Airflow Agent with Reload & CIDR Support")


# ============================================================================
# CONFIG PARSING
# ============================================================================

def load_config():
    """Load config.xml with IP + CIDR support and store mtime."""
    global CONFIG, CONFIG_MTIME

    if not os.path.exists(CONFIG_FILE):
        print("[agent] No config.xml found")
        return

    stat = os.stat(CONFIG_FILE)
    if CONFIG_MTIME == stat.st_mtime:
        return  # no change

    CONFIG_MTIME = stat.st_mtime

    print(f"[agent] Loading configuration from {CONFIG_FILE}")

    tree = ET.parse(CONFIG_FILE)
    root = tree.getroot()

    def get(tag, default=None):
        node = root.find(tag)
        return node.text.strip() if node is not None and node.text else default

    CONFIG["token"] = get("token", None)
    CONFIG["listen_host"] = get("listen_host", "0.0.0.0")
    CONFIG["listen_port"] = int(get("listen_port", 18443))

    # IP + CIDR rules
    CONFIG["allowed_ips"] = []
    CONFIG["allowed_cidrs"] = []

    ip_node = root.find("allowed_ips")
    if ip_node is not None:
        for node in ip_node:
            if node.tag == "ip":
                CONFIG["allowed_ips"].append(node.text.strip())
            elif node.tag == "cidr":
                try:
                    CONFIG["allowed_cidrs"].append(
                        ipaddress.ip_network(node.text.strip(), strict=False)
                    )
                except Exception as e:
                    print(f"[agent] Invalid CIDR: {node.text} ({e})")

    # Command blacklist
    CONFIG["command_blacklist"] = []
    bl_node = root.find("command_blacklist")
    if bl_node is not None:
        for cmd in bl_node.findall("cmd"):
            CONFIG["command_blacklist"].append(cmd.text.strip())

    # Rate limit
    rl = root.find("rate_limit")
    if rl is not None:
        w = rl.find("window_seconds")
        m = rl.find("max_requests")
        CONFIG["rate_limit_window"] = int(w.text.strip()) if w is not None else 60
        CONFIG["rate_limit_max"] = int(m.text.strip()) if m is not None else 200

    # TLS
    tls = root.find("tls")
    if tls is not None:
        cert = tls.find("server_cert")
        key = tls.find("server_key")
        CONFIG["tls_cert"] = cert.text.strip() if cert is not None else None
        CONFIG["tls_key"] = key.text.strip() if key is not None else None

    print("[agent] Config loaded successfully:")
    print(CONFIG)


def config_auto_reloader():
    """Reload config.xml every 30 seconds."""
    while True:
        try:
            load_config()
        except Exception as e:
            print(f"[agent] Config reload error: {e}")
        time.sleep(30)


threading.Thread(target=config_auto_reloader, daemon=True).start()


# ============================================================================
# SECURITY CHECKS
# ============================================================================

def check_ip_allowed(request: Request):
    client_ip = ipaddress.ip_address(request.client.host)

    # Allow any IP if allow-list empty
    if not CONFIG["allowed_ips"] and not CONFIG["allowed_cidrs"]:
        return

    # Check single-IP allow list
    if request.client.host in CONFIG["allowed_ips"]:
        return

    # Check CIDR groups
    for cidr in CONFIG["allowed_cidrs"]:
        if client_ip in cidr:
            return

    raise Exception(f"IP {client_ip} not allowed")


async def validate_token(request: Request):
    token = CONFIG["token"]
    if not token:
        return
    provided = request.headers.get("X-Agent-Token")
    if provided != token:
        raise Exception("Invalid token")


def check_blacklist(command: str):
    for bad in CONFIG["command_blacklist"]:
        if bad in command:
            raise Exception(f"Blocked command pattern: {bad}")


def check_rate_limit(request: Request):
    ip = request.client.host
    now = time.time()
    window = CONFIG["rate_limit_window"]
    max_req = CONFIG["rate_limit_max"]

    ts = RATE_STATE.get(ip, [])
    ts = [t for t in ts if now - t < window]
    ts.append(now)
    RATE_STATE[ip] = ts

    if len(ts) > max_req:
        raise Exception("Rate limit exceeded")


# ============================================================================
# JOB EXECUTION
# ============================================================================

class JobRequest(BaseModel):
    command: str
    run_as_user: Optional[str] = None
    job_id: Optional[str] = None
    use_tmux: bool = True
    skip_if_running: bool = True
    fire_and_forget: bool = False
    env: Dict[str, str] = {}


def write_file(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(str(content))


def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except:
        return None


def build_script(job_id, req: JobRequest):
    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    out = f"{job_path}/stdout.log"
    err = f"{job_path}/stderr.log"
    exitf = f"{job_path}/exit"
    status = f"{job_path}/status"

    write_file(status, "running")

    if req.run_as_user:
        esc = req.command.replace("'", "'\"'\"'")
        run = f"su - {req.run_as_user} -c '{esc}'"
    else:
        run = req.command

    script = f"""#!/bin/bash
set -o pipefail
{run} >> "{out}" 2>> "{err}"
echo $? > "{exitf}"
echo finished > "{status}"
"""

    sp = f"{job_path}/run.sh"
    with open(sp, "w") as f:
        f.write(script)
    os.chmod(sp, 0o755)
    return sp


def execute_job(job_id, req: JobRequest):
    script = build_script(job_id, req)

    if req.use_tmux:
        session = f"agent_{job_id}"
        subprocess.Popen(["/usr/bin/tmux", "new-session", "-d", "-s", session, script])
    else:
        subprocess.Popen(["/bin/bash", script])


# ============================================================================
# API
# ============================================================================

@app.post("/reload_config")
async def reload_config_api(request: Request):
    await validate_token(request)
    load_config()
    return {"status": "reloaded"}


@app.post("/run")
async def run_job(request: Request, req: JobRequest):
    check_ip_allowed(request)
    check_rate_limit(request)
    await validate_token(request)
    check_blacklist(req.command)

    job_id = req.job_id or str(uuid.uuid4())
    statusfile = os.path.join(JOB_DIR, job_id, "status")

    if req.skip_if_running and os.path.exists(statusfile):
        if (read_file(statusfile) or "").strip() == "running":
            return {"job_id": job_id, "status": "already_running"}

    execute_job(job_id, req)
    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    jsp = os.path.join(JOB_DIR, job_id)
    if not os.path.exists(jsp):
        return {"status": "unknown"}

    status = read_file(f"{jsp}/status") or "running"
    rc = read_file(f"{jsp}/exit")

    return {
        "job_id": job_id,
        "status": status.strip(),
        "return_code": int(rc) if rc else None,
        "stdout": read_file(f"{jsp}/stdout.log") or "",
        "stderr": read_file(f"{jsp}/stderr.log") or "",
    }


@app.post("/cancel/{job_id}")
async def cancel_job(request: Request, job_id: str):
    check_ip_allowed(request)
    check_rate_limit(request)
    await validate_token(request)

    session = f"agent_{job_id}"
    subprocess.call(["/usr/bin/tmux", "kill-session", "-t", session])

    write_file(os.path.join(JOB_DIR, job_id, "status"), "cancelled")
    return {"job_id": job_id, "status": "cancelled"}
