#!/usr/bin/env python3
import os
import time
import yaml
import uuid
import shutil
import socket
import threading
import subprocess
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import platform

# ======================================================================
#   LOAD CONFIG
# ======================================================================

CONFIG_FILE = "/opt/airflow_agent/config.xml"

with open(CONFIG_FILE, "r") as f:
    CONFIG = yaml.safe_load(f)

TOKEN = CONFIG.get("token")
ALLOWED_IPS = CONFIG.get("allowed_ips", [])
BLACKLIST = CONFIG.get("command_blacklist", [])
RATE = CONFIG.get("rate_limit", {"window_seconds": 60, "max_requests": 100})
RETENTION_DAYS = CONFIG.get("retention_days", 60)

LISTEN_HOST = CONFIG.get("listen", {}).get("host", "0.0.0.0")
LISTEN_PORT = CONFIG.get("listen", {}).get("port", 18443)
TLS_CERT = CONFIG.get("tls", {}).get("server_cert")
TLS_KEY = CONFIG.get("tls", {}).get("server_key")

JOB_DIR = "/opt/airflow_agent/jobs"
os.makedirs(JOB_DIR, exist_ok=True)

app = FastAPI()
rate_bucket = {}   # rate limit memory


# ======================================================================
# SECURITY HELPERS
# ======================================================================

def client_ip(request: Request):
    return request.client.host

def is_ip_allowed(ip):
    if not ALLOWED_IPS:
        return True
    for rule in ALLOWED_IPS:
        if "/" in rule:
            import ipaddress
            if ipaddress.ip_address(ip) in ipaddress.ip_network(rule, strict=False):
                return True
        else:
            if ip == rule:
                return True
    return False

def rate_limit(ip):
    window = RATE["window_seconds"]
    now = time.time()
    bucket = rate_bucket.get(ip, [])
    bucket = [t for t in bucket if t > now - window]
    bucket.append(now)
    rate_bucket[ip] = bucket
    return len(bucket) <= RATE["max_requests"]

def blacklist_check(cmd):
    for w in BLACKLIST:
        if w in cmd:
            return False
    return True

async def validate_security(request: Request):
    ip = client_ip(request)
    token = request.headers.get("X-Agent-Token")

    if token != TOKEN:
        return JSONResponse({"error": "Invalid token"}, status_code=401)

    if not is_ip_allowed(ip):
        return JSONResponse({"error": "IP not allowed"}, status_code=403)

    if not rate_limit(ip):
        return JSONResponse({"error": "Rate limit exceeded"}, status_code=429)

    return None


# ======================================================================
# JOB EXECUTION HELPERS
# ======================================================================

def write(path, content):
    with open(path, "w") as f:
        f.write(str(content))

def read(path):
    if os.path.exists(path):
        return open(path).read().strip()
    return None

def run_local_command(cmd, job_id, run_as):
    job_path = os.path.join(JOB_DIR, job_id)
    log_out = os.path.join(job_path, "stdout.log")
    log_err = os.path.join(job_path, "stderr.log")
    exitf = os.path.join(job_path, "exit")
    statusf = os.path.join(job_path, "status")

    write(statusf, "running")

    if platform.system() == "Windows":
        base = f'powershell -Command "{cmd}"'
    else:
        if run_as:
            base = f"su - {run_as} -c '{cmd}'"
        else:
            base = cmd

    full = f"bash -c \"{base} > {log_out} 2> {log_err}; echo $? > {exitf}\""

    subprocess.Popen(full, shell=True)

    # background monitor
    def wait_exit():
        while True:
            if os.path.exists(exitf):
                write(statusf, "finished")
                return
            time.sleep(2)

    threading.Thread(target=wait_exit, daemon=True).start()


# ======================================================================
# API ENDPOINTS
# ======================================================================

@app.post("/ping")
async def ping(request: Request):
    sec = await validate_security(request)
    if sec:
        return sec
    return {"status": "ok", "time": datetime.utcnow().isoformat()}


@app.post("/run")
async def run_job(request: Request):
    sec = await validate_security(request)
    if sec:
        return sec

    payload = await request.json()
    cmd = payload["command"]
    run_as = payload.get("run_as_user")

    if not blacklist_check(cmd):
        return JSONResponse({"error": "Command blocked (blacklist)"}, status_code=400)

    job_id = str(uuid.uuid4())
    job_path = os.path.join(JOB_DIR, job_id)
    os.makedirs(job_path, exist_ok=True)

    write(os.path.join(job_path, "status"), "running")

    run_local_command(cmd, job_id, run_as)

    return {"job_id": job_id, "status": "submitted"}


@app.get("/status/{job_id}")
async def get_status(job_id: str, request: Request):
    sec = await validate_security(request)
    if sec:
        return sec

    job_path = os.path.join(JOB_DIR, job_id)
    return {
        "job_id": job_id,
        "status": read(os.path.join(job_path, "status")),
        "return_code": read(os.path.join(job_path, "exit")),
        "stdout": read(os.path.join(job_path, "stdout.log")),
        "stderr": read(os.path.join(job_path, "stderr.log")),
    }


# ======================================================================
# CLEANUP THREAD
# ======================================================================

def cleanup():
    while True:
        cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
        for job in os.listdir(JOB_DIR):
            path = os.path.join(JOB_DIR, job)
            if os.path.isdir(path):
                if datetime.fromtimestamp(os.path.getmtime(path)) < cutoff:
                    shutil.rmtree(path, ignore_errors=True)
        time.sleep(3600)

threading.Thread(target=cleanup, daemon=True).start()


# ======================================================================
# PROGRAMMATIC UVICORN START
# ======================================================================

if __name__ == "__main__":
    uvicorn.run(
        "agent_unified:app",
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        ssl_keyfile=TLS_KEY,
        ssl_certfile=TLS_CERT,
        workers=1,
    )
