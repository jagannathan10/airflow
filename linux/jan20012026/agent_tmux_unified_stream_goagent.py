"""
agent_tmux_unified_stream_goagent.py

NON-DEFERRABLE operator with *real* stdout/stderr streaming for the provided Go agent (main.go).

Go agent contracts (from main.go):
- POST  /run                       -> {"job_id":"...","status":"submitted|already_running","active_job_id": "...?"}
- GET   /status/<job_id>           -> {"job_id":"...","status":"running|finished|cancelled|unknown","return_code": <int|null>}
- POST  /cancel/<job_id>           -> {"job_id":"...","status":"cancelled"}
- GET   /logs/<job_id>?stream=stdout|stderr&offset=<n>
     -> {"job_id": "...", "stream":"stdout|stderr", "offset": n, "next": n2, "data": "...", "eof": true|false, ...}

This operator:
- submits /run
- polls /status/<job_id> indefinitely (no timeout)
- simultaneously streams stdout/stderr by tracking offsets using the agent /logs endpoint.

NOTE:
- This file is self-contained (does not modify your original agent_tmux_unified.py).
- If you prefer, you can import your existing operator and subclass it; this keeps it simple and explicit.
"""

import json
import time
import hashlib
import os
from typing import Optional, Dict, Any, Tuple

import requests
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class AgentTmuxUnifiedStreamOperatorGo(BaseOperator):
    ui_color = "#f5b042"
    template_fields = ("command", "env", "job_user")

    def __init__(
        self,
        *,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_token: Optional[str] = None,
        poll_interval: int = 10,
        log_poll_interval: int = 5,
        env: Optional[Dict[str, str]] = None,
        verify_ssl: bool = False,
        cert: Optional[str] = None,
        max_log_chunk: int = 64 * 1024,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_token = agent_token or os.environ.get("AGENT_TOKEN")
        self.poll_interval = int(poll_interval)
        self.log_poll_interval = int(log_poll_interval)
        self.env = env or {}
        self.verify_ssl = bool(verify_ssl)
        self.cert = cert
        self.max_log_chunk = int(max_log_chunk)

    def _generate_job_id(self, context) -> str:
        # Stable-ish job_id; adapt if you want your dedupKey/reattach scheme
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        run_id = context["run_id"]
        base = f"{dag_id}__{task_id}__{run_id}"
        safe = base.replace(":", "_").replace(".", "_").replace("+", "_").replace("/", "_")
        h = hashlib.sha1(base.encode()).hexdigest()[:10]
        return f"{safe}__{h}"

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.agent_token:
            h["X-Agent-Token"] = self.agent_token
        return h

    def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = requests.post(
            url,
            headers=self._headers(),
            data=json.dumps(payload),
            timeout=30,
            verify=self.verify_ssl,
            cert=self.cert,
        )
        if r.status_code != 200:
            raise AirflowException(f"POST {url} failed HTTP {r.status_code}: {r.text[:1000]}")
        try:
            return r.json()
        except Exception:
            raise AirflowException(f"POST {url} returned non-JSON: {r.text[:1000]}")

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        try:
            r = requests.get(
                url,
                headers=self._headers(),
                params=params,
                timeout=30,
                verify=self.verify_ssl,
                cert=self.cert,
            )
        except Exception:
            return None
        if r.status_code != 200:
            return None
        try:
            return r.json()
        except Exception:
            return None

    def _stream_one(self, job_id: str, stream: str, offset: int) -> Tuple[int, str, bool]:
        """
        Returns: (next_offset, data, eof)
        """
        url = f"https://{self.target_server}/logs/{job_id}"
        params = {"stream": stream, "offset": str(offset)}
        info = self._get_json(url, params=params)
        if not info:
            return offset, "", False

        data = info.get("data") or ""
        nxt = info.get("next")
        eof = bool(info.get("eof"))

        try:
            next_offset = int(nxt) if nxt is not None else (offset + len(data.encode("utf-8", errors="ignore")))
        except Exception:
            next_offset = offset + len(data.encode("utf-8", errors="ignore"))

        return next_offset, data, eof

    def execute(self, context):
        job_id = self._generate_job_id(context)

        run_url = f"https://{self.target_server}/run"
        status_url = f"https://{self.target_server}/status/{job_id}"

        payload = {
            "command": self.command,
            "run_as_user": self.job_user or "",
            "job_id": job_id,
            "dedup_key": "",               # keep empty unless you implement cross-run queueing
            "skip_if_running": True,
            "fire_and_forget": False,
            "env": self.env,
            "use_tmux": True,
        }

        resp = self._post_json(run_url, payload)
        st = (resp.get("status") or "").lower()
        self.log.info("[Agent] /run response: %s", resp)

        if st not in ("submitted", "already_running"):
            raise AirflowException(f"Unexpected /run status: {resp}")

        # Offsets for streaming stdout/stderr
        out_off = 0
        err_off = 0
        last_log_ts = 0.0

        self.log.info("[Agent] job_id=%s started. Streaming stdout/stderr enabled.", job_id)

        while True:
            now = time.time()
            if (now - last_log_ts) >= self.log_poll_interval:
                # stdout
                new_out_off, out_data, _ = self._stream_one(job_id, "stdout", out_off)
                if out_data:
                    for line in out_data.splitlines():
                        self.log.info("[STDOUT] %s", line)
                out_off = new_out_off

                # stderr
                new_err_off, err_data, _ = self._stream_one(job_id, "stderr", err_off)
                if err_data:
                    for line in err_data.splitlines():
                        self.log.warning("[STDERR] %s", line)
                err_off = new_err_off

                last_log_ts = now

            # status
            info = self._get_json(status_url)
            if not info:
                time.sleep(self.poll_interval)
                continue

            status = (info.get("status") or "").lower()
            rc = info.get("return_code")

            self.log.info("[Agent] status=%s rc=%s", status, rc)

            if status == "finished":
                # final flush
                for _ in range(3):
                    new_out_off, out_data, _ = self._stream_one(job_id, "stdout", out_off)
                    if out_data:
                        for line in out_data.splitlines():
                            self.log.info("[STDOUT] %s", line)
                    out_off = new_out_off

                    new_err_off, err_data, _ = self._stream_one(job_id, "stderr", err_off)
                    if err_data:
                        for line in err_data.splitlines():
                            self.log.warning("[STDERR] %s", line)
                    err_off = new_err_off
                    time.sleep(0.2)

                # Go agent returns return_code as *int or null
                # Treat None as "unknown" (fail safe) — change if you want current semantics.
                if rc is None:
                    raise AirflowException(f"Remote job finished but return_code is null: {info}")
                try:
                    rc_i = int(rc)
                except Exception:
                    raise AirflowException(f"Invalid return_code type: {info}")

                if rc_i != 0:
                    raise AirflowException(f"Remote TMUX job failed rc={rc_i}: {info}")
                return info

            if status == "cancelled":
                raise AirflowException("Remote TMUX job cancelled")

            time.sleep(self.poll_interval)
