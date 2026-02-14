import time
import json
import hashlib
import requests
import os
from typing import Optional, Dict, Tuple
import urllib3

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AgentWindowsUnifiedOperator(BaseOperator):
    ui_color = "#6aa9ff"

    def __init__(
        self,
        target_server: str,
        command: str,
        agent_conn_id: str = "agent_default",
        poll_interval: int = 10,
        timeout_seconds: Optional[int] = None,
        env: Optional[Dict[str, str]] = None,
        stream_stdout: bool = True,
        verify_ssl: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.agent_conn_id = agent_conn_id
        self.poll_interval = poll_interval
        self.timeout_seconds = timeout_seconds
        self.env = env or {}
        self.stream_stdout = stream_stdout
        self.verify_ssl = verify_ssl

        self._stdout_off = 0
        self._stderr_off = 0
        self._job_id = None
        self._dedup_key = None

    def _generate_job_id(self, context) -> str:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        run_id = context.get("run_id") or context["dag_run"].run_id
        map_index = getattr(context.get("ti"), "map_index", -1)

        base = f"{dag_id}__{task_id}__{run_id}__{map_index}"
        safe = (
            base.replace(":", "_")
                .replace(".", "_")
                .replace("+", "_")
                .replace(" ", "_")
                .replace("/", "_")
        )
        h = hashlib.sha1(base.encode()).hexdigest()[:8]
        return f"{safe}__{h}"

    # Stable across runs for queueing (serialize same task/cmd/server)
    def _generate_dedup_key(self, context) -> str:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        cmd_hash = hashlib.sha1(self.command.encode()).hexdigest()[:12]
        return f"{dag_id}||{task_id}||{self.target_server}||{cmd_hash}"

    def _prepare_conn(self) -> Tuple[Optional[str], bool, Optional[Tuple[str, str]]]:
        # Keep your existing behavior (token via env). You can later switch to BaseHook if desired.
        token = os.environ.get("AGENT_TOKEN")
        verify = False
        cert = None
        return token, verify, cert

    def _get(self, url, headers, verify, cert):
        try:
            return requests.get(url, headers=headers, verify=verify, cert=cert, timeout=20)
        except Exception:
            return None

    def _post(self, url, headers, payload, verify, cert):
        try:
            return requests.post(url, headers=headers, json=payload, verify=verify, cert=cert, timeout=30)
        except Exception as e:
            raise AirflowException(f"POST {url} failed: {e}")

    def _get_remote_state(self, status_url, headers, verify, cert):
        r = self._get(status_url, headers, verify, cert)
        if not r or r.status_code != 200:
            return None
        try:
            return r.json()
        except Exception:
            return None

    def _stream_logs(self, job_id, headers, verify, cert):
        base = f"https://{self.target_server}/logs/{job_id}"

        err_url = f"{base}?stream=stderr&offset={self._stderr_off}"
        r = self._get(err_url, headers, verify, cert)
        if r and r.status_code == 200:
            j = r.json()
            data = j.get("data") or ""
            if data:
                for line in data.splitlines():
                    self.log.error("%s", line)
            self._stderr_off = int(j.get("next") or self._stderr_off)

        if self.stream_stdout:
            out_url = f"{base}?stream=stdout&offset={self._stdout_off}"
            r = self._get(out_url, headers, verify, cert)
            if r and r.status_code == 200:
                j = r.json()
                data = j.get("data") or ""
                if data:
                    for line in data.splitlines():
                        self.log.info("%s", line)
                self._stdout_off = int(j.get("next") or self._stdout_off)

    def _wait_for_active_job(self, active_job_id, headers, verify, cert):
        status_url = f"https://{self.target_server}/status/{active_job_id}"
        self.log.warning("[WinAgent] QUEUED behind active_job_id=%s", active_job_id)

        start = time.time()
        while True:
            if self.timeout_seconds and self.timeout_seconds > 0:
                if (time.time() - start) > self.timeout_seconds:
                    raise AirflowException(f"Timeout while waiting for active job ({self.timeout_seconds}s)")

            r = self._get(status_url, headers, verify, cert)
            if not r:
                time.sleep(self.poll_interval)
                continue

            info = r.json()
            status = (info.get("status") or "").strip().lower()
            if status in ("finished", "cancelled"):
                self.log.warning("[WinAgent] Active job ended (%s). Starting queued job now.", status)
                return

            time.sleep(self.poll_interval)

    def _finalize_from_status(self, info: dict):
        status = (info.get("status") or "").strip().lower()
        rc = info.get("return_code")

        if status == "finished":
            if rc not in (0, None):
                raise AirflowException(f"Remote job failed rc={rc}")
            return info

        if status == "cancelled":
            raise AirflowException("Remote job cancelled")

        return None  # still running / unknown

    def execute(self, context):
        self._job_id = self._generate_job_id(context)
        self._dedup_key = self._generate_dedup_key(context)

        token, verify, cert = self._prepare_conn()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        run_url = f"https://{self.target_server}/run"
        status_url = f"https://{self.target_server}/status/{self._job_id}"

        payload = {
            "command": self.command,
            "run_as_user": "",
            "job_id": self._job_id,
            "dedup_key": self._dedup_key,
            "skip_if_running": True,
            "fire_and_forget": False,
            "use_tmux": False,
            "env": self.env,
        }

        # =========================================================
        # 1) REATTACH FIRST: if job_id exists, adopt it (no new /run)
        # =========================================================
        existing = self._get_remote_state(status_url, headers, verify, cert)
        if existing:
            self.log.warning("[WinAgent] Reattaching to existing job_id=%s", self._job_id)

            done = self._finalize_from_status(existing)
            if done is not None:
                # job already completed while Airflow was down/restarting
                return done

        # =========================================================
        # 2) If not existing, submit (may queue behind other run via dedup_key)
        # =========================================================
        if not existing:
            resp = self._post(run_url, headers, payload, verify, cert)
            if resp.status_code != 200:
                raise AirflowException(f"/run failed: HTTP {resp.status_code}: {resp.text}")

            data = resp.json()
            st = (data.get("status") or "").strip().lower()

            if st == "already_running":
                # Another run (or same task/cmd/server) is active due to dedup_key -> queue behind it
                active_job_id = data.get("active_job_id")
                if not active_job_id:
                    raise AirflowException(f"already_running without active_job_id: {data}")
                self._wait_for_active_job(active_job_id, headers, verify, cert)

                # Try submit again after active completes (cross-run serialization)
                resp2 = self._post(run_url, headers, payload, verify, cert)
                if resp2.status_code != 200:
                    raise AirflowException(f"/run after queue failed: {resp2.text}")
                data2 = resp2.json()
                st2 = (data2.get("status") or "").strip().lower()
                if st2 not in ("submitted", "already_running"):
                    raise AirflowException(f"Unexpected /run response: {data2}")

            elif st != "submitted":
                raise AirflowException(f"Unexpected /run response: {data}")

        # =========================================================
        # 3) Monitor until finished/cancelled
        # =========================================================
        start = time.time()
        while True:
            if self.timeout_seconds and self.timeout_seconds > 0:
                if (time.time() - start) > self.timeout_seconds:
                    raise AirflowException(f"Remote job timeout exceeded ({self.timeout_seconds}s)")

            self._stream_logs(self._job_id, headers, verify, cert)

            info = self._get_remote_state(status_url, headers, verify, cert)
            if not info:
                time.sleep(self.poll_interval)
                continue

            done = self._finalize_from_status(info)
            if done is not None:
                self._stream_logs(self._job_id, headers, verify, cert)
                return done

            time.sleep(self.poll_interval)

    def on_kill(self):
        try:
            if not self._job_id:
                return
            token, verify, cert = self._prepare_conn()
            headers = {"Content-Type": "application/json"}
            if token:
                headers["X-Agent-Token"] = token
            cancel_url = f"https://{self.target_server}/cancel/{self._job_id}"
            requests.post(cancel_url, headers=headers, verify=verify, cert=cert, timeout=10)
        except Exception:
            pass
