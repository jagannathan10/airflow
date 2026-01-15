import json
import time
import hashlib
import requests
from typing import Optional, Dict, Any, Tuple

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AgentWindowsUnifiedOperator(BaseOperator):
    """
    Windows unified operator with:
      - stderr streaming by default
      - stdout optional (stream_stdout=True)
      - incremental log offsets
      - no timeout by default (multi-week jobs)
    """

    ui_color = "#6aa9ff"

    def __init__(
        self,
        target_server: str,                 # host:port
        command: str,
        agent_conn_id: str = "agent_default",
        poll_interval: int = 10,
        timeout_seconds: Optional[int] = None,   # None = no timeout
        env: Optional[Dict[str, str]] = None,
        stream_stdout: bool = False,              # ⭐ NEW
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

    # ------------------------------------------------------------------
    def _generate_job_id(self, context) -> str:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        logical_date = str(context.get("logical_date") or context.get("ts"))
        try_number = getattr(context["ti"], "try_number", 1)

        base = f"{dag_id}__{task_id}__{logical_date}__try{try_number}"
        safe = (
            base.replace(":", "_")
                .replace(".", "_")
                .replace("+", "_")
                .replace(" ", "_")
        )
        h = hashlib.sha1(base.encode()).hexdigest()[:8]
        return f"{safe}__{h}"

    def _prepare_conn(self):
        conn = BaseHook.get_connection(self.agent_conn_id)
        extras = conn.extra_dejson or {}

        token = extras.get("agent_token")
        verify = extras.get("verify_ssl", False)
        if self.verify_ssl is not None:
            verify = self.verify_ssl

        cert = None
        if extras.get("client_cert") and extras.get("client_key"):
            cert = (extras["client_cert"], extras["client_key"])

        return token, verify, cert

    def _get(self, url, headers, verify, cert):
        try:
            return requests.get(url, headers=headers, verify=verify, cert=cert, timeout=20)
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 🔹 LOG STREAMING (stderr always, stdout optional)
    # ------------------------------------------------------------------
    def _stream_logs(self, job_id, headers, verify, cert):
        base = f"https://{self.target_server}/logs/{job_id}"

        # ---- STDERR (always) ----
        err_url = f"{base}?stream=stderr&offset={self._stderr_off}"
        r = self._get(err_url, headers, verify, cert)
        if r and r.status_code == 200:
            j = r.json()
            data = j.get("data") or ""
            if data:
                for line in data.splitlines():
                    self.log.error("%s", line)
            self._stderr_off = int(j.get("next") or self._stderr_off)

        # ---- STDOUT (optional) ----
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

    # ------------------------------------------------------------------
    def execute(self, context):
        job_id = self._generate_job_id(context)
        token, verify, cert = self._prepare_conn()

        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        run_url = f"https://{self.target_server}/run"
        status_url = f"https://{self.target_server}/status/{job_id}"

        payload = {
            "command": self.command,
            "run_as_user": "",
            "job_id": job_id,
            "skip_if_running": True,
            "fire_and_forget": False,
            "use_tmux": False,
            "env": self.env,
        }

        resp = requests.post(run_url, headers=headers, json=payload,
                             verify=verify, cert=cert, timeout=30)
        if resp.status_code != 200:
            raise AirflowException(f"/run failed: {resp.text}")

        start = time.time()

        while True:
            if self.timeout_seconds and (time.time() - start) > self.timeout_seconds:
                raise AirflowException("Remote job timeout")

            self._stream_logs(job_id, headers, verify, cert)

            r = self._get(status_url, headers, verify, cert)
            if not r:
                time.sleep(self.poll_interval)
                continue

            info = r.json()
            status = (info.get("status") or "").lower()
            rc = info.get("return_code")

            if status == "finished":
                self._stream_logs(job_id, headers, verify, cert)
                if rc not in (0, None):
                    raise AirflowException(f"Remote job failed rc={rc}")
                return info

            if status == "cancelled":
                raise AirflowException("Remote job cancelled")

            time.sleep(self.poll_interval)
