import time
import json
import hashlib
import requests
from typing import Optional, Dict, Tuple

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AgentLinuxTmuxUnifiedOperator(BaseOperator):
    """
    Linux TMUX unified operator with:
      - cross-run queueing via dedup_key (no duplicate start)
      - re-attach after pod crash (job_id uses run_id, no try_number)
      - UI cancel kills remote job (on_kill)
      - stderr streaming default; stdout optional
      - timeout optional: None/<=0 => no timeout, >0 => enforce

    Requires agent endpoints:
      POST /run  (accepts dedup_key, returns active_job_id)
      GET  /status/<job_id>
      POST /cancel/<job_id>
      GET  /logs/<job_id>?stream=stderr|stdout&offset=<n>
    """

    ui_color = "#f5b042"

    def __init__(
        self,
        target_server: str,                 # host:port
        command: str,
        job_user: Optional[str] = None,     # su - user on Linux
        agent_conn_id: str = "agent_default",
        poll_interval: int = 10,
        timeout_seconds: Optional[int] = None,     # None/0 => no timeout
        env: Optional[Dict[str, str]] = None,
        stream_stdout: bool = False,
        verify_ssl: Optional[bool] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
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

    # Stable per DagRun for crash-reattach
    def _generate_job_id(self, context) -> str:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        run_id = context.get("run_id") or context["dag_run"].run_id
        base = f"{dag_id}__{task_id}__{run_id}"
        safe = (
            base.replace(":", "_")
                .replace(".", "_")
                .replace("+", "_")
                .replace(" ", "_")
                .replace("/", "_")
        )
        h = hashlib.sha1(base.encode()).hexdigest()[:8]
        return f"{safe}__{h}"

    # Stable across runs for queueing (no duplicates until complete)
    def _generate_dedup_key(self, context) -> str:
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        user = self.job_user or ""
        cmd_hash = hashlib.sha1(self.command.encode()).hexdigest()[:12]
        return f"{dag_id}||{task_id}||{self.target_server}||{user}||{cmd_hash}"

    def _prepare_conn(self) -> Tuple[Optional[str], bool, Optional[Tuple[str, str]]]:
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

    def _post(self, url, headers, payload, verify, cert):
        try:
            return requests.post(
                url, headers=headers, data=json.dumps(payload),
                verify=verify, cert=cert, timeout=30,
            )
        except Exception as e:
            raise AirflowException(f"POST {url} failed: {e}")

    # stderr always; stdout optional
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
        self.log.warning("[LinuxAgent] QUEUED behind active_job_id=%s", active_job_id)

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
                self.log.warning("[LinuxAgent] Active job ended (%s). Starting queued job now.", status)
                return

            time.sleep(self.poll_interval)

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
            "run_as_user": self.job_user or "",
            "job_id": self._job_id,
            "dedup_key": self._dedup_key,     # ⭐ queue lock key
            "skip_if_running": True,
            "fire_and_forget": False,
            "use_tmux": True,
            "env": self.env,
        }

        # submit or get queued
        resp = self._post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"/run failed HTTP {resp.status_code}: {resp.text}")

        data = resp.json()
        st = (data.get("status") or "").strip().lower()

        if st == "already_running":
            active_job_id = data.get("active_job_id")
            if not active_job_id:
                raise AirflowException(f"already_running without active_job_id: {data}")
            self._wait_for_active_job(active_job_id, headers, verify, cert)

            # submit again after active completes
            resp = self._post(run_url, headers, payload, verify, cert)
            if resp.status_code != 200:
                raise AirflowException(f"/run after queue failed: {resp.text}")
            data = resp.json()
            st = (data.get("status") or "").strip().lower()

        if st not in ("submitted", "already_running"):
            raise AirflowException(f"Unexpected /run response: {data}")

        start = time.time()
        while True:
            if self.timeout_seconds and self.timeout_seconds > 0:
                if (time.time() - start) > self.timeout_seconds:
                    raise AirflowException(f"Remote job timeout exceeded ({self.timeout_seconds}s)")

            self._stream_logs(self._job_id, headers, verify, cert)

            r = self._get(status_url, headers, verify, cert)
            if not r:
                time.sleep(self.poll_interval)
                continue

            info = r.json()
            status = (info.get("status") or "").strip().lower()
            rc = info.get("return_code")

            if status == "finished":
                self._stream_logs(self._job_id, headers, verify, cert)
                if rc not in (0, None):
                    raise AirflowException(f"Remote TMUX job failed rc={rc}")
                return info

            if status == "cancelled":
                raise AirflowException("Remote TMUX job cancelled")

            time.sleep(self.poll_interval)

    # UI cancel only: called on graceful termination, not on hard pod crash
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
