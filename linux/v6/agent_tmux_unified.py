import json
import time
import hashlib
import requests
from typing import Optional, Dict

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AgentTmuxUnifiedOperator(BaseOperator):
    """
    Unified TMUX operator for the Unified Airflow Agent.

    Features:
      - job_id = deterministic (<dag_id>__<task_id>__<execution_date>)
      - skip_if_running=True (agent-side dedup)
      - Polling built-in (no separate sensor)
      - Never runs duplicates
      - Matches agent.py API exactly
    """

    ui_color = "#f5b042"

    def __init__(
        self,
        target_server: str,               # host:port (HTTPS)
        command: str,
        job_user: Optional[str] = None,   # su - user
        agent_conn_id: str = "agent_default",
        poll_interval: int = 10,
        timeout_seconds: int = 86400,     # max 1 day job default
        env: Optional[Dict[str, str]] = None,
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

    # ---------------------------------------------------------------------
    # Utility: Compute deterministic job_id based on (dag, task, exec date)
    # ---------------------------------------------------------------------
    def _generate_job_id(self, context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        exec_date = context["ts"]  # 2025-12-03T05:00:00+00:00

        base = f"{dag_id}__{task_id}__{exec_date}"
        # safe for filesystem
        safe = base.replace(":", "_").replace(".", "_").replace("+", "_")
        return safe

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _prepare_conn(self):
        """Loads token, TLS settings and verify mode from Airflow connection."""
        conn = BaseHook.get_connection(self.agent_conn_id)
        extras = conn.extra_dejson

        token = extras.get("agent_token")
        verify = extras.get("verify_ssl", False)

        cert = None
        if extras.get("client_cert") and extras.get("client_key"):
            cert = (extras.get("client_cert"), extras.get("client_key"))

        return token, verify, cert

    def _post(self, url, headers, data, verify, cert):
        try:
            return requests.post(
                url,
                headers=headers,
                data=json.dumps(data),
                verify=verify,
                cert=cert,
                timeout=30,
            )
        except Exception as e:
            raise AirflowException(f"POST error: {e}")

    def _safe_get(self, url, headers, verify, cert):
        try:
            return requests.get(url, headers=headers, verify=verify, cert=cert, timeout=20)
        except:
            return None

    # ---------------------------------------------------------------------
    # Execute
    # ---------------------------------------------------------------------
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
            "run_as_user": self.job_user,
            "job_id": job_id,
            "skip_if_running": True,     # always TRUE
            "fire_and_forget": False,    # operator will poll
            "use_tmux": True,            # always TRUE
            "env": self.env,
        }

        self.log.info(f"[Agent] Starting job_id={job_id} on {self.target_server}")
        self.log.info(f"[Agent] Command: {self.command}")

        # -----------------------------------------------------------------
        # RUN
        # -----------------------------------------------------------------
        resp = self._post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"/run failed: {resp.text}")

        data = resp.json()
        state = data.get("status")

        if state == "already_running":
            self.log.warning(f"[Agent] Job already running → job_id={job_id}")
            # Continue polling until completion

        elif state != "submitted":
            raise AirflowException(f"Unexpected agent response: {data}")

        # -----------------------------------------------------------------
        # POLLING LOOP
        # -----------------------------------------------------------------
        start = time.time()

        while True:
            if (time.time() - start) > self.timeout_seconds:
                raise AirflowException("Agent job timeout exceeded")

            res = self._safe_get(status_url, headers, verify, cert)
            if not res:
                time.sleep(self.poll_interval)
                continue

            st = res.json()
            status = st.get("status")
            rc = st.get("return_code")

            self.log.info(f"[Agent] status={status}, rc={rc}, job_id={job_id}")

            # FINISHED?
            if status == "finished":
                if rc not in (0, None):
                    self.log.error(st.get("stderr", ""))
                    raise AirflowException(f"Remote TMUX job failed rc={rc}")
                return st  # SUCCESS

            # CANCELLED?
            if status == "cancelled":
                raise AirflowException("Remote TMUX job was cancelled")

            # Any other state → keep polling
            time.sleep(self.poll_interval)
