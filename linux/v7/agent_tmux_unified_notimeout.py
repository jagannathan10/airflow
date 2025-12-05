import json
import time
import hashlib
import requests
from typing import Optional, Dict

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class AgentTmuxUnifiedOperator(BaseOperator):

    ui_color = "#f5b042"

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_conn_id: str = "agent_default",
        poll_interval: int = 10,
        env: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.poll_interval = poll_interval
        self.env = env or {}

    # ------------------------------------------------------------------
    # PARALLEL-SAFE deterministic job_id
    # ------------------------------------------------------------------
    def _generate_job_id(self, context):
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id
        exec_date = context["ts"]

        base = f"{dag_id}__{task_id}__{exec_date}"
        safe = base.replace(":", "_").replace(".", "_").replace("+", "_")

        # Add short hash to avoid collisions
        h = hashlib.sha1(base.encode()).hexdigest()[:8]
        return f"{safe}__{h}"

    # ------------------------------------------------------------------
    def _prepare_conn(self):
        conn = BaseHook.get_connection(self.agent_conn_id)
        extras = conn.extra_dejson

        token = extras.get("agent_token")
        verify = extras.get("verify_ssl", False)

        cert = None
        if extras.get("client_cert") and extras.get("client_key"):
            cert = (extras.get("client_cert"), extras.get("client_key"))

        return token, verify, cert

    def _post(self, url, headers, data, verify, cert):
        return requests.post(
            url,
            headers=headers,
            data=json.dumps(data),
            verify=verify,
            cert=cert,
            timeout=30,
        )

    def _safe_get(self, url, headers, verify, cert):
        try:
            return requests.get(url, headers=headers, verify=verify, cert=cert, timeout=20)
        except:
            return None

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
            "run_as_user": self.job_user,
            "job_id": job_id,
            "skip_if_running": True,
            "fire_and_forget": False,
            "use_tmux": True,
            "env": self.env,
        }

        # ---------------- RUN ----------------
        resp = self._post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"/run failed: {resp.text}")

        data = resp.json()
        if data.get("status") not in ("submitted", "already_running"):
            raise AirflowException(f"Unexpected agent response: {data}")

        # ---------------- INFINITE POLLING (NO TIMEOUT) ----------------
        self.log.info(f"[Agent] Job started job_id={job_id} (NO TIMEOUT MODE)")

        while True:
            res = self._safe_get(status_url, headers, verify, cert)
            if not res:
                time.sleep(self.poll_interval)
                continue

            info = res.json()
            status = info.get("status")
            rc = info.get("return_code")

            self.log.info(f"[Agent] job_id={job_id}, status={status}, rc={rc}")

            # FINISHED
            if status == "finished":
                if rc != 0:
                    raise AirflowException(f"Remote TMUX job failed: {info}")
                return info

            # CANCELLED
            if status == "cancelled":
                raise AirflowException("Remote job cancelled")

            # Still running → keep polling forever
            time.sleep(self.poll_interval)
