import json
import time
import hashlib
import requests
from typing import Optional

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin


# =====================================================================
# CONNECTION HELPERS
# =====================================================================

def _get_agent_conn(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson

    token = extras.get("agent_token")
    verify_ssl = extras.get("verify_ssl", False)

    cert = None
    if extras.get("client_cert") and extras.get("client_key"):
        cert = (extras.get("client_cert"), extras.get("client_key"))

    return token, verify_ssl, cert


def _post(url, headers, data, verify, cert):
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
        raise AirflowException(f"POST {url} failed: {e}")


def _get(url, headers, verify, cert):
    try:
        return requests.get(
            url,
            headers=headers,
            verify=verify,
            cert=cert,
            timeout=30,
        )
    except Exception as e:
        raise AirflowException(f"GET {url} failed: {e}")
        

# =====================================================================
# FINAL UNIFIED TMUX OPERATOR
# =====================================================================

class AgentTmuxUnifiedOperator(BaseOperator):
    """
    Single universal operator:
      ✔ Submit TMUX job
      ✔ Poll /status until finished
      ✔ Prevent duplicate executions
      ✔ Automatically correlates job_id with DAG + run_id
      ✔ Return SUCCESS/FAILED based on exit code

    No separate sensor required.
    No sync/async distinction.
    """

    template_fields = ("target_server", "command", "job_user")

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_conn_id: str = "agent_default",
        poll_interval: int = 30,
        timeout_seconds: int = 86400,   # 24 hours
        skip_if_running: bool = True,   # <<< DEFAULT = TRUE
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.poll_interval = poll_interval
        self.timeout_seconds = timeout_seconds
        self.skip_if_running = skip_if_running  # default true

    # --------------------------------------------------------------
    # Deterministic job_id = SHA256(dag_id + run_id)
    # --------------------------------------------------------------
    def _make_job_id(self, context):
        raw = f"{context['dag'].dag_id}:{context['run_id']}"
        return hashlib.sha256(raw.encode()).hexdigest()

    # --------------------------------------------------------------
    # Main operator execution
    # --------------------------------------------------------------
    def execute(self, context):

        token, verify, cert = _get_agent_conn(self.agent_conn_id)
        job_id = self._make_job_id(context)

        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        run_url = f"https://{self.target_server}/run"
        status_url = f"https://{self.target_server}/status/{job_id}"

        # -----------------------------------------------------------------
        # 1) CHECK IF JOB ALREADY EXISTS ON TARGET SERVER
        # -----------------------------------------------------------------
        self.log.info(f"Checking job status for job_id={job_id}")

        try:
            r = _get(status_url, headers, verify, cert)
            if r.status_code == 200:
                info = r.json()
                state = (info.get("status") or "").strip()
                rc = info.get("return_code")

                if state == "running" and self.skip_if_running:
                    self.log.info(
                        f"Job {job_id} is already running → SKIPPING duplicate execution."
                    )
                    return info

                if state == "finished":
                    if self.skip_if_running:
                        self.log.info(
                            f"Job {job_id} already finished earlier → REUSING result."
                        )
                        return info
        except Exception:
            pass  # no existing job

        # -----------------------------------------------------------------
        # 2) SUBMIT TMUX JOB (idempotent using forced_job_id)
        # -----------------------------------------------------------------
        payload = {
            "job_type": "shell",
            "command": self.command,
            "run_as_user": self.job_user,
            "sync": False,
            "use_tmux": True,
            "forced_job_id": job_id,
        }

        self.log.info(f"Submitting new TMUX job_id={job_id}")

        resp = _post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        # -----------------------------------------------------------------
        # 3) POLL UNTIL FINISHED
        # -----------------------------------------------------------------
        start = time.time()

        while True:
            elapsed = time.time() - start
            if elapsed > self.timeout_seconds:
                raise AirflowException(
                    f"TMUX job timed out after {self.timeout_seconds} seconds"
                )

            try:
                r = _get(status_url, headers, verify, cert)
            except Exception as e:
                self.log.warning(f"Status check error: {e}")
                time.sleep(self.poll_interval)
                continue

            if r.status_code != 200:
                time.sleep(self.poll_interval)
                continue

            info = r.json()
            state = (info.get("status") or "").strip()
            rc = info.get("return_code")

            self.log.info(f"[{job_id}] state={state} rc={rc}")

            if state == "running":
                time.sleep(self.poll_interval)
                continue

            if state == "finished":
                if rc not in (0, None):
                    raise AirflowException(f"TMUX job failed with rc={rc}: {info}")
                return info

            if state in ("failed", "cancelled"):
                raise AirflowException(f"TMUX job ended state={state}: {info}")

            time.sleep(self.poll_interval)


# =====================================================================
# PLUGIN EXPORT
# =====================================================================

class AgentUnifiedPlugin(AirflowPlugin):
    name = "agent_tmux_unified"
    operators = [AgentTmuxUnifiedOperator]
