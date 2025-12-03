import json
import time
import re
from typing import Optional

import requests

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin


# ============================================================
#   COMMON UTILITIES
# ============================================================

def _get_agent_conn(conn_id: str):
    """
    Reads an Airflow HTTP connection for the agent.

    Expected:
      Conn ID: agent_default (or whatever you configure)
      conn.extra: JSON like:
        {
          "agent_token": "scb-airflowagent-....",
          "verify_ssl": false,
          "client_cert": "/path/to/cert.pem",
          "client_key": "/path/to/key.pem"
        }
    """
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
            data=json.dumps(data),
            headers=headers,
            verify=verify,
            cert=cert,
            timeout=30,
        )
    except Exception as e:
        raise AirflowException(f"POST {url} failed: {e}")


def _get(url, headers, verify, cert, timeout=20):
    try:
        return requests.get(
            url,
            headers=headers,
            verify=verify,
            cert=cert,
            timeout=timeout,
        )
    except Exception as e:
        raise AirflowException(f"GET {url} failed: {e}")


def _sanitize_id(s: str) -> str:
    """
    Make a safe job_id for the agent / tmux by stripping weird chars.
    """
    return re.sub(r"[^A-Za-z0-9_.-]", "_", s)


# ============================================================
#   TMUX-ONLY UNIFIED OPERATOR (WITH BUILT-IN SENSOR)
# ============================================================

class AgentTmuxUnifiedOperator(BaseOperator):
    """
    TMUX-only operator with built-in status polling.

    - Always uses tmux on the agent
    - No separate sensor DAG/task
    - No sync/async operators
    - Prevents duplicate parallel runs via deterministic job_id + skip_if_running

    JobID semantics:
      job_id = "<dag_id>__<task_id>__<run_id>"  (sanitized)
    The same job_id is reused for retries of the same DAG run.

    Behaviour:
      1. POST /run with:
         {
           "job_id": job_id,
           "command": ...,
           "run_as_user": ...,
           "fire_and_forget": false,
           "skip_if_running": true
         }

      2. If /run returns "already_running":
         - We DO NOT start a new tmux job.
         - We poll /status/<job_id> until it finishes.
         - This means no duplicate remote jobs.

      3. If /run returns "submitted":
         - We poll /status/<job_id> until finished/failed/timeout.
    """

    template_fields = ("target_server", "command", "job_user", "job_id")

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_conn_id: str = "agent_default",
        timeout_seconds: int = 86400,
        poll_interval: int = 30,
        job_id: Optional[str] = None,
        skip_if_running: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval
        self.job_id = job_id
        self.skip_if_running = skip_if_running

    # --------------------------------------------------------
    # Helpers
    # --------------------------------------------------------

    def _build_job_id(self, context) -> str:
        """
        Deterministic job_id based on DAG / Task / Run.
        This ensures correlation & prevents parallel dupes.
        """
        dag_id = context["dag"].dag_id
        run_id = context["run_id"]
        task_id = self.task_id

        raw = f"{dag_id}__{task_id}__{run_id}"
        return _sanitize_id(raw)

    def _poll_status(self, job_id: str, headers, verify, cert):
        """
        Built-in sensor: poll /status until finished/failed/timeout.
        """
        status_url = f"https://{self.target_server}/status/{job_id}"
        start = time.time()

        while True:
            if time.time() - start > self.timeout_seconds:
                raise AirflowException(
                    f"Agent job timeout after {self.timeout_seconds}s, job_id={job_id}"
                )

            resp = _get(status_url, headers, verify, cert, timeout=20)
            if resp.status_code != 200:
                self.log.warning(
                    "Status check failed (%s): %s", resp.status_code, resp.text
                )
                time.sleep(self.poll_interval)
                continue

            info = resp.json()
            state = (info.get("status") or "").strip()
            rc = info.get("return_code", None)

            self.log.info("Agent /status for job_id=%s: %s", job_id, info)

            if state in ("running", "", None):
                time.sleep(self.poll_interval)
                continue

            if state in ("cancelled", "failed"):
                raise AirflowException(
                    f"Agent job {job_id} state={state}, info={info}"
                )

            if state == "finished":
                if rc not in (0, None):
                    raise AirflowException(
                        f"Agent job {job_id} finished with rc={rc}, info={info}"
                    )
                return info

            # Unknown state → keep polling
            self.log.warning("Unknown agent state '%s' for job_id=%s", state, job_id)
            time.sleep(self.poll_interval)

    # --------------------------------------------------------
    # Main
    # --------------------------------------------------------

    def execute(self, context):
        token, verify, cert = _get_agent_conn(self.agent_conn_id)

        job_id = self.job_id or self._build_job_id(context)
        self.log.info("Using deterministic job_id=%s", job_id)

        run_url = f"https://{self.target_server}/run"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        payload = {
            "job_id": job_id,
            "command": self.command,
            "run_as_user": self.job_user,
            "fire_and_forget": False,   # we will poll
            "skip_if_running": self.skip_if_running,
            "timeout_seconds": self.timeout_seconds,
        }

        self.log.info("Submitting TMUX job to agent at %s", run_url)
        resp = _post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        body = resp.json()
        status = (body.get("status") or "").strip()

        # Case 1: job already running → DO NOT create a new tmux session.
        if status == "already_running":
            self.log.info(
                "Agent reports job_id=%s already running; "
                "not starting a duplicate. Polling existing job.",
                job_id,
            )
            return self._poll_status(job_id, headers, verify, cert)

        # Case 2: new job submitted, we always poll until finished
        self.log.info(
            "Agent accepted job_id=%s with status=%s; starting status polling.",
            job_id,
            status or "submitted",
        )
        return self._poll_status(job_id, headers, verify, cert)


# ============================================================
#   PLUGIN EXPORT (ONLY TMUX UNIFIED OPERATOR)
# ============================================================

class AgentPlugin(AirflowPlugin):
    name = "agent_tmux_unified"
    operators = [
        AgentTmuxUnifiedOperator,
    ]
