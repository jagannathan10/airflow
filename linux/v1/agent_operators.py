import json
import time
from typing import Optional, Dict

import requests

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base import BaseSensorOperator


# ============================================================
#   COMMON UTILITIES
# ============================================================

def _get_agent_conn(conn_id: str):
    """
    Reads an Airflow HTTP connection for the agent.

    Expected:
      Conn ID: agent_default  (or whatever you set)
      conn.host: not used
      conn.password: not used
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


# ============================================================
#   1️⃣ SYNC OPERATOR
# ============================================================

class AgentSyncOperator(BaseOperator):
    """
    Runs a command synchronously on the agent.

    The agent blocks until completion and returns stdout/stderr + rc.
    """

    template_fields = ("target_server", "command", "job_user")

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_conn_id: str = "agent_default",
        timeout_seconds: int = 3600,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.timeout_seconds = timeout_seconds

    def execute(self, context):
        token, verify, cert = _get_agent_conn(self.agent_conn_id)

        url = f"https://{self.target_server}/run"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        payload = {
            "job_type": "shell",
            "command": self.command,
            "run_as_user": self.job_user,
            "sync": True,
            "use_tmux": False,
            "timeout_seconds": self.timeout_seconds,
        }

        resp = _post(url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        data = resp.json()
        rc = data.get("return_code", 0)

        if rc != 0:
            raise AirflowException(
                f"Sync job failed rc={rc}, stderr={data.get('stderr')}"
            )

        return data


# ============================================================
#   2️⃣ ASYNC OPERATOR (non-tmux)
# ============================================================

class AgentAsyncOperator(BaseOperator):
    """
    Submits a non-tmux async job to the agent and polls /status
    until completion.

    Use this only for jobs of *limited* duration.
    For very long jobs, prefer tmux + AgentStatusSensor.
    """

    template_fields = ("target_server", "command", "job_user")

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        agent_conn_id: str = "agent_default",
        timeout_seconds: int = 3600,
        poll_interval: int = 15,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval

    def execute(self, context):
        token, verify, cert = _get_agent_conn(self.agent_conn_id)

        run_url = f"https://{self.target_server}/run"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        payload = {
            "job_type": "shell",
            "command": self.command,
            "run_as_user": self.job_user,
            "sync": False,
            "use_tmux": False,
        }

        resp = _post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        body = resp.json()
        job_id = body["job_id"]
        status_url = f"https://{self.target_server}/status/{job_id}"

        start = time.time()

        while True:
            if time.time() - start > self.timeout_seconds:
                raise AirflowException("Async job timeout")

            r = _get(status_url, headers, verify, cert, timeout=20)
            info = r.json()
            state = info.get("status", "").strip()

            if state == "finished":
                rc = info.get("return_code")
                if rc not in (0, None):
                    raise AirflowException(f"Async job failed: {info}")
                return info

            if state == "failed":
                raise AirflowException(f"Agent job failed: {info}")

            time.sleep(self.poll_interval)


# ============================================================
#   3️⃣ TMUX OPERATOR (fire-and-forget by default)
# ============================================================

class AgentTmuxOperator(BaseOperator):
    """
    Runs a long-running command on the agent using tmux.

    fire_and_forget=True (default):
      - Submit job
      - Return immediately with job_id
      - No polling, no long blocking in Airflow

    fire_and_forget=False:
      - Polls /status until completion (NOT recommended for
        very long jobs; better to use AgentStatusSensor).
    """

    template_fields = ("target_server", "command", "job_user")

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: Optional[str] = None,
        fire_and_forget: bool = True,
        agent_conn_id: str = "agent_default",
        timeout_seconds: int = 86400,
        poll_interval: int = 30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.fire_and_forget = fire_and_forget
        self.agent_conn_id = agent_conn_id
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval

    def execute(self, context):
        token, verify, cert = _get_agent_conn(self.agent_conn_id)

        url = f"https://{self.target_server}/run"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        payload = {
            "job_type": "shell",
            "command": self.command,
            "run_as_user": self.job_user,
            "sync": False,
            "use_tmux": True,
        }

        resp = _post(url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        body = resp.json()
        job_id = body["job_id"]
        status_url = f"https://{self.target_server}/status/{job_id}"

        # FIRE AND FORGET = TRUE → return immediately with job_id
        if self.fire_and_forget:
            self.log.info("Submitted tmux job_id=%s (fire_and_forget=True)", job_id)
            return {"job_id": job_id, "status": "submitted"}

        # Otherwise, poll until completion (NOT recommended for very long jobs)
        self.log.warning(
            "fire_and_forget=False: long polling in operator. "
            "For very long jobs, prefer AgentStatusSensor."
        )

        start = time.time()

        while True:
            if time.time() - start > self.timeout_seconds:
                raise AirflowException("TMUX job timeout")

            r = _get(status_url, headers, verify, cert, timeout=20)
            info = r.json()
            state = info.get("status", "").strip()

            if state == "finished":
                rc = info.get("return_code")
                if rc not in (0, None):
                    raise AirflowException(f"TMUX job failed: {info}")
                return info

            if state == "failed":
                raise AirflowException(f"TMUX job failed: {info}")

            time.sleep(self.poll_interval)


# ============================================================
#   4️⃣ STATUS SENSOR (recommended for long jobs)
# ============================================================

class AgentStatusSensor(BaseSensorOperator):
    """
    Polls the agent /status/<job_id> endpoint until:
      - status == "finished" and rc == 0 → SUCCESS
      - status == "finished" and rc != 0 → FAIL
      - status == "failed" → FAIL
      - status == "cancelled" → FAIL (or soft_fail if you want)

    Usage:
      1. Use AgentTmuxOperator(fire_and_forget=True) to submit.
      2. Pass job_id via XCom to this sensor.
    """

    template_fields = ("target_server", "job_id")

    def __init__(
        self,
        target_server: str,
        job_id: str,
        agent_conn_id: str = "agent_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.job_id = job_id
        self.agent_conn_id = agent_conn_id

    def poke(self, context):
        token, verify, cert = _get_agent_conn(self.agent_conn_id)

        url = f"https://{self.target_server}/status/{self.job_id}"
        headers = {}
        if token:
            headers["X-Agent-Token"] = token

        resp = _get(url, headers, verify, cert, timeout=20)
        if resp.status_code != 200:
            self.log.warning("Status check failed (%s): %s", resp.status_code, resp.text)
            return False

        info = resp.json()
        state = (info.get("status") or "").strip()
        rc = info.get("return_code", None)

        self.log.info("Agent status for job_id=%s: %s", self.job_id, info)

        if state in ("running", "", None):
            return False

        if state in ("cancelled", "failed"):
            raise AirflowException(f"Agent job {self.job_id} state={state}, info={info}")

        if state == "finished":
            if rc not in (0, None):
                raise AirflowException(
                    f"Agent job {self.job_id} finished with rc={rc}, info={info}"
                )
            return True

        # Unknown state → keep polling
        return False


# ============================================================
#   PLUGIN EXPORT
# ============================================================

class AgentPlugin(AirflowPlugin):
    name = "agent_operators"
    operators = [
        AgentSyncOperator,
        AgentAsyncOperator,
        AgentTmuxOperator,
        AgentStatusSensor,
    ]
