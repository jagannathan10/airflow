import json
import time
import requests

from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class AgentTmuxUnifiedOperator(BaseOperator):
    """
    Single operator for Linux tmux + Windows background execution.

    - Always triggers /run fire_and_forget=True
    - skip_if_running=True is enforced on agent side
    - Built-in polling (sensor) inside this operator
    - Deterministic job_id tied to task run_id
      => No duplicate execution
      => DagRun failure recovery safe
    """

    def __init__(
        self,
        target_server: str,
        command: str,
        job_user: str = None,
        agent_conn_id: str = "agent_default",
        timeout_seconds: int = 86400,   # default 24 hrs
        poll_interval: int = 30,
        fire_and_forget: bool = True,   # always fire-and-forget
        skip_if_running: bool = True,   # default behavior
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.target_server = target_server
        self.command = command
        self.job_user = job_user
        self.agent_conn_id = agent_conn_id
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval
        self.fire_and_forget = fire_and_forget
        self.skip_if_running = skip_if_running

    # --------------------------------------------------------------
    # INTERNAL: get Airflow Connection fields
    # --------------------------------------------------------------
    def _get_conn(self):
        conn = BaseHook.get_connection(self.agent_conn_id)
        extras = conn.extra_dejson
        token = extras.get("agent_token")
        verify_ssl = extras.get("verify_ssl", False)

        cert = None
        if extras.get("client_cert") and extras.get("client_key"):
            cert = (extras["client_cert"], extras["client_key"])

        return token, verify_ssl, cert

    # --------------------------------------------------------------
    # INTERNAL: POST helper
    # --------------------------------------------------------------
    def _post(self, url, headers, payload, verify, cert):
        try:
            return requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                verify=verify,
                cert=cert,
                timeout=30
            )
        except Exception as e:
            raise AirflowException(f"POST failed: {e}")

    # --------------------------------------------------------------
    # INTERNAL: GET helper
    # --------------------------------------------------------------
    def _get(self, url, headers, verify, cert):
        try:
            return requests.get(url, headers=headers, verify=verify, cert=cert, timeout=30)
        except Exception:
            return None

    # --------------------------------------------------------------
    # MAIN EXECUTION
    # --------------------------------------------------------------
    def execute(self, context):
        job_id = f"{self.task_id}_{context['run_id']}"

        token, verify, cert = self._get_conn()

        run_url = f"https://{self.target_server}/run"
        status_url = f"https://{self.target_server}/status/{job_id}"

        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        self.log.info("Triggering job_id=%s on agent=%s", job_id, self.target_server)

        payload = {
            "job_id": job_id,
            "command": self.command,
            "run_as_user": self.job_user,
            "fire_and_forget": True,        # Always fire and forget
            "skip_if_running": True,        # Prevent duplicates
            "timeout_seconds": self.timeout_seconds,
            "env": {},
        }

        resp = self._post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"Agent /run failed: {resp.text}")

        data = resp.json()
        status = data.get("status")

        if status == "already_running":
            self.log.info("Job %s is already running on target. Skipping.", job_id)
            return {"job_id": job_id, "status": "already_running"}

        # Submission OK
        self.log.info("Job submitted successfully: %s", data)

        # ------------------------------------------------------------------
        # Built-in SENSOR section: Poll until agent marks finished/failed
        # ------------------------------------------------------------------
        self.log.info("Starting internal status polling for job_id=%s", job_id)

        start = time.time()

        while True:
            if time.time() - start > self.timeout_seconds:
                raise AirflowException(
                    f"Timeout waiting for job {job_id} to finish."
                )

            r = self._get(status_url, headers, verify, cert)
            if not r:
                time.sleep(self.poll_interval)
                continue

            info = r.json()
            agent_status = info.get("status", "").strip()
            rc = info.get("return_code")

            self.log.info("Polled status: %s -> rc=%s", agent_status, rc)

            if agent_status == "finished":
                if rc is None or rc == 0:
                    self.log.info("Job %s completed successfully.", job_id)
                    return info
                else:
                    raise AirflowException(
                        f"Agent job failed (rc={rc}): {info.get('stderr')}"
                    )

            if agent_status == "failed":
                raise AirflowException(
                    f"Agent job failed: {info}"
                )

            time.sleep(self.poll_interval)
