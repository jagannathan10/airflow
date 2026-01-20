"""
agent_tmux_unified_deferrable_goagent_addon.py

DEFERRABLE ADD-ON operator for the provided Go agent (main.go) WITHOUT changing your base code.

Key points:
- Deferrable means: submit job quickly, then defer (no long-running task pods on K8sExecutor).
- Triggerer polls /status/<job_id> asynchronously.
- Streaming "live" stdout/stderr while DEFERRED is not possible in Airflow (task isn't running).
  So this add-on supports:
    - optional log TAIL dump on completion (best-effort)
    - optional limited log scan (capped bytes) if you want more context

Go agent /logs contract supports offset reads per stream:
  GET /logs/<job_id>?stream=stdout|stderr&offset=<n> -> JSON with "next","data","eof"

Because /logs does NOT return file size, an efficient tail is not possible without scanning.
So we implement a SAFE capped scan (default 2MB per stream) to avoid pulling huge logs.
"""

import asyncio
from typing import Any, Dict, Optional, Tuple

import aiohttp
import requests

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException, AirflowFailException

# Import your original base operator module (unchanged)
from agent_tmux_unified import AgentTmuxUnifiedOperator


class GoAgentStatusTrigger(BaseTrigger):
    def __init__(
        self,
        *,
        target_server: str,
        job_id: str,
        headers: Dict[str, str],
        verify_ssl: bool,
        poll_interval: int,
        timeout_seconds: Optional[int] = None,  # None/<=0 => no timeout
        max_consecutive_failures: int = 60,
    ):
        super().__init__()
        self.target_server = target_server
        self.job_id = job_id
        self.headers = headers
        self.verify_ssl = bool(verify_ssl)
        self.poll_interval = int(poll_interval)
        self.timeout_seconds = timeout_seconds
        self.max_consecutive_failures = int(max_consecutive_failures)

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "agent_tmux_unified_deferrable_goagent_addon.GoAgentStatusTrigger",
            {
                "target_server": self.target_server,
                "job_id": self.job_id,
                "headers": self.headers,
                "verify_ssl": self.verify_ssl,
                "poll_interval": self.poll_interval,
                "timeout_seconds": self.timeout_seconds,
                "max_consecutive_failures": self.max_consecutive_failures,
            },
        )

    async def run(self):
        url = f"https://{self.target_server}/status/{self.job_id}"
        start = asyncio.get_event_loop().time()
        failures = 0

        timeout = aiohttp.ClientTimeout(total=30)
        ssl_arg = self.verify_ssl

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                if self.timeout_seconds and self.timeout_seconds > 0:
                    elapsed = asyncio.get_event_loop().time() - start
                    if elapsed > self.timeout_seconds:
                        yield TriggerEvent({"event": "timeout", "job_id": self.job_id, "message": "Remote wait timed out"})
                        return

                try:
                    async with session.get(url, headers=self.headers, ssl=ssl_arg) as resp:
                        if resp.status != 200:
                            failures += 1
                            if failures >= self.max_consecutive_failures:
                                body = await resp.text()
                                yield TriggerEvent(
                                    {"event": "error", "job_id": self.job_id, "message": f"/status HTTP {resp.status}: {body[:500]}"}
                                )
                                return
                            await asyncio.sleep(self.poll_interval)
                            continue

                        failures = 0
                        info = await resp.json()

                except Exception as e:
                    failures += 1
                    if failures >= self.max_consecutive_failures:
                        yield TriggerEvent({"event": "error", "job_id": self.job_id, "message": f"/status unreachable: {repr(e)}"})
                        return
                    await asyncio.sleep(self.poll_interval)
                    continue

                status = (info.get("status") or "").strip().lower()
                rc = info.get("return_code")

                if status == "finished":
                    yield TriggerEvent({"event": "finished", "job_id": self.job_id, "info": info, "return_code": rc})
                    return
                if status == "cancelled":
                    yield TriggerEvent({"event": "cancelled", "job_id": self.job_id, "info": info, "return_code": rc})
                    return

                await asyncio.sleep(self.poll_interval)


class AgentTmuxUnifiedDeferrableOperatorGo(AgentTmuxUnifiedOperator):
    """
    Deferrable wrapper around your base operator.
    - Keeps your submit payload and headers behaviour (token, ssl verify, cert) from base
    - Replaces the long polling with Triggerer
    - On completion: optional capped scan of stdout/stderr for diagnostics
    """

    def __init__(
        self,
        *args,
        timeout_seconds: Optional[int] = None,  # None/0 => no timeout
        max_consecutive_failures: int = 60,
        dump_logs_on_complete: bool = True,
        max_bytes_per_stream: int = 2 * 1024 * 1024,  # 2MB scan cap per stream
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.timeout_seconds = timeout_seconds
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.dump_logs_on_complete = bool(dump_logs_on_complete)
        self.max_bytes_per_stream = int(max_bytes_per_stream)
        self._job_id: Optional[str] = None

    def _scan_stream(self, job_id: str, headers: Dict[str, str], verify: bool, cert, stream: str) -> str:
        """
        Capped scan from offset=0 in chunks until EOF or cap reached.
        This is a best-effort diagnostic dump (not true tail).
        """
        if not self.dump_logs_on_complete:
            return ""

        offset = 0
        out_parts = []
        total = 0

        while total < self.max_bytes_per_stream:
            url = f"https://{self.target_server}/logs/{job_id}"
            params = {"stream": stream, "offset": str(offset)}
            try:
                r = requests.get(url, headers=headers, params=params, timeout=30, verify=verify, cert=cert)
            except Exception:
                break
            if r.status_code != 200:
                break
            try:
                data = r.json()
            except Exception:
                break

            chunk = data.get("data") or ""
            nxt = data.get("next")
            eof = bool(data.get("eof"))

            if chunk:
                out_parts.append(chunk)
                total += len(chunk.encode("utf-8", errors="ignore"))

            try:
                offset = int(nxt) if nxt is not None else offset + len(chunk.encode("utf-8", errors="ignore"))
            except Exception:
                offset = offset + len(chunk.encode("utf-8", errors="ignore"))

            if eof:
                break
            if not chunk and not eof:
                # no progress; avoid infinite loop
                break

        text = "".join(out_parts)
        if total >= self.max_bytes_per_stream:
            text += f"\n[log scan capped at {self.max_bytes_per_stream} bytes; increase max_bytes_per_stream if needed]\n"
        return text

    def execute(self, context):
        # Use base operator's job id generator
        job_id = self._generate_job_id(context)
        self._job_id = job_id

        token, verify, cert = self._prepare_conn()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        run_url = f"https://{self.target_server}/run"
        payload = {
            "command": self.command,
            "run_as_user": self.job_user or "",
            "job_id": job_id,
            "dedup_key": getattr(self, "dedup_key", "") or "",
            "skip_if_running": True,
            "fire_and_forget": False,
            "env": self.env,
            "use_tmux": True,
        }

        resp = self._post(run_url, headers, payload, verify, cert)
        if resp.status_code != 200:
            raise AirflowException(f"/run failed: {resp.text[:1000]}")
        data = resp.json()
        st = (data.get("status") or "").lower()

        # Preserve already_running behavior if your base operator supports queueing logic elsewhere
        if st not in ("submitted", "already_running"):
            raise AirflowException(f"Unexpected agent response: {data}")

        self.log.info("[Agent] job_id=%s started (DEFERRABLE).", job_id)

        self.defer(
            trigger=GoAgentStatusTrigger(
                target_server=self.target_server,
                job_id=job_id,
                headers=headers,
                verify_ssl=verify,
                poll_interval=self.poll_interval,
                timeout_seconds=self.timeout_seconds,
                max_consecutive_failures=self.max_consecutive_failures,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        if not event:
            raise AirflowFailException("No trigger event received")

        job_id = event.get("job_id") or self._job_id
        token, verify, cert = self._prepare_conn()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["X-Agent-Token"] = token

        ev = event.get("event")
        info = event.get("info") or {}
        rc = event.get("return_code")

        if self.dump_logs_on_complete and job_id:
            out = self._scan_stream(job_id, headers, verify, cert, "stdout")
            err = self._scan_stream(job_id, headers, verify, cert, "stderr")
            if out.strip():
                for line in out.splitlines():
                    self.log.info("[STDOUT] %s", line)
            if err.strip():
                for line in err.splitlines():
                    self.log.warning("[STDERR] %s", line)

        if ev == "timeout":
            raise AirflowFailException(event.get("message") or "Remote wait timed out")
        if ev == "error":
            raise AirflowFailException(event.get("message") or "Error while waiting for remote job")
        if ev == "cancelled":
            raise AirflowFailException("Remote job cancelled")

        if ev == "finished":
            if rc is None:
                raise AirflowFailException(f"Finished but return_code is null: {info}")
            try:
                rc_i = int(rc)
            except Exception:
                raise AirflowFailException(f"Invalid return_code: {info}")
            if rc_i != 0:
                raise AirflowFailException(f"Remote TMUX job failed rc={rc_i}: {info}")
            return info

        raise AirflowFailException(f"Unexpected trigger event: {event}")
