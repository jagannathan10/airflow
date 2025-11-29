/home/almalinux/airflow/plugins/agent_operators.py
touch /home/almalinux/airflow/plugins/__init__.py

Sync DAG:

# /home/almalinux/airflow/dags/agent_sync_test.py
from datetime import datetime
from airflow import DAG
from agent_operators import AgentSyncOperator

with DAG(
    dag_id="agent_sync_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    AgentSyncOperator(
        task_id="sync_ls_root",
        target_server="192.168.120.139:18443",
        command="ls -l /root",
        job_user=None,                 # run as root
        agent_conn_id="agent_default",
        timeout_seconds=600,
    )


Async (non-tmux) DAG

# /home/almalinux/airflow/dags/agent_async_test.py
from datetime import datetime
from airflow import DAG
from agent_operators import AgentAsyncOperator

with DAG(
    dag_id="agent_async_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    AgentAsyncOperator(
        task_id="async_short_job",
        target_server="192.168.120.139:18443",
        command="sleep 30 && echo 'done async'",
        job_user=None,
        agent_conn_id="agent_default",
        timeout_seconds=900,
        poll_interval=15,
    )


TMUX + Sensor DAG (recommended for long jobs)

# /home/almalinux/airflow/dags/agent_tmux_test.py
from datetime import datetime
from airflow import DAG
from agent_operators import AgentTmuxOperator, AgentStatusSensor

with DAG(
    dag_id="agent_tmux_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/10 * * * *",   # every 10 minutes
    catchup=False,
):

    start_tmux = AgentTmuxOperator(
        task_id="tmux_longrun",
        target_server="192.168.120.139:18443",
        command="sh /home/testuser/run_etl.sh",
        job_user="testuser",
        fire_and_forget=True,      # IMPORTANT: no blocking in operator
        agent_conn_id="agent_default",
    )

    wait_tmux = AgentStatusSensor(
        task_id="wait_for_tmux_longrun",
        target_server="192.168.120.139:18443",
        agent_conn_id="agent_default",
        job_id="{{ ti.xcom_pull('tmux_longrun')['job_id'] }}",
        poke_interval=30,          # every 30s
        timeout=48 * 3600,         # up to 48 hours
    )

    start_tmux >> wait_tmux


what you want for 1–2 day jobs:

Operator is light

Sensor is light

Agent does all the heavy lifting

Airflow schedulers/workers stay healthy


agent_operators.py in plugins/

__init__.py in plugins/

Restart webserver + scheduler after plugin changes

Connection agent_default with:

Type: HTTP (or generic)

Extra JSON:

{
  "agent_token": "scb-airflowagent-.....",
  "verify_ssl": false
}
