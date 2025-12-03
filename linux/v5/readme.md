from datetime import datetime
from airflow import DAG
from agent_tmux_unified_operator import AgentTmuxUnifiedOperator

with DAG(
    dag_id="etl_tmux_job",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
):

    AgentTmuxUnifiedOperator(
        task_id="run_etl",
        target_server="192.168.120.139:18443",
        command="sh /home/testuser/run_etl.sh",
        job_user="testuser",
        agent_conn_id="agent_default",
        # skip_if_running=True by default
        # job_id=None → derived from dag_id / task_id / run_id
    )
