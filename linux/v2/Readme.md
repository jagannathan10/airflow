    from datetime import datetime
    from airflow import DAG
    from agent_tmux_unified import AgentTmuxUnifiedOperator
    
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
          # skip_if_running=True is DEFAULT
      )


Copy the code from agent_tmux_unified.py to $AIRFLOW_HOME/plugins/agent_tmux_unified.py
