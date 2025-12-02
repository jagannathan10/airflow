Sample DAG
      
    from datetime import datetime
    from airflow import DAG
    from agent_tmux_unified import AgentTmuxUnifiedOperator
    
    with DAG(
        dag_id="etl_multi_os",
        start_date=datetime(2025, 1, 1),
        schedule_interval="*/10 * * * *",
        catchup=False,
    ):
    
        linux_job = AgentTmuxUnifiedOperator(
            task_id="etl_linux",
            target_server="192.168.120.139:18443",
            command="sh /home/testuser/run_etl.sh",
            job_user="testuser",
        )
    
        windows_job = AgentTmuxUnifiedOperator(
            task_id="etl_windows",
            target_server="WINHOST:18443",
            command="C:\\scripts\\etl_job.bat",
            job_user=None,
        )
