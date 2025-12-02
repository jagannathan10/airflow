Works on both Linux and Windows

Uses tmux on Linux, detached cmd.exe on Windows

Reads everything (host, port, TLS certs, token, allowed_ips, blacklist, rate limits) from /opt/airflow_agent/config.xml (or C:\airflow_agent\config.xml on Windows)

Enforces:

Agent token via X-Agent-Token

Allowed IPs

Command blacklist

Simple per-IP rate limiting

Supports:

job_id / forced_job_id from Airflow operator

skip_if_running logic

Long-running jobs (fire-and-forget)

60-day cleanup thread

Note: config.xml is actually YAML (as in your example). You’ll need PyYAML installed: pip install pyyaml.

/etc/systemd/system/airflow-agent.service:

      [Unit]
      Description=Airflow Unified Agent
      After=network.target
      
      [Service]
      WorkingDirectory=/opt/airflow_agent
      Environment="PYTHONUNBUFFERED=1"
      
      ExecStart=/usr/bin/python3 /opt/airflow_agent/agent_unified.py
      
      User=root
      Restart=always
      RestartSec=5
      PrivateTmp=true
      NoNewPrivileges=true
      
      [Install]
      WantedBy=multi-user.target

Reload Systemd

      systemctl daemon-reload
      systemctl enable --now airflow-agent
      systemctl status airflow-agent

/opt/airflow_agent/config.xml & C:\airflow_agent\config.xml on Windows

      token: "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"
      
      allowed_ips:
        - "192.168.120.0/24"
        - "10.0.0.0/8"
      
      command_blacklist:
        - "shutdown"
        - "reboot"
        - "poweroff"
        - "init 0"
        - "halt"
        - "rm -rf /"
        - "mkfs "
        - ":(){ :|:& };:"
      
      rate_limit:
        window_seconds: 60
        max_requests: 120
      
      network:
        host: "0.0.0.0"
        port: 18443
      
      tls:
        server_cert: "/opt/airflow_agent/certs/cert.pem"
        server_key: "/opt/airflow_agent/certs/key.pem"



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
