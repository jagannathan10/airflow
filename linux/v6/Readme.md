Compatible with RHEL8/RHEL9 with SELinux enforcing.

/etc/systemd/system/airflow-agent.service
[Unit]
Description=Airflow Unified TMUX Agent (Root)
After=network.target
Wants=network-online.target

[Service]
User=root
Group=root

WorkingDirectory=/opt/airflow_agent

Environment="PYTHONUNBUFFERED=1"
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"
Environment="AIRFLOW_AGENT_HOME=/opt/airflow_agent"

ExecStart=/usr/local/bin/uvicorn agent:app \
    --host 0.0.0.0 \
    --port 18443 \
    --ssl-keyfile /opt/airflow_agent/certs/key.pem \
    --ssl-certfile /opt/airflow_agent/certs/cert.pem

Restart=always
RestartSec=5
StartLimitIntervalSec=60
StartLimitBurst=5

# Needed for tmux session creation
NoNewPrivileges=no
PrivateTmp=no

[Install]
WantedBy=multi-user.target

Permissions required
chown -R root:root /opt/airflow_agent
chmod 700 /opt/airflow_agent
chmod 600 /opt/airflow_agent/agent_token
chmod 600 /opt/airflow_agent/certs/*

Enable + start
systemctl daemon-reload
systemctl enable airflow-agent
systemctl start airflow-agent
systemctl status airflow-agent
