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



When agent runs as root, you only need:

# Directory for the agent code
    /opt/airflow_agent            owned by root:root
    /opt/airflow_agent/agent_unified.py

# Job directory (created by agent as root)
    /opt/airflow_agent/jobs       owned by root:root


Recommended permissions:

    chown -R root:root /opt/airflow_agent
    chmod 755 /opt/airflow_agent
    chmod -R 755 /opt/airflow_agent/jobs


Root will auto-create jobs/<job_id> with the right user permissions for each.

No user-specific permissions required

Since root user handles job execution and switching user context, there is nothing that target users need to do.

✅ 2️⃣ Final Hardened systemd Service File

👉 With root user
👉 Running uvicorn via the embedded Python launcher
👉 Correct cert usage from config.xml
👉 Persistent cleanup thread inside the agent
👉 SELinux-friendly

Save as:

/etc/systemd/system/airflow-agent.service

    [Unit]
    Description=Airflow Unified Root TMUX Agent
    After=network.target
    Wants=network-online.target
    
    [Service]
    # -------- RUN AS ROOT --------
    User=root
    Group=root
    
    # -------- WORKING DIRECTORY --------
    WorkingDirectory=/opt/airflow_agent
    
    # -------- ENV VARS --------
    Environment="PYTHONUNBUFFERED=1"
    Environment="AGENT_CONFIG=/opt/airflow_agent/config.xml"
    
    # -------- START THE AGENT --------
    ExecStart=/usr/bin/python3 /opt/airflow_agent/agent_unified.py
    
    # -------- RESTART POLICY --------
    Restart=always
    RestartSec=5
    StartLimitIntervalSec=60
    StartLimitBurst=5
    
    # Logs go to journal
    StandardOutput=journal
    StandardError=journal
    
    # -------- SECURITY HARDENING --------
    # Protect system but allow user switching + tmux
    ProtectSystem=full
    ProtectHome=yes
    NoNewPrivileges=no             # Must be NO for tmux + su to work
    PrivateTmp=yes
    
    [Install]
    WantedBy=multi-user.target

⚙️ 3️⃣ Why we MUST run as root for switching users

Your job execution uses:

su - {payload.run_as_user} -c '{payload.command}'


Only root can:

create a session for another user

load that user's entire environment (HOME, profile, shell)

execute secure commands under that user

create tmux sessions that belong to root but run user commands

Running as airflow would break:

password-less su

SELinux enforcement

tmux session creation under different UID

HOME directory permissions

full multi-user support

Banks normally run agents like this as root with a hardened boundary (firewall/TLS/token/command-blacklist).

💾 4️⃣ Permissions for TLS and config.xml

Ensure root can read:

/opt/airflow_agent/certs/cert.pem
/opt/airflow_agent/certs/key.pem
/opt/airflow_agent/config.xml
/opt/airflow_agent/agent_unified.py


Recommended:

chown -R root:root /opt/airflow_agent/certs
chmod 600 /opt/airflow_agent/certs/key.pem
chmod 644 /opt/airflow_agent/certs/cert.pem
chmod 600 /opt/airflow_agent/config.xml

🔐 5️⃣ SELinux considerations

When running as root:

tmux works

su works

file operations in /opt/airflow_agent work

If SELinux blocks something, create a policy from audit logs:

ausearch -m AVC -ts recent > /tmp/avc.log
audit2allow -M airflowagent < /tmp/avc.log
semodule -i airflowagent.pp


I can generate a clean .te module if you share the AVC logs.

🧪 6️⃣ Test after enabling systemd
systemctl daemon-reload
systemctl enable airflow-agent
systemctl start airflow-agent
systemctl status airflow-agent
journalctl -u airflow-agent -f


You should see:

[agent] Started. BASE_DIR=/opt/airflow_agent, TMUX=/usr/bin/tmux


Then test:

curl -k -X POST https://<server>:18443/ping -H "X-Agent-Token: <token>"

🎉 Final answer: YES — running agent as root is correct & supported

And the systemd file above is the correct, hardened configuration for your unified TMUX-only agent.

If you want, I can next send:

✔ hardened version of systemd
✔ RHEL7 compatibility
✔ Windows-service agent version
✔ SELinux .te file
✔ DAG examples for multi-server orchestration

Just tell me:
“Send hardened version” or “Send SELinux module”
