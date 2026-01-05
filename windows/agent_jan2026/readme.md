Jan 2026 Update - working agent


Build 

    C:\Users\jagan>cd c:\airflow_agent
    
    c:\airflow_agent>go mod init airflow_agent
    go: creating new go.mod: module airflow_agent
    go: to add module requirements and sums:
            go mod tidy
    
    c:\airflow_agent>go get gopkg.in/yaml.v3
    go: downloading gopkg.in/yaml.v3 v3.0.1
    go: added gopkg.in/yaml.v3 v3.0.1
    
    c:\airflow_agent>go mod tidy
    go: downloading gopkg.in/check.v1 v0.0.0-20161208181325-20d25e280405
    
    c:\airflow_agent>go build -o C:\airflow_agent\airflow_agent.exe .\airflow_agent_windows.go


Create certificate (we use self-signed)

    openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem -days 1825 -subj "/C=IN/ST=TN/L=Chennai/O=SCB/OU=Airflow/CN=airflow-agent"



How to start

    c:\airflow_agent>C:\airflow_agent\airflow_agent.exe --config C:\airflow_agent\config.xml
    2026/01/05 13:24:10 [AGENT] WARNING: token missing in config.xml → using fallback token
    2026/01/05 13:24:10 [AGENT] config.xml loaded
    2026/01/05 13:24:10 [AGENT] TLS enabled. Serving HTTPS on 0.0.0.0:18443
    2026/01/05 13:24:10 [AGENT] cert=C:\airflow_agent\certs\cert.pem key=C:\airflow_agent\certs\key.pem
    2026/01/05 13:24:10 [AGENT] WARNING: token missing in config.xml → using fallback token
    2026/01/05 13:24:10 [AGENT] config.xml loaded
