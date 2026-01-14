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

Create Service 

    $svcName = "airflow-agent"
    $bin = "C:\airflow_agent\airflow_agent.exe"
    $args = "--config C:\airflow_agent\config.xml"
    
    # Create service
    sc.exe create $svcName binPath= "`"$bin`" $args" start= auto DisplayName= "Airflow Remote Agent"
    sc.exe failure $svcName reset= 60 actions= restart/5000/restart/5000/restart/5000
    
    # Open firewall port
    New-NetFirewallRule -DisplayName "Airflow Agent 18443" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 18443
    
    # Start service
    sc.exe start $svcName

runit (Admin powershell)

    powershell -ExecutionPolicy Bypass -File C:\airflow_agent\install-service.ps1

Verify

    powershell -ExecutionPolicy Bypass -File C:\airflow_agent\install-service.ps1

sc.exe query airflow-agent
netstat -ano | findstr 18443


sc.exe create airflow-agent binPath= "\"C:\airflow_agent\airflow_agent.exe\" --config \"C:\airflow_agent\config.xml\"" start= auto
sc.exe failure airflow-agent reset= 60 actions= restart/5000/restart/5000/restart/5000
sc.exe start airflow-agent
sc.exe query airflow-agent
