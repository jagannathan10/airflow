Jan 2026 Update - working agent


Build 

    c:\airflow_agent>cd src
    
    c:\airflow_agent\src>go mod init airflow-agent 2>$null
    
    c:\airflow_agent\src>go mod init airflow-agent 2>$null
    
    c:\airflow_agent\src>go get gopkg.in/yaml.v3
    go: added gopkg.in/yaml.v3 v3.0.1
    
    c:\airflow_agent\src>go get golang.org/x/sys/windows/svc
    go: downloading golang.org/x/sys v0.40.0
    go: added golang.org/x/sys v0.40.0
    
    c:\airflow_agent\src>go get golang.org/x/sys/windows/svc/eventlog
       
    c:\airflow_agent\src>go build -o C:\airflow_agent\airflow_agent.exe .


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

    c:\airflow_agent>sc.exe start airflow-agent
    
    SERVICE_NAME: airflow-agent
            TYPE               : 10  WIN32_OWN_PROCESS
            STATE              : 2  START_PENDING
                                    (NOT_STOPPABLE, NOT_PAUSABLE, IGNORES_SHUTDOWN)
            WIN32_EXIT_CODE    : 0  (0x0)
            SERVICE_EXIT_CODE  : 0  (0x0)
            CHECKPOINT         : 0x0
            WAIT_HINT          : 0x7d0
            PID                : 6784
            FLAGS              :

    c:\airflow_agent>sc.exe query airflow-agent
    
    SERVICE_NAME: airflow-agent
            TYPE               : 10  WIN32_OWN_PROCESS
            STATE              : 4  RUNNING
                                    (STOPPABLE, NOT_PAUSABLE, ACCEPTS_SHUTDOWN)
            WIN32_EXIT_CODE    : 0  (0x0)
            SERVICE_EXIT_CODE  : 0  (0x0)
            CHECKPOINT         : 0x0
            WAIT_HINT          : 0x0
    
    c:\airflow_agent>

netstat -ano | findstr :18443


    @echo off
    setlocal
    
    set TARGET_DIR=C:\temp\testfolder
    set FILE_NAME=testfile.txt
    
    if not exist "%TARGET_DIR%" mkdir "%TARGET_DIR%"
    
    echo Test > "%TARGET_DIR%\%FILE_NAME%"
    if errorlevel 1 (
        echo ERROR: Failed to create file
        exit /b 1
    )
    
    echo SUCCESS: File created
    exit /b 0

Powershell

    $ErrorActionPreference = "Stop"
    
    $TargetDir = "C:\temp\testfolder"
    $FileName  = "testfile.txt"
    
    try {
        if (-not (Test-Path $TargetDir)) {
            New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null
        }
    
        "Test file created on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" |
            Set-Content -Path (Join-Path $TargetDir $FileName) -Encoding UTF8
    
        Write-Host "SUCCESS: File created"
        exit 0
    }
    catch {
        Write-Error "ERROR: Failed to create file - $($_.Exception.Message)"
        exit 1
    }



    curl.exe -k -X POST ^
      https://WINDOWS_HOST:18443/run ^
      -H "Content-Type: application/json" ^
      -H "X-Agent-Token: scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2" ^
      -d @- <<EOF
    {
      "command": "C:\\airflow_agent\\jobs\\test_job.bat",
      "job_id": "windows_test_job_001",
      "skip_if_running": true,
      "fire_and_forget": false,
      "use_tmux": false
    }
    EOF

    curl.exe -k -X GET ^
      https://WINDOWS_HOST:18443/status/windows_test_job_001 ^
      -H "X-Agent-Token: scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

        curl.exe -k -X GET ^
      "https://WINDOWS_HOST:18443/logs/windows_test_job_001?stream=stderr&offset=0" ^
      -H "X-Agent-Token: scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"


    curl.exe -k -X GET ^
      "https://WINDOWS_HOST:18443/logs/windows_test_job_001?stream=stdout&offset=0" ^
      -H "X-Agent-Token: scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

    curl.exe -k -X POST ^
      https://WINDOWS_HOST:18443/cancel/windows_test_job_001 ^
      -H "X-Agent-Token: scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"


