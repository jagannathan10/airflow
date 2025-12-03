Save as:
C:\airflow_agent\install-agent.ps1


    $svcName = "AirflowAgent"
    $svcDisplay = "Airflow Windows Agent Service"
    $agentPath = "C:\airflow_agent\agent-service.ps1"
    
    # Stop existing service
    Get-Service $svcName -ErrorAction SilentlyContinue | ForEach-Object {
        Stop-Service $_ -Force -ErrorAction SilentlyContinue
        sc.exe delete $svcName | Out-Null
    }
    
    # Install
    New-Service -Name $svcName -BinaryPathName "powershell.exe -ExecutionPolicy Bypass -File `"$agentPath`"" `
        -DisplayName $svcDisplay -StartupType Automatic
    
    Start-Service $svcName
    
    Write-Host "Airflow Windows Agent installed & started."


uninstall-agent.ps1 (Clean remove)

        $svcName = "AirflowAgent"
        
        Stop-Service $svcName -Force -ErrorAction SilentlyContinue
        sc.exe delete $svcName | Out-Null
        
        Write-Host "Airflow Agent removed."

🟢 How to Install
mkdir C:\airflow_agent
cd C:\airflow_agent

# Place all files + certs here
.\install-agent.ps1

🔵 How to Remove
cd C:\airflow_agent
.\uninstall-agent.ps1

🎯 Agent Endpoints (HTTPS)
Method	Endpoint	Description
POST	/run	Start Job
GET	/status?id=JOBID	Job Status
GET	/logs?id=JOBID	Fetch Logs
POST	/cleanup?id=JOBID	Cleanup

Folder Structure
C:\airflow_agent\
    agent-service.ps1
    install-agent.ps1
    certs\
        cert.pem
        key.pem
    logs\

4️⃣ How to Install

Run as Administrator:

Set-ExecutionPolicy Bypass -Force
cd C:\airflow_agent
.\install-agent.ps1


Service starts automatically:

AirflowAgent


Check:

Get-Service AirflowAgent
