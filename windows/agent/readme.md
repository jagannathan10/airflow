Save as:
C:\airflow_agent\install-agent.ps1


    $svcName = "AirflowAgent"
    $svcDisplay = "Airflow Windows Agent Service"
    
    $scriptPath = "C:\airflow_agent\agent-service.ps1"
    
    if (!(Test-Path $scriptPath)) {
        Write-Error "agent-service.ps1 not found"
        exit 1
    }
    
    New-Service -Name $svcName `
        -BinaryPathName "powershell.exe -ExecutionPolicy Bypass -File `"$scriptPath`"" `
        -DisplayName $svcDisplay `
        -StartupType Automatic
    
    Start-Service $svcName
    Write-Output "Airflow Agent installed & started."



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
