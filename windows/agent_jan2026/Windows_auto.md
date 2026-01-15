Below is a complete automation approach for Windows agent rollout:

PowerShell installer (creates folders, downloads EXE + config + certs, sets ACLs, opens firewall, installs service, starts, health-check)

Optional upgrade mode (replace EXE + restart)

Optional Chocolatey package layout + chocolateyinstall.ps1 that does the same (so you can push to internal choco repo)

I’m using your standards:

Base directory: C:\airflow_agent

Service name: airflow-agent

EXE: airflow_agent.exe

Config: config.xml (YAML)

Certs: cert.pem, key.pem

Token/auth: from config (or fallback in code)

verify_ssl=false on Airflow side is ok for self-signed

1) PowerShell “one-click” installer script

Save as: install_airflow_agent.ps1

You just set your Artifactory URLs and allowed IP CIDR.
This script is idempotent: running again upgrades/reconfigures.

    
    param(
      [string]$BaseDir = "C:\airflow_agent",
      [string]$ServiceName = "airflow-agent",
      [string]$DisplayName = "Airflow Remote Agent",
      [string]$ExeName = "airflow_agent.exe",
      [string]$ListenHost = "0.0.0.0",
      [int]$ListenPort = 18443,
    
      # Artifactory URLs (example placeholders)
      [string]$ExeUrl   = "https://artifactory.example.com/airflow-agent/windows/airflow_agent.exe",
      [string]$CertUrl  = "https://artifactory.example.com/airflow-agent/certs/cert.pem",
      [string]$KeyUrl   = "https://artifactory.example.com/airflow-agent/certs/key.pem",
      [string]$ConfigUrl = "",
    
      # If ConfigUrl empty, script writes config.xml locally using these:
      [string]$Token = "scb-airflowagent-CHANGE-ME",
      [string[]]$AllowedCidrs = @("10.0.0.0/8"),
      [int]$RateWindowSeconds = 60,
      [int]$RateMaxRequests = 120,
      [int]$RetentionDays = 60,
    
      [switch]$InsecureTlsDownload # if Artifactory uses internal TLS chain issues
    )
    
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    
    function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
    function Write-Warn($msg) { Write-Host "[WARN] $msg" -ForegroundColor Yellow }
    function Write-Err ($msg) { Write-Host "[ERR ] $msg" -ForegroundColor Red }
    
    function Ensure-Dir($p) {
      if (!(Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null }
    }
    
    function Download-File($url, $dest) {
      Write-Info "Downloading: $url -> $dest"
      if ($InsecureTlsDownload) {
        # Use .NET to ignore cert validation (only for controlled intranet)
        add-type @"
    using System.Net;
    using System.Security.Cryptography.X509Certificates;
    public class TrustAllCertsPolicy : ICertificatePolicy {
      public bool CheckValidationResult(
        ServicePoint srvPoint, X509Certificate certificate,
        WebRequest request, int certificateProblem) { return true; }
    }
    "@
        [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
      }
    
      Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing
    }
    
    function Write-Config($path) {
      Write-Info "Writing config.xml to $path"
    
      $allowedYaml = ""
      foreach ($cidr in $AllowedCidrs) { $allowedYaml += "  - `"$cidr`"`n" }
    
      $content = @"
    listen:
      host: "$ListenHost"
      port: $ListenPort
    
    tls:
      server_cert: "$BaseDir\\certs\\cert.pem"
      server_key:  "$BaseDir\\certs\\key.pem"
    
    token: "$Token"
    
    allowed_ips:
    $allowedYaml
    command_blacklist:
      - "shutdown"
      - "reboot"
      - "rm -rf"
      - "format "
      - "diskpart"
      - "bcdedit"
      - "net user"
    
    rate_limit:
      window_seconds: $RateWindowSeconds
      max_requests: $RateMaxRequests
    
    retention_days: $RetentionDays
    "@
    
      $content | Out-File -FilePath $path -Encoding ascii
    }
    
    function Set-Permissions($baseDir) {
      Write-Info "Setting ACLs for SYSTEM on $baseDir"
      icacls $baseDir /grant "SYSTEM:(OI)(CI)RX" | Out-Null
      icacls "$baseDir\jobs" /grant "SYSTEM:(OI)(CI)M" | Out-Null
      icacls "$baseDir\certs" /grant "SYSTEM:(OI)(CI)R" | Out-Null
      icacls "$baseDir\config.xml" /grant "SYSTEM:R" | Out-Null
    }
    
    function Install-Or-Update-Service($exePath, $configPath) {
      $binPath = "`"$exePath`" --config `"$configPath`""
    
      $existing = (sc.exe query $ServiceName 2>$null)
      if ($LASTEXITCODE -eq 0) {
        Write-Info "Service exists. Updating binPath and restarting."
        sc.exe stop $ServiceName 2>$null | Out-Null
        sc.exe config $ServiceName binPath= $binPath start= auto DisplayName= "$DisplayName" | Out-Null
      } else {
        Write-Info "Creating service $ServiceName"
        sc.exe create $ServiceName binPath= $binPath start= auto DisplayName= "$DisplayName" | Out-Null
        sc.exe failure $ServiceName reset= 60 actions= restart/5000/restart/5000/restart/5000 | Out-Null
      }
    
      # Firewall rule (idempotent)
      $ruleName = "Airflow Agent $ListenPort"
      if (-not (Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue)) {
        New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Action Allow -Protocol TCP -LocalPort $ListenPort | Out-Null
      }
    
      Write-Info "Starting service..."
      sc.exe start $ServiceName | Out-Null
    }
    
    function Health-Check($host, $port, $token) {
      Write-Info "Health check /ping on https://$host:$port/ping"
      try {
        $headers = @{ "X-Agent-Token" = $token }
        $resp = Invoke-RestMethod -Method Post -Uri "https://127.0.0.1:$port/ping" -Headers $headers -SkipCertificateCheck
        Write-Info "Health OK: $($resp.status)"
      } catch {
        Write-Warn "Health check failed (may be cert trust issue if running remote). Try curl -k or check agent.log."
      }
    }
    
    # --------------------------------------------------------------------
    # MAIN
    # --------------------------------------------------------------------
    Ensure-Dir $BaseDir
    Ensure-Dir "$BaseDir\certs"
    Ensure-Dir "$BaseDir\jobs"
    
    $exePath = Join-Path $BaseDir $ExeName
    $configPath = Join-Path $BaseDir "config.xml"
    
    # Download artifacts
    Download-File $ExeUrl $exePath
    Download-File $CertUrl (Join-Path "$BaseDir\certs" "cert.pem")
    Download-File $KeyUrl  (Join-Path "$BaseDir\certs" "key.pem")
    
    # config.xml
    if ($ConfigUrl -and $ConfigUrl.Trim() -ne "") {
      Download-File $ConfigUrl $configPath
    } else {
      Write-Config $configPath
    }
    
    Set-Permissions $BaseDir
    
    Install-Or-Update-Service $exePath $configPath
    
    # Wait and check
    Start-Sleep -Seconds 2
    sc.exe query $ServiceName
    Health-Check "127.0.0.1" $ListenPort $Token
    
    Write-Info "Done. BaseDir=$BaseDir, Service=$ServiceName"




Run it (Admin PowerShell)

        powershell -ExecutionPolicy Bypass -File .\install_airflow_agent.ps1 `
          -ExeUrl "https://artifactory.../airflow_agent.exe" `
          -CertUrl "https://artifactory.../cert.pem" `
          -KeyUrl "https://artifactory.../key.pem" `
          -Token "scb-airflowagent-<your-long-token>" `
          -AllowedCidrs @("10.193.106.181/32","10.0.0.0/8")
    
    Remove service (separate script)
    sc.exe stop airflow-agent
    sc.exe delete airflow-agent
    Remove-Item -Recurse -Force C:\airflow_agent


2) Chocolatey packaging (internal repo)
2.1 Package structure

Create a folder:

    airflow-agent-choco\
      airflow-agent.nuspec
      tools\
        chocolateyinstall.ps1
        chocolateyuninstall.ps1

  2.2 airflow-agent.nuspec

    <?xml version="1.0" encoding="utf-8"?>
    <package xmlns="http://schemas.microsoft.com/packaging/2015/06/nuspec.xsd">
      <metadata>
        <id>airflow-agent</id>
        <version>1.0.0</version>
        <title>Airflow Remote Agent</title>
        <authors>SCB</authors>
        <owners>SCB</owners>
        <description>Airflow Remote Agent (Windows Service) installer.</description>
        <tags>airflow agent windows service</tags>
      </metadata>
    </package>

2.3 tools\chocolateyinstall.ps1

This calls the same logic as above (embedded). Keep it minimal:

    $ErrorActionPreference="Stop"
    
    $BaseDir="C:\airflow_agent"
    $ServiceName="airflow-agent"
    $ExeUrl="https://artifactory.example.com/airflow-agent/windows/airflow_agent.exe"
    $CertUrl="https://artifactory.example.com/airflow-agent/certs/cert.pem"
    $KeyUrl="https://artifactory.example.com/airflow-agent/certs/key.pem"
    $Token="scb-airflowagent-CHANGE-ME"
    $ListenPort=18443
    
    function Ensure-Dir($p){ if(!(Test-Path $p)){ New-Item -ItemType Directory -Path $p | Out-Null } }
    function Download-File($url,$dest){ Invoke-WebRequest -Uri $url -OutFile $dest -UseBasicParsing }
    
    Ensure-Dir $BaseDir
    Ensure-Dir "$BaseDir\certs"
    Ensure-Dir "$BaseDir\jobs"
    
    $exePath="$BaseDir\airflow_agent.exe"
    $configPath="$BaseDir\config.xml"
    
    Download-File $ExeUrl $exePath
    Download-File $CertUrl "$BaseDir\certs\cert.pem"
    Download-File $KeyUrl  "$BaseDir\certs\key.pem"
    
    @"
    listen:
      host: "0.0.0.0"
      port: $ListenPort
    tls:
      server_cert: "$BaseDir\\certs\\cert.pem"
      server_key:  "$BaseDir\\certs\\key.pem"
    token: "$Token"
    allowed_ips:
      - "10.0.0.0/8"
    command_blacklist:
      - "shutdown"
      - "reboot"
    rate_limit:
      window_seconds: 60
      max_requests: 120
    retention_days: 60
    "@ | Out-File -FilePath $configPath -Encoding ascii
    
    icacls $BaseDir /grant "SYSTEM:(OI)(CI)RX" | Out-Null
    icacls "$BaseDir\jobs" /grant "SYSTEM:(OI)(CI)M" | Out-Null
    icacls "$BaseDir\certs" /grant "SYSTEM:(OI)(CI)R" | Out-Null
    icacls "$BaseDir\config.xml" /grant "SYSTEM:R" | Out-Null
    
    $binPath="`"$exePath`" --config `"$configPath`""
    
    # Create/update service
    (sc.exe query $ServiceName) 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) {
      sc.exe stop $ServiceName 2>$null | Out-Null
      sc.exe config $ServiceName binPath= $binPath start= auto | Out-Null
    } else {
      sc.exe create $ServiceName binPath= $binPath start= auto | Out-Null
      sc.exe failure $ServiceName reset= 60 actions= restart/5000/restart/5000/restart/5000 | Out-Null
    }
    
    # Firewall
    $ruleName="Airflow Agent $ListenPort"
    if (-not (Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue)) {
      New-NetFirewallRule -DisplayName $ruleName -Direction Inbound -Action Allow -Protocol TCP -LocalPort $ListenPort | Out-Null
    }
    
    sc.exe start $ServiceName | Out-Null


  2.4 tools\chocolateyuninstall.ps1

    $ErrorActionPreference="Stop"
    $ServiceName="airflow-agent"
    
    sc.exe stop $ServiceName 2>$null | Out-Null
    sc.exe delete $ServiceName 2>$null | Out-Null
    
    Start-Sleep -Seconds 2
    Remove-Item -Recurse -Force C:\airflow_agent -ErrorAction SilentlyContinue

3) Build and push to your internal Chocolatey repo
Build .nupkg

From the airflow-agent-choco folder:

    choco pack
    airflow-agent.1.0.0.nupkg

Push to internal repo

Example (your repo URL will differ):

    choco push airflow-agent.1.0.0.nupkg --source "https://choco-repo.example.com/chocolatey" --api-key "YOUR_API_KEY"

Then on target hosts:

    choco install airflow-agent -y --source "https://choco-repo.example.com/chocolatey"

Practical notes for enterprise rollout

For 2000+ Windows nodes, prefer:

Chocolatey (standardized)

or SCCM / Intune / GPO running the PowerShell installer

Keep token out of package if required; inject via:

templated config pulled from Artifactory (host-specific)

or post-install Set-Content with secrets from vault (recommended)

