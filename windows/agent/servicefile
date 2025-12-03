# ================================
# Windows Airflow Agent Installer
# ================================

$Base = "C:\airflow_agent"
$CertPath = "$Base\certs"
$LogsPath = "$Base\logs"
$ServiceName = "AirflowWindowsAgent"

Write-Host "[*] Creating directories..."
New-Item -ItemType Directory -Force -Path $Base | Out-Null
New-Item -ItemType Directory -Force -Path $CertPath | Out-Null
New-Item -ItemType Directory -Force -Path $LogsPath | Out-Null

Write-Host "[*] Writing config.json..."
@"
{
  "port": 18443,
  "token": "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2",
  "poll_interval": 30
}
"@ | Out-File "$Base\config.json" -Encoding UTF8

Write-Host "[*] Checking for existing certificates..."
if (!(Test-Path "$CertPath\cert.pem") -or !(Test-Path "$CertPath\key.pem")) {
    Write-Host "[+] Generating self-signed certificate..."

    $cert = New-SelfSignedCertificate `
        -DnsName "localhost" `
        -CertStoreLocation "cert:\LocalMachine\My" `
        -KeyLength 4096 `
        -HashAlgorithm SHA256

    Export-PfxCertificate -Cert $cert -FilePath "$CertPath\cert.pfx" -Password (ConvertTo-SecureString "pass123" -AsPlainText -Force)
    openssl pkcs12 -in "$CertPath\cert.pfx" -nocerts -nodes -password pass:pass123 -out "$CertPath\key.pem"
    openssl pkcs12 -in "$CertPath\cert.pfx" -clcerts -nokeys -password pass:pass123 -out "$CertPath\cert.pem"
}

Write-Host "[*] Writing agent.ps1..."
@"
param()

# ================================
# Windows Airflow Agent
# HTTPS + Task Scheduler Backend
# ================================

\$Base = "C:\airflow_agent"
\$Config = Get-Content "\$Base\config.json" | ConvertFrom-Json
\$Token  = \$Config.token
\$Port   = \$Config.port
\$Poll   = \$Config.poll_interval

\$Listener = New-Object System.Net.HttpListener
\$Listener.Prefixes.Add("https://+:\$Port/")
\$Listener.Start()

Write-Host "Airflow Agent listening on https://0.0.0.0:\$Port"
Write-Host "Token: \$Token"

while (\$true) {
    \$ctx  = \$Listener.GetContext()
    \$req  = \$ctx.Request
    \$resp = \$ctx.Response

    # TOKEN CHECK
    if (\$req.Headers["X-Agent-Token"] -ne \$Token) {
        \$resp.StatusCode = 401
        \$resp.Close()
        continue
    }

    function SendJSON(\$obj) {
        \$json = \$obj | ConvertTo-Json -Depth 10
        \$buf  = [System.Text.Encoding]::UTF8.GetBytes(\$json)
        \$resp.ContentType = "application/json"
        \$resp.OutputStream.Write(\$buf, 0, \$buf.Length)
        \$resp.Close()
    }

    switch (\$req.Url.AbsolutePath) {

        "/run" {
            \$body = (New-Object IO.StreamReader \$req.InputStream).ReadToEnd() | ConvertFrom-Json
            
            \$id = \$body.id
            \$cmd = \$body.command
            \$RunAs = \$body.run_as_user

            # prevent duplicate run
            \$existing = schtasks /Query /TN "\$id" 2>&1
            if (\$existing -notmatch "ERROR") {
                SendJSON @{ status="duplicate"; message="Job already running" }
                break
            }

            # write script to temp
            \$psFile = "C:\Temp\${id}.cmd.ps1"
            \$logFile = "C:\airflow_agent\logs\${id}.log"
            \$exitFile = "C:\Temp\${id}.exit"

@"
Start-Transcript -Path "$logFile" -Force
try {
    $cmd
    \$exitCode = 0
} catch {
    \$_.Exception.Message | Out-File -Encoding utf8 "$logFile" -Append
    \$exitCode = 1
}
Stop-Transcript
Set-Content "$exitFile" \$exitCode
exit \$exitCode
"@ | Out-File \$psFile -Encoding UTF8

            # schedule + start
            schtasks /Create /TN "\$id" /SC ONCE /RU SYSTEM /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \$psFile" /ST 00:00 /F
            schtasks /Run /TN "\$id"

            SendJSON @{ status="started"; id=\$id }
        }

        "/status" {
            \$id = \$req.QueryString["id"]
            \$exitFile = "C:\Temp\${id}.exit"

            \$task = schtasks /Query /TN "\$id" 2>&1
            if (\$task -match "ERROR") {
                SendJSON @{ finished=\$true; exit_code=1 }
                break
            }

            if (!(Test-Path \$exitFile)) {
                SendJSON @{ finished=\$false; exit_code=\$null }
                break
            }

            \$code = Get-Content \$exitFile
            schtasks /Delete /TN "\$id" /F | Out-Null
            SendJSON @{ finished=\$true; exit_code=[int]\$code }
        }

        "/logs" {
            \$id = \$req.QueryString["id"]
            \$logFile = "C:\airflow_agent\logs\${id}.log"
            if (Test-Path \$logFile) {
                \$data = Get-Content \$logFile -Raw
                \$bytes = [System.Text.Encoding]::UTF8.GetBytes(\$data)
                \$resp.ContentType = "text/plain"
                \$resp.OutputStream.Write(\$bytes,0,\$bytes.Length)
            }
            \$resp.Close()
        }

        default {
            \$resp.StatusCode = 404
            \$resp.Close()
        }
    }
}
"@ | Out-File "$Base\agent.ps1" -Encoding UTF8

Write-Host "[*] Creating service $ServiceName..."

New-Service `
  -Name $ServiceName `
  -BinaryPathName "powershell.exe -ExecutionPolicy Bypass -File C:\airflow_agent\agent.ps1" `
  -DisplayName "Airflow Windows Agent" `
  -Description "Secure HTTPS agent for Airflow job execution" `
  -StartupType Automatic

Start-Service $ServiceName

Write-Host "`nInstallation complete."
Write-Host "Agent running on https://0.0.0.0:18443"
