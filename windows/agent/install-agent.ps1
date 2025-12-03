<# 
===============================================
 Windows Airflow Agent - PowerShell Version
 TLS 18443, Embedded Token, Task Scheduler Service
===============================================
#>

# -------------------------
# CONFIGURATION
# -------------------------

$AgentPort = 18443

# Embedded Token (as requested)
$AgentToken = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

$AgentHome = "C:\airflow_agent"
$CertDir   = "$AgentHome\certs"
$LogDir    = "$AgentHome\logs"
$JobDir    = "$AgentHome\jobs"

# Create directory structure
New-Item -ItemType Directory -Force -Path $AgentHome,$CertDir,$LogDir,$JobDir | Out-Null

# -------------------------
# TLS Certificate Handling
# -------------------------
$CertPath = "$CertDir\cert.pem"
$KeyPath  = "$CertDir\key.pem"

if (!(Test-Path $CertPath) -or !(Test-Path $KeyPath)) {
    Write-Host "ERROR: TLS certificate or key missing in $CertDir"
    exit 1
}

# Load certificate
$Cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2
$Cert.Import($CertPath)

# -------------------------
# Function: Send JSON Output
# -------------------------
function Send-JSON($Response, $Object) {
    $json = ($Object | ConvertTo-Json -Depth 5)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)

    $Response.ContentType = "application/json"
    $Response.ContentLength64 = $bytes.Length
    $Response.OutputStream.Write($bytes, 0, $bytes.Length)
    $Response.OutputStream.Close()
}

# -------------------------
# JOB MANAGEMENT HELPERS
# -------------------------

function Get-JobLogPath($JobID) { return "$JobDir\$JobID.log" }
function Get-JobExitPath($JobID) { return "$JobDir\$JobID.exit" }
function Get-JobCmdPath($JobID) {  return "$JobDir\$JobID.ps1" }

function Is-JobRunning($JobID) {
    schtasks /Query /TN $JobID /FO LIST 2>$null | Select-String "Running" | ForEach-Object { return $true }
    return $false
}

# -------------------------
# MAIN AGENT SERVER SCRIPT
# -------------------------

$ScriptPath = "$AgentHome\agent-service.ps1"

@"
# ============================================
#   Running Windows Airflow Agent Service
# ============================================

`$Listener = New-Object System.Net.HttpListener
`$Prefix = "https://+:$AgentPort/"
`$Listener.Prefixes.Add(`$Prefix)
`$Listener.Start()

Write-Host "Windows Airflow Agent running on port $AgentPort"

while (`$true) {
    try {
        `$Context = `$Listener.GetContext()
        `$Req = `$Context.Request
        `$Resp = `$Context.Response

        # Security Check
        if (-not `$Req.Headers["X-Agent-Token"] -or `$Req.Headers["X-Agent-Token"] -ne "$AgentToken") {
            Send-JSON `$Resp @{ error="Invalid token" }
            continue
        }

        switch (`$Req.Url.AbsolutePath) {

            "/run" {
                `$Body = [IO.StreamReader]::new(`$Req.InputStream).ReadToEnd() | ConvertFrom-Json
                `$JobID = `$Body.id
                `$Cmd   = `$Body.command

                if (Is-JobRunning `$JobID) {
                    Send-JSON `$Resp @{ running=$true }
                    continue
                }

                # Write command to PS1 file
                `$ScriptFile = "$(Get-JobCmdPath $JobID)"
                `$Cmd | Out-File `$ScriptFile -Encoding UTF8

                # Create log + exit placeholders
                `$LogFile = "$(Get-JobLogPath $JobID)"
                `$ExitFile = "$(Get-JobExitPath $JobID)"
                if (Test-Path `$ExitFile) { Remove-Item `$ExitFile -Force }

                # Create scheduled task
                schtasks /Create /TN `$JobID /TR "powershell -NoProfile -ExecutionPolicy Bypass -File `"$ScriptFile`"" /SC ONCE /ST 00:00 /RU SYSTEM /F | Out-Null
                schtasks /Run /TN `$JobID | Out-Null

                Send-JSON `$Resp @{ started=$true }
            }

            "/status" {
                `$JobID = `$Req.QueryString["id"]
                `$ExitFile = "$(Get-JobExitPath $JobID)"

                if (Is-JobRunning `$JobID) {
                    Send-JSON `$Resp @{ finished=$false }
                }
                elseif (Test-Path `$ExitFile) {
                    `$Code = [int](Get-Content `$ExitFile)
                    Send-JSON `$Resp @{ finished=$true; exit_code=`$Code }
                }
                else {
                    Send-JSON `$Resp @{ finished=$true; exit_code=1 }
                }
            }

            "/logs" {
                `$JobID = `$Req.QueryString["id"]
                `$LogFile = "$(Get-JobLogPath $JobID)"

                if (Test-Path `$LogFile) {
                    `$Bytes = [IO.File]::ReadAllBytes(`$LogFile)
                    `$Resp.ContentType = "text/plain"
                    `$Resp.OutputStream.Write(`$Bytes, 0, `$Bytes.Length)
                } else {
                    Send-JSON `$Resp @{ error="No logs found" }
                }
                `$Resp.OutputStream.Close()
            }

            "/cleanup" {
                `$JobID = `$Req.QueryString["id"]
                Remove-Item "$(Get-JobCmdPath $JobID)" -Force -ErrorAction SilentlyContinue
                Remove-Item "$(Get-JobExitPath $JobID)" -Force -ErrorAction SilentlyContinue
                Remove-Item "$(Get-JobLogPath $JobID)" -Force -ErrorAction SilentlyContinue

                Send-JSON `$Resp @{ cleaned=$true }
            }

            default {
                Send-JSON `$Resp @{ error="Unknown endpoint" }
            }
        }
    }
    catch {
        Write-Host "ERROR: $_"
    }
}

"@ | Out-File $ScriptPath -Encoding UTF8

# -------------------------
# INSTALL AS SCHEDULED TASK (SERVICE MODE)
# -------------------------

$Action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

$Trigger = New-ScheduledTaskTrigger -AtStartup

Register-ScheduledTask -TaskName "AirflowAgentService" `
    -Action $Action -Trigger $Trigger -RunLevel Highest -Force

Write-Host "Airflow Agent Installed Successfully."
Write-Host "Listening on HTTPS port $AgentPort"
