# ==============================
# Airflow Windows Agent Service
# ==============================

param()

$ErrorActionPreference = "Stop"

# --- Configuration ---
$Port = 18443
$Token = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"
$BaseDir = "C:\airflow_agent"
$Cert = "$BaseDir\certs\cert.pem"
$Key  = "$BaseDir\certs\key.pem"
$LogDir = "$BaseDir\logs"

if (!(Test-Path $LogDir)) { New-Item -ItemType Directory $LogDir | Out-Null }

# --- Create EventLog source if missing ---
if (-not [System.Diagnostics.EventLog]::SourceExists("AirflowAgent")) {
    New-EventLog -LogName Application -Source "AirflowAgent"
}

function Log($msg) {
    Write-EventLog -LogName Application -Source "AirflowAgent" -EntryType Information -EventId 1000 -Message $msg
}

# --- HTTPS Listener ---
Add-Type -AssemblyName System.Net.HttpListener

$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("https://*:18443/")
$listener.Start()
Log "Airflow Agent Service Started on port 18443"

# --- Job Tracking ---
$Running = @{}

# --- API Routes ---
while ($true) {
    try {
        $ctx = $listener.GetContext()
        $req = $ctx.Request
        $resp = $ctx.Response

        # Token check
        if ($req.Headers["X-Agent-Token"] -ne $Token) {
            $resp.StatusCode = 401
            $resp.Close()
            continue
        }

        switch ($req.Url.AbsolutePath) {

            "/run" {
                $body = (New-Object IO.StreamReader($req.InputStream)).ReadToEnd()
                $data = $body | ConvertFrom-Json

                $id = $data.id
                $cmd = $data.command
                $taskName = "Airflow_$id"

                if ($Running.ContainsKey($id)) {
                    # Already running → do not trigger duplicate
                    $resp.StatusCode = 409
                    $resp.Close()
                    continue
                }

                # Write command file
                $cmdFile = "$LogDir\$id.ps1"
                $cmd | Out-File -Encoding UTF8 $cmdFile

                # Create scheduled task
                schtasks /Create /TN $taskName /TR "powershell -NoProfile -File `"$cmdFile`"" /SC ONCE /ST 00:00 /RL HIGHEST /RU SYSTEM /F | Out-Null
                schtasks /Run /TN $taskName | Out-Null

                $Running[$id] = $true
                $resp.StatusCode = 200
                $resp.Close()
            }

            "/status" {
                $id = $req.QueryString["id"]
                $exitFile = "$LogDir\$id.exit"

                if (Test-Path $exitFile) {
                    $code = Get-Content $exitFile | Out-String
                    $resp.StatusCode = 200
                    $json = "{ `"finished`": true, `"exit_code`": $code }"
                    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
                    $resp.OutputStream.Write($bytes,0,$bytes.Length)
                    $resp.Close()
                    $Running.Remove($id) | Out-Null
                    continue
                }

                $json = "{ `"finished`": false }"
                $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
                $resp.OutputStream.Write($bytes,0,$bytes.Length)
                $resp.Close()
            }

            "/logs" {
                $id = $req.QueryString["id"]
                $logFile = "$LogDir\$id.log"

                if (Test-Path $logFile) {
                    $content = Get-Content $logFile -Raw
                } else {
                    $content = ""
                }

                $bytes = [System.Text.Encoding]::UTF8.GetBytes($content)
                $resp.OutputStream.Write($bytes,0,$bytes.Length)
                $resp.Close()
            }

            Default {
                $resp.StatusCode = 404
                $resp.Close()
            }
        }
    }
    catch {
        Log "Error: $_"
    }
}
