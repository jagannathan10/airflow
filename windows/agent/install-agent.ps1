### ============================================
### Airflow Windows Agent - HTTPS Service
### ============================================

param()

# --- Settings ---
$PORT       = 18443
$TOKEN      = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"
$BASE       = "C:\airflow_agent"
$CERT_PATH  = "$BASE\certs\cert.pem"
$KEY_PATH   = "$BASE\certs\key.pem"
$TASK_DIR   = "$BASE\tasks"
$LOG_DIR    = "$BASE\logs"

New-Item -ItemType Directory -Force -Path $TASK_DIR | Out-Null
New-Item -ItemType Directory -Force -Path $LOG_DIR  | Out-Null

# --- TLS 1.2 / TLS 1.3 only ---
[Net.ServicePointManager]::SecurityProtocol =
    [Net.SecurityProtocolType]::Tls12 `
  -bor `
    [Net.SecurityProtocolType]::Tls13

# --- Create HTTPS listener ---
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("https://+:$PORT/")
$listener.Start()
Write-Host "Windows Agent running on https://0.0.0.0:$PORT"

function SendJSON($respObj, $ctx) {
    $json = ($respObj | ConvertTo-Json -Depth 6)
    $buffer = [System.Text.Encoding]::UTF8.GetBytes($json)

    $ctx.Response.StatusCode = 200
    $ctx.Response.ContentType = "application/json"
    $ctx.Response.OutputStream.Write($buffer,0,$buffer.Length)
    $ctx.Response.Close()
}

function CheckAuth($ctx) {
    if ($ctx.Request.Headers["X-Agent-Token"] -ne $TOKEN) {
        SendJSON @{ error="Unauthorized" } $ctx
        return $false
    }
    return $true
}

while ($true) {
    $ctx = $listener.GetContext()
    $path = $ctx.Request.Url.AbsolutePath.ToLower()

    if (-not (CheckAuth $ctx)) { continue }

    switch ($path) {

        "/run" {
            $body   = (New-Object IO.StreamReader($ctx.Request.InputStream)).ReadToEnd()
            $data   = $body | ConvertFrom-Json
            $id     = $data.id
            $cmd    = $data.command

            $logFile  = "$LOG_DIR\$id.log"
            $exitFile = "$TASK_DIR\$id.exit"

            # No duplicate runs
            $exists = schtasks /Query /TN $id 2>$null
            if ($LASTEXITCODE -eq 0) {
                SendJSON @{ status="already_running" } $ctx
                continue
            }

            # Write command to temp script
            $psCmd = "$TASK_DIR\$id.ps1"
            @"
\$ErrorActionPreference='Continue'
try {
    $cmd
    \$code = \$LASTEXITCODE
} catch {
    \$code = 1
}
Set-Content -Path '$exitFile' -Value \$code
"@ | Out-File $psCmd -Encoding UTF8

            # Create task
            schtasks /Create /TN $id /SC ONCE /RU SYSTEM /TR "powershell -ExecutionPolicy Bypass -File `"$psCmd`"" /ST 00:00 /F | Out-Null
            schtasks /Run /TN $id | Out-Null

            SendJSON @{ status="started"; id=$id } $ctx
        }

        "/status" {
            $id = $ctx.Request.QueryString["id"]

            $exitFile = "$TASK_DIR\$id.exit"
            $running  = schtasks /Query /TN $id 2>$null

            if ($LASTEXITCODE -ne 0) {
                SendJSON @{ finished=$true; exit_code=1; error="not_found" } $ctx
                continue
            }

            # Still running?
            if (-not (Test-Path $exitFile)) {
                SendJSON @{ finished=$false } $ctx
                continue
            }

            # Completed
            $code = Get-Content $exitFile
            schtasks /Delete /TN $id /F | Out-Null
            SendJSON @{ finished=$true; exit_code=[int]$code } $ctx
        }

        "/logs" {
            $id = $ctx.Request.QueryString["id"]
            $logFile = "$LOG_DIR\$id.log"

            if (Test-Path $logFile) {
                $content = Get-Content $logFile -Raw
            } else {
                $content = ""
            }

            SendJSON @{ logs=$content } $ctx
        }

        "/cleanup" {
            $id = $ctx.Request.QueryString["id"]
            Remove-Item "$TASK_DIR\$id*" -Force -ErrorAction SilentlyContinue
            Remove-Item "$LOG_DIR\$id*" -Force -ErrorAction SilentlyContinue
            schtasks /Delete /TN $id /F 2>$null | Out-Null

            SendJSON @{ cleaned=$true; id=$id } $ctx
        }

        default {
            SendJSON @{ error="Invalid endpoint" } $ctx
        }
    }
}
