# ============================================================
# Windows Airflow Agent Service (HTTPS + Task Scheduler)
# Port: 18443
# Token: embedded
# No cipher restrictions
# ============================================================

$ErrorActionPreference = "Stop"

# -------------------------------
# CONFIG
# -------------------------------
$Port       = 18443
$BaseDir    = "C:\airflow_agent"
$CertDir    = "$BaseDir\certs"
$LogDir     = "$BaseDir\logs"
$Token      = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"
$Poll       = 30   # poll interval

# Ensure folders
New-Item -ItemType Directory -Path $BaseDir -Force | Out-Null
New-Item -ItemType Directory -Path $CertDir -Force | Out-Null
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null

# Certificates
$CertPath = "$CertDir\cert.pem"
$KeyPath  = "$CertDir\key.pem"

if (!(Test-Path $CertPath) -or !(Test-Path $KeyPath)) {
    Write-Error "Missing TLS certificate. Place cert.pem and key.pem in $CertDir"
}

# Load certificate
$cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($CertPath)

# ----------------------------------
# HTTP(S) Listener
# ----------------------------------
$listener = New-Object System.Net.HttpListener
$listener.Prefixes.Add("https://+:$Port/")
$listener.AuthenticationSchemes = "Anonymous"
$listener.Start()
Write-Output "Airflow Windows Agent running on https://+:$Port/"

# ----------------------------------
# Helper: send JSON
# ----------------------------------
function SendJSON($resp, $obj) {
    $json = ($obj | ConvertTo-Json -Depth 5)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
    $resp.ContentType = "application/json"
    $resp.ContentLength64 = $bytes.Length
    $resp.OutputStream.Write($bytes, 0, $bytes.Length)
    $resp.OutputStream.Close()
}

# ----------------------------------
# Helper: validate token
# ----------------------------------
function Validate-Token($context) {
    if (-not $context.Request.Headers["X-Agent-Token"]) {
        return $false
    }
    return ($context.Request.Headers["X-Agent-Token"] -eq $Token)
}

# ----------------------------------
# MAIN LOOP
# ----------------------------------
while ($true) {
    $context = $listener.GetContext()

    # Token check
    if (-not (Validate-Token $context)) {
        $res = $context.Response
        SendJSON $res @{ error="Invalid token" }
        continue
    }

    $path = $context.Request.Url.AbsolutePath.ToLower()

    switch ($path) {

        "/run" {
            $reqBody = (New-Object IO.StreamReader($context.Request.InputStream)).ReadToEnd()
            $data = $reqBody | ConvertFrom-Json

            $id = $data.id
            $command = $data.command

            $logFile = "$LogDir\$id.log"
            $exitFile = "$LogDir\$id.exit"

            # Do not run if a job already exists
            if (Get-ScheduledTask -TaskName $id -ErrorAction SilentlyContinue) {
                SendJSON $context.Response @{ msg="Already running" }
                continue
            }

            # Write command to PS1 file
            $cmdPath = "$BaseDir\$id.ps1"
            @"
`$ErrorActionPreference='Continue'
try {
    $command | Out-File "$logFile" -Append
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "$command" *>> "$logFile"
    `$exit = if (`$LASTEXITCODE) { `$LASTEXITCODE } else { 0 }
} catch {
    `$_ | Out-String | Out-File "$logFile" -Append
    `$exit = 1
}
"`$exit" | Out-File "$exitFile"
"@ | Out-File $cmdPath -Encoding UTF8

            # Create scheduled task
            schtasks /Create /TN $id /SC ONCE /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File $cmdPath" /ST 00:00 /RU SYSTEM /F | Out-Null
            schtasks /Run /TN $id | Out-Null

            SendJSON $context.Response @{ msg="Started"; id=$id }
        }

        "/status" {
            $id = $context.Request.QueryString["id"]

            $exitFile = "$LogDir\$id.exit"

            if (Test-Path $exitFile) {
                $code = Get-Content $exitFile
                SendJSON $context.Response @{ finished=$true; exit_code=[int]$code }
            } else {
                SendJSON $context.Response @{ finished=$false }
            }
        }

        "/logs" {
            $id = $context.Request.QueryString["id"]
            $logFile = "$LogDir\$id.log"

            $resp = $context.Response
            if (Test-Path $logFile) {
                $bytes = [System.IO.File]::ReadAllBytes($logFile)
                $resp.OutputStream.Write($bytes, 0, $bytes.Length)
            }
            $resp.Close()
        }

        "/cleanup" {
            $id = $context.Request.QueryString["id"]
            $logFile  = "$LogDir\$id.log"
            $exitFile = "$LogDir\$id.exit"

            if (Get-ScheduledTask -TaskName $id -ErrorAction SilentlyContinue) {
                schtasks /Delete /TN $id /F | Out-Null
            }

            Remove-Item $logFile -ErrorAction SilentlyContinue
            Remove-Item $exitFile -ErrorAction SilentlyContinue

            SendJSON $context.Response @{ cleaned=$true }
        }

        default {
            SendJSON $context.Response @{ error="Unknown endpoint $path" }
        }
    }
}
