$ErrorActionPreference = "Stop"

# ===========================
# CONFIG
# ===========================
$Port      = 18443
$Token     = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"
$BasePath  = "C:\airflow_agent"
$CertPath  = "$BasePath\certs\cert.pem"
$KeyPath   = "$BasePath\certs\key.pem"
$PollInt   = 30

# Enable TLS12 / TLS13 only
[System.Net.ServicePointManager]::SecurityProtocol = `
    [System.Net.SecurityProtocolType]::Tls12 `
  -bor `
    [System.Net.SecurityProtocolType]::Tls13

# Ensure log folder exists
$LogDir = "$BasePath\logs"
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory $LogDir | Out-Null }

# ===========================
# Start HTTPS Listener
# ===========================
$Listener = New-Object System.Net.HttpListener
$Listener.Prefixes.Add("https://+:$Port/")
$Listener.Start()

Write-Host "Airflow Windows Agent listening on port $Port (TLS12/13)"

while ($true) {
    $Ctx = $Listener.GetContext()
    $Req = $Ctx.Request
    $Resp = $Ctx.Response

    try {
        # AUTH
        $reqToken = $Req.Headers["X-Agent-Token"]
        if ($reqToken -ne $Token) {
            $Resp.StatusCode = 403
            $Resp.OutputStream.Close()
            continue
        }

        # ROUTER
        switch ($Req.Url.AbsolutePath) {

            "/run" {
                $body = (New-Object IO.StreamReader($Req.InputStream)).ReadToEnd()
                $json = $body | ConvertFrom-Json

                $JobID = $json.id
                $Cmd   = $json.command

                # Prevent duplicate run
                $task = Get-ScheduledTask -TaskName $JobID -ErrorAction SilentlyContinue
                if ($task -and $task.State -eq "Running") {
                    $Resp.StatusCode = 409   # Conflict
                    $Resp.OutputStream.Close()
                    continue
                }

                $LogFile = "$BasePath\task_$JobID.log"
                $ExitFile = "$BasePath\task_$JobID.exit"

                if (Test-Path $LogFile) { Remove-Item $LogFile -Force }
                if (Test-Path $ExitFile) { Remove-Item $ExitFile -Force }

                $psScript = "$BasePath\$JobID.ps1"
                @"
try {
    & cmd.exe /c "$Cmd" *> "$LogFile"
    Set-Content "$ExitFile" 0
} catch {
    Set-Content "$ExitFile" 1
}
"@ | Set-Content $psScript

                schtasks /Create /TN $JobID /SC ONCE /RU SYSTEM /TR "powershell.exe -ExecutionPolicy Bypass -File $psScript" /ST 00:00 /F | Out-Null
                schtasks /Run /TN $JobID | Out-Null

                $Resp.StatusCode = 200
                $bytes = [Text.Encoding]::UTF8.GetBytes("{""status"":""started""}")
                $Resp.OutputStream.Write($bytes,0,$bytes.Length)
                $Resp.OutputStream.Close()
            }

            "/status" {
                $JobID = $Req.QueryString["id"]
                $task = Get-ScheduledTask -TaskName $JobID -ErrorAction SilentlyContinue

                if (!$task) {
                    $json = '{ "finished": true, "exit_code": 1 }'
                }
                elseif ($task.State -eq "Running") {
                    $json = '{ "finished": false }'
                }
                else {
                    $ExitFile = "$BasePath\task_$JobID.exit"
                    $code = 1
                    if (Test-Path $ExitFile) {
                        $code = [int](Get-Content $ExitFile)
                    }
                    $json = "{ ""finished"": true, ""exit_code"": $code }"
                }

                $bytes = [Text.Encoding]::UTF8.GetBytes($json)
                $Resp.OutputStream.Write($bytes, 0, $bytes.Length)
                $Resp.OutputStream.Close()
            }

            "/logs" {
                $JobID = $Req.QueryString["id"]
                $LogFile = "$BasePath\task_$JobID.log"

                if (Test-Path $LogFile) {
                    $txt = Get-Content $LogFile -Raw
                } else {
                    $txt = "NO LOG"
                }

                $bytes = [Text.Encoding]::UTF8.GetBytes($txt)
                $Resp.OutputStream.Write($bytes,0,$bytes.Length)
                $Resp.OutputStream.Close()
            }

            default {
                $Resp.StatusCode = 404
                $Resp.OutputStream.Close()
            }
        }

    } catch {
        $Resp.StatusCode = 500
        $Resp.OutputStream.Close()
    }
}
