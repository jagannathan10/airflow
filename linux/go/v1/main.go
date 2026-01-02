package main

import (
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "io/ioutil"
        "log"
        "net"
        "net/http"
        "os"
        "os/exec"
        "path/filepath"
        "strconv"
        "strings"
        "sync"
        "time"

        "gopkg.in/yaml.v3"
)

/*
   Universal Airflow Remote Agent (TMUX-only)

   - config.xml (YAML-style) with:
       listen.host / listen.port
       tls.server_cert / tls.server_key
       token (optional; fallback built into code)
       allowed_ips (CIDR)
       command_blacklist
       rate_limit.{window_seconds,max_requests}
       retention_days (not used yet in this snippet, but kept)

   - Per-job TMUX session: agent_<job_id>
   - Parallel-friendly: different job_id → different tmux session
*/

type Config struct {
        Listen struct {
                Host string `yaml:"host"`
                Port int    `yaml:"port"`
        } `yaml:"listen"`

        TLS struct {
                Cert string `yaml:"server_cert"`
                Key  string `yaml:"server_key"`
        } `yaml:"tls"`

        Token            string   `yaml:"token"`
        AllowedIPs       []string `yaml:"allowed_ips"`
        CommandBlacklist []string `yaml:"command_blacklist"`
        RateLimit        RLConfig `yaml:"rate_limit"`
        RetentionDays    int      `yaml:"retention_days"`
}

type RLConfig struct {
        WindowSeconds int `yaml:"window_seconds"`
        MaxRequests   int `yaml:"max_requests"`
}

var (
        configFile = "/opt/airflow_agent/config.xml"
        baseDir    = "/opt/airflow_agent"
        jobDir     = "/opt/airflow_agent/jobs"
        tmuxBin    = "/usr/bin/tmux"

        config     Config
        configLock sync.RWMutex

        // Fallback token if config.xml has no token
        DefaultAgentToken = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

        rateBucket = make(map[string][]int64) // ip → timestamps
        rateLock   sync.Mutex
)

// ---------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------

func loadConfig() error {
        data, err := ioutil.ReadFile(configFile)
        if err != nil {
                return err
        }

        var cfg Config
        if err := yaml.Unmarshal(data, &cfg); err != nil {
                return err
        }

        // Fallback token logic
        if strings.TrimSpace(cfg.Token) == "" {
                cfg.Token = DefaultAgentToken
                log.Println("[AGENT] WARNING: token missing in config.xml → using fallback token")
        }

        // Reasonable defaults
        if cfg.Listen.Host == "" {
                cfg.Listen.Host = "0.0.0.0"
        }
        if cfg.Listen.Port == 0 {
                cfg.Listen.Port = 18443
        }
        if cfg.RateLimit.WindowSeconds == 0 {
                cfg.RateLimit.WindowSeconds = 60
        }
        if cfg.RateLimit.MaxRequests == 0 {
                cfg.RateLimit.MaxRequests = 120
        }

        configLock.Lock()
        config = cfg
        configLock.Unlock()

        log.Println("[AGENT] config.xml loaded")
        return nil
}

func watchConfig() {
        var lastMod int64 = 0
        for {
                fi, err := os.Stat(configFile)
                if err == nil {
                        mt := fi.ModTime().Unix()
                        if mt != lastMod {
                                lastMod = mt
                                if err := loadConfig(); err != nil {
                                        log.Printf("[AGENT] ERROR reloading config.xml: %v\n", err)
                                }
                        }
                }
                time.Sleep(10 * time.Second)
        }
}

// ---------------------------------------------------------------------
// SECURITY HELPERS
// ---------------------------------------------------------------------

func validateToken(r *http.Request) error {
        configLock.RLock()
        expected := config.Token
        configLock.RUnlock()

        if strings.TrimSpace(expected) == "" {
                // shouldn't happen (we inject fallback), but be safe
                return nil
        }

        provided := r.Header.Get("X-Agent-Token")
        if provided != expected {
                return errors.New("invalid token")
        }
        return nil
}

func ipAllowed(ip string) bool {
        configLock.RLock()
        allowed := config.AllowedIPs
        configLock.RUnlock()

        if len(allowed) == 0 {
                return true
        }
        client := net.ParseIP(ip)
        if client == nil {
                return false
        }
        for _, cidr := range allowed {
                _, subnet, err := net.ParseCIDR(cidr)
                if err == nil && subnet.Contains(client) {
                        return true
                }
        }
        return false
}

func checkRateLimit(ip string) bool {
        configLock.RLock()
        window := config.RateLimit.WindowSeconds
        limit := config.RateLimit.MaxRequests
        configLock.RUnlock()

        now := time.Now().Unix()

        rateLock.Lock()
        defer rateLock.Unlock()

        bucket := rateBucket[ip]
        filtered := []int64{}
        for _, t := range bucket {
                if now-t <= int64(window) {
                        filtered = append(filtered, t)
                }
        }
        rateBucket[ip] = filtered

        if len(filtered) >= limit {
                return false
        }

        rateBucket[ip] = append(rateBucket[ip], now)
        return true
}

func blockedCommand(cmd string) bool {
        configLock.RLock()
        list := config.CommandBlacklist
        configLock.RUnlock()

        for _, b := range list {
                if b != "" && strings.Contains(cmd, b) {
                        return true
                }
        }
        return false
}

// ---------------------------------------------------------------------
// JOB MODEL + TMUX
// ---------------------------------------------------------------------

type JobRequest struct {
        Command       string            `json:"command"`
        RunAsUser     string            `json:"run_as_user"`
        JobID         string            `json:"job_id"`
        SkipIfRunning bool              `json:"skip_if_running"`
        FireAndForget bool              `json:"fire_and_forget"`
        Env           map[string]string `json:"env"`
        UseTmux       bool              `json:"use_tmux"`
}

func tmuxSession(jobID string) string {
        return "agent_" + jobID
}

func tmuxAlive(jobID string) bool {
        session := tmuxSession(jobID)
        cmd := exec.Command(tmuxBin, "has-session", "-t", session)
        err := cmd.Run()
        return err == nil
}

func buildScript(jobID string, req JobRequest) (string, error) {
        jobPath := filepath.Join(jobDir, jobID)
        if err := os.MkdirAll(jobPath, 0755); err != nil {
                return "", err
        }

        scriptFile := filepath.Join(jobPath, "run.sh")
        stdout := filepath.Join(jobPath, "stdout.log")
        stderr := filepath.Join(jobPath, "stderr.log")
        exitFile := filepath.Join(jobPath, "exit")
        statusFile := filepath.Join(jobPath, "status")

        _ = ioutil.WriteFile(statusFile, []byte("starting"), 0644)

        command := req.Command
        if req.RunAsUser != "" {
                safe := strings.ReplaceAll(command, "'", "'\"'\"'")
                command = "su - " + req.RunAsUser + " -c '" + safe + "'"
        }

        content := "#!/bin/bash\n" +
                "set -o pipefail\n" +
                command + " >> \"" + stdout + "\" 2>> \"" + stderr + "\"\n" +
                "echo $? > \"" + exitFile + "\"\n" +
                "echo finished > \"" + statusFile + "\"\n"

        if err := ioutil.WriteFile(scriptFile, []byte(content), 0755); err != nil {
                return "", err
        }
        return scriptFile, nil
}

func launchTmux(jobID string, req JobRequest) error {
        script, err := buildScript(jobID, req)
        if err != nil {
                return err
        }

        session := tmuxSession(jobID)
        cmd := exec.Command(tmuxBin, "new-session", "-d", "-s", session, "bash", "-lc", script)
        if err := cmd.Run(); err != nil {
                return err
        }

        statusFile := filepath.Join(jobDir, jobID, "status")
        _ = ioutil.WriteFile(statusFile, []byte("running"), 0644)
        return nil
}

// ---------------------------------------------------------------------
// HTTP HELPERS
// ---------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, obj interface{}) {
        w.Header().Set("Content-Type", "application/json")
        b, _ := json.Marshal(obj)
        _, _ = w.Write(b)
}

func readSafe(path string) string {
        data, _ := ioutil.ReadFile(path)
        return string(data)
}

func parseIntPtr(s string) *int {
        s = strings.TrimSpace(s)
        if s == "" {
                return nil
        }
        n, err := strconv.Atoi(s)
        if err != nil {
                return nil
        }
        return &n
}

// ---------------------------------------------------------------------
// HTTP HANDLERS
// ---------------------------------------------------------------------

func pingHandler(w http.ResponseWriter, r *http.Request) {
        ip := strings.Split(r.RemoteAddr, ":")[0]

        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
                return
        }
        if !ipAllowed(ip) {
                http.Error(w, `{"error":"ip_not_allowed"}`, http.StatusForbidden)
                return
        }
        if !checkRateLimit(ip) {
                http.Error(w, `{"error":"rate_limited"}`, http.StatusTooManyRequests)
                return
        }

        writeJSON(w, map[string]string{
                "status": "ok",
                "time":   time.Now().Format(time.RFC3339),
        })
}

func runHandler(w http.ResponseWriter, r *http.Request) {
        ip := strings.Split(r.RemoteAddr, ":")[0]

        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
                return
        }
        if !ipAllowed(ip) {
                http.Error(w, `{"error":"ip_not_allowed"}`, http.StatusForbidden)
                return
        }
        if !checkRateLimit(ip) {
                http.Error(w, `{"error":"rate_limited"}`, http.StatusTooManyRequests)
                return
        }

        var req JobRequest
        body, _ := ioutil.ReadAll(r.Body)
        _ = json.Unmarshal(body, &req)

        if blockedCommand(req.Command) {
                http.Error(w, `{"error":"command_blocked"}`, http.StatusBadRequest)
                return
        }

        if req.SkipIfRunning && tmuxAlive(req.JobID) {
                writeJSON(w, map[string]string{
                        "job_id": req.JobID,
                        "status": "already_running",
                })
                return
        }

        if err := launchTmux(req.JobID, req); err != nil {
                log.Printf("[AGENT] ERROR launchTmux: %v\n", err)
                http.Error(w, `{"error":"failed_to_launch"}`, http.StatusInternalServerError)
                return
        }

        writeJSON(w, map[string]string{
                "job_id": req.JobID,
                "status": "submitted",
        })
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
                return
        }

        jobID := strings.TrimPrefix(r.URL.Path, "/status/")

        jobPath := filepath.Join(jobDir, jobID)
        status := readSafe(filepath.Join(jobPath, "status"))
        exit := readSafe(filepath.Join(jobPath, "exit"))
        stdout := readSafe(filepath.Join(jobPath, "stdout.log"))
        stderr := readSafe(filepath.Join(jobPath, "stderr.log"))

        writeJSON(w, map[string]interface{}{
                "job_id":      jobID,
                "status":      strings.TrimSpace(status),
                "return_code": parseIntPtr(exit),
                "stdout":      stdout,
                "stderr":      stderr,
        })
}

func cancelHandler(w http.ResponseWriter, r *http.Request) {
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
                return
        }

        jobID := strings.TrimPrefix(r.URL.Path, "/cancel/")
        session := tmuxSession(jobID)

        _ = exec.Command(tmuxBin, "kill-session", "-t", session).Run()
        _ = ioutil.WriteFile(filepath.Join(jobDir, jobID, "status"), []byte("cancelled"), 0644)

        writeJSON(w, map[string]string{
                "job_id": jobID,
                "status": "cancelled",
        })
}

// ---------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------

func main() {
        cfgPath := flag.String("config", "/opt/airflow_agent/config.xml", "Path to config.xml")
        flag.Parse()
        configFile = *cfgPath

        if err := loadConfig(); err != nil {
                log.Printf("[AGENT] WARNING: could not load config.xml (%v). Using defaults.\n", err)
        }
        go watchConfig()

        if err := os.MkdirAll(jobDir, 0755); err != nil {
                log.Fatalf("[AGENT] Failed to create job dir: %v", err)
        }

        http.HandleFunc("/ping", pingHandler)
        http.HandleFunc("/run", runHandler)
        http.HandleFunc("/status/", statusHandler)
        http.HandleFunc("/cancel/", cancelHandler)

        configLock.RLock()
        host := config.Listen.Host
        port := config.Listen.Port
        cert := config.TLS.Cert
        key := config.TLS.Key
        configLock.RUnlock()

        addr := fmt.Sprintf("%s:%d", host, port)

        // Decide HTTP vs HTTPS based on cert/key presence
        if strings.TrimSpace(cert) == "" || strings.TrimSpace(key) == "" {
                log.Printf("[AGENT] TLS disabled (no cert/key). Serving HTTP on %s\n", addr)
                if err := http.ListenAndServe(addr, nil); err != nil {
                        log.Fatalf("[AGENT] HTTP server exited with error: %v", err)
                }
        } else {
                log.Printf("[AGENT] TLS enabled. Serving HTTPS on %s\n", addr)
                log.Printf("[AGENT] cert=%s key=%s\n", cert, key)
                if err := http.ListenAndServeTLS(addr, cert, key, nil); err != nil {
                        log.Fatalf("[AGENT] HTTPS server exited with error: %v", err)
                }
        }
}
