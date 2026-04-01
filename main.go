package main

import (
        "crypto/sha1"
        "encoding/json"
        "errors"
        "flag"
        "fmt"
        "gopkg.in/yaml.v3"
        "io"
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
)

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
        AllowedIPs        []string `yaml:"allowed_ips"`
        CommandBlacklist  []string `yaml:"command_blacklist"`
        RateLimit         RLConfig `yaml:"rate_limit"`
        RetentionDays     int      `yaml:"retention_days"`

        BaseDir string `yaml:"base_dir"`
        TmuxBin string `yaml:"tmux_bin"`
}

type RLConfig struct {
        WindowSeconds int `yaml:"window_seconds"`
        MaxRequests   int `yaml:"max_requests"`
}

type JobRequest struct {
        Command       string            `json:"command"`
        RunAsUser     string            `json:"run_as_user"`
        JobID         string            `json:"job_id"`
        DedupKey      string            `json:"dedup_key"`
        SkipIfRunning bool              `json:"skip_if_running"`
        FireAndForget bool              `json:"fire_and_forget"`
        Env           map[string]string `json:"env"`
        UseTmux       bool              `json:"use_tmux"`
}

var (
        configFile = "/opt/airflow_agent/config.xml"
        baseDir    = "/opt/airflow_agent"
        jobDir     = "/opt/airflow_agent/jobs"
        tmuxBin    = "/usr/bin/tmux"

        config     Config
        configLock sync.RWMutex

        DefaultAgentToken = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

        rateBucket = make(map[string][]int64)
        rateLock   sync.Mutex
)

func setupFileLogging() {
        logPath := filepath.Join(baseDir, "agent.log")
        f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
        if err == nil {
                log.SetOutput(f)
                log.Println("[AGENT] logging to", logPath)
        } else {
                log.Println("[AGENT] failed to open log file:", err)
        }
}

func loadConfig() error {
        data, err := os.ReadFile(configFile)
        if err != nil {
                return err
        }

        var cfg Config
        if err := yaml.Unmarshal(data, &cfg); err != nil {
                return err
        }

        if strings.TrimSpace(cfg.Token) == "" {
                cfg.Token = DefaultAgentToken
                log.Println("[AGENT] WARNING: token missing in config.xml → using fallback token")
        }
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
        if cfg.RetentionDays == 0 {
                cfg.RetentionDays = 60
        }

        if strings.TrimSpace(cfg.BaseDir) != "" {
                baseDir = cfg.BaseDir
                jobDir = filepath.Join(baseDir, "jobs")
        }
        if strings.TrimSpace(cfg.TmuxBin) != "" {
                tmuxBin = cfg.TmuxBin
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
                                _ = loadConfig()
                        }
                }
                time.Sleep(10 * time.Second)
        }
}

func validateToken(r *http.Request) error {
        configLock.RLock()
        expected := config.Token
        configLock.RUnlock()
        if r.Header.Get("X-Agent-Token") != expected {
                return errors.New("invalid token")
        }
        return nil
}

func clientIP(r *http.Request) string {
        host, _, err := net.SplitHostPort(r.RemoteAddr)
        if err == nil {
                return host
        }
        parts := strings.Split(r.RemoteAddr, ":")
        return parts[0]
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

func checkRateLimit(ip string) bool {
// Rate limiting disabled
    return true
}

func jobPath(jobID string) string { return filepath.Join(jobDir, jobID) }

func writeFile(path, content string) {
        _ = os.MkdirAll(filepath.Dir(path), 0755)
        _ = os.WriteFile(path, []byte(content), 0644)
}

func readFile(path string) string {
        b, err := os.ReadFile(path)
        if err != nil {
                return ""
        }
        return string(b)
}

func locksDir() string {
        p := filepath.Join(baseDir, "locks")
        _ = os.MkdirAll(p, 0755)
        return p
}

func sha1Hex(s string) string {
        sum := sha1.Sum([]byte(s))
        return fmt.Sprintf("%x", sum)
}

func dedupLockPath(dedupKey string) string {
        return filepath.Join(locksDir(), sha1Hex(dedupKey)+".lock")
}

func readLock(path string) string {
        b, err := os.ReadFile(path)
        if err != nil {
                return ""
        }
        return strings.TrimSpace(string(b))
}

func writeLock(path, jobID string) error {
        return os.WriteFile(path, []byte(jobID), 0644)
}

func removeLock(path string) { _ = os.Remove(path) }

func tmuxSession(jobID string) string {
        j := strings.ReplaceAll(jobID, "-", "")
        if len(j) > 60 {
                j = j[:60]
        }
        return "agent_" + j
}

func tmuxAlive(jobID string) bool {
        session := tmuxSession(jobID)
        cmd := exec.Command(tmuxBin, "has-session", "-t", session)
        return cmd.Run() == nil
}

func buildScript(jobID string, req JobRequest) (string, error) {
        jp := jobPath(jobID)
        if err := os.MkdirAll(jp, 0755); err != nil {
                return "", err
        }

        stdout := filepath.Join(jp, "stdout.log")
        stderr := filepath.Join(jp, "stderr.log")
        exitFile := filepath.Join(jp, "exit")
        statusFile := filepath.Join(jp, "status")
        scriptFile := filepath.Join(jp, "run.sh")

        writeFile(statusFile, "starting")

        // store lock path for cancel/release
        if strings.TrimSpace(req.DedupKey) != "" {
                lp := dedupLockPath(req.DedupKey)
                writeFile(filepath.Join(jp, "dedup_lock"), lp)
        }

        cmd := req.Command
        if req.RunAsUser != "" {
                safe := strings.ReplaceAll(cmd, "'", "'\"'\"'")
                cmd = "su - " + req.RunAsUser + " -c '" + safe + "'"
        }

        content := "#!/bin/bash\n" +
                "set -o pipefail\n" +
                cmd + " >> \"" + stdout + "\" 2>> \"" + stderr + "\"\n" +
                "RC=$?\n" +
                "echo $RC > \"" + exitFile + "\"\n" +
                "echo finished > \"" + statusFile + "\"\n"

        if err := os.WriteFile(scriptFile, []byte(content), 0755); err != nil {
                return "", err
        }
        return scriptFile, nil
}

func releaseLockWhenFinished(jobID string) {
        exitFile := filepath.Join(jobPath(jobID), "exit")
        for {
                if _, err := os.Stat(exitFile); err == nil {
                        lp := strings.TrimSpace(readFile(filepath.Join(jobPath(jobID), "dedup_lock")))
                        if lp != "" {
                                removeLock(lp)
                        }
                        return
                }
                time.Sleep(2 * time.Second)
        }
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

        writeFile(filepath.Join(jobPath(jobID), "status"), "running")

        // release lock when exit file appears
        go releaseLockWhenFinished(jobID)
        return nil
}

func cancelJob(jobID string) {
        session := tmuxSession(jobID)
        _ = exec.Command(tmuxBin, "kill-session", "-t", session).Run()
        writeFile(filepath.Join(jobPath(jobID), "status"), "cancelled")

        lp := strings.TrimSpace(readFile(filepath.Join(jobPath(jobID), "dedup_lock")))
        if lp != "" {
                removeLock(lp)
        }
}

func cleanupLoop() {
        for {
                configLock.RLock()
                ret := config.RetentionDays
                configLock.RUnlock()
                if ret <= 0 {
                        ret = 60
                }
                cutoff := time.Now().Add(-time.Duration(ret) * 24 * time.Hour)

                entries, _ := os.ReadDir(jobDir)
                for _, e := range entries {
                        if !e.IsDir() {
                                continue
                        }
                        jid := e.Name()
                        jp := jobPath(jid)

                        st := strings.TrimSpace(readFile(filepath.Join(jp, "status")))
                        if st == "running" && tmuxAlive(jid) {
                                continue
                        }

                        info, err := os.Stat(jp)
                        if err != nil {
                                continue
                        }
                        if info.ModTime().After(cutoff) {
                                continue
                        }

                        _ = os.RemoveAll(jp)
                }
                time.Sleep(1 * time.Hour)
        }
}

func writeJSON(w http.ResponseWriter, obj interface{}) {
        w.Header().Set("Content-Type", "application/json")
        b, _ := json.Marshal(obj)
        _, _ = w.Write(b)
}

func logsHandler(w http.ResponseWriter, r *http.Request) {
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, 403)
                return
        }
        jobID := strings.TrimPrefix(r.URL.Path, "/logs/")
        stream := r.URL.Query().Get("stream")
        if stream != "stdout" && stream != "stderr" {
                http.Error(w, `{"error":"invalid_stream"}`, 400)
                return
        }
        offsetStr := r.URL.Query().Get("offset")
        var offset int64 = 0
        if offsetStr != "" {
                if v, err := strconv.ParseInt(offsetStr, 10, 64); err == nil && v >= 0 {
                        offset = v
                }
        }

        logFile := filepath.Join(jobPath(jobID), stream+".log")
        f, err := os.Open(logFile)
        if err != nil {
                writeJSON(w, map[string]interface{}{"job_id": jobID, "stream": stream, "offset": offset, "next": offset, "data": "", "eof": true, "not_found": true})
                return
        }
        defer f.Close()

        fi, err := f.Stat()
        if err != nil {
                http.Error(w, `{"error":"stat_failed"}`, 500)
                return
        }
        size := fi.Size()
        if offset > size {
                offset = size
        }
        _, _ = f.Seek(offset, 0)

        const maxRead = 64 * 1024
        buf := make([]byte, maxRead)
        n, _ := f.Read(buf)

        next := offset + int64(n)
        eof := next >= size

        data := ""
        if n > 0 {
                data = string(buf[:n])
        }
        writeJSON(w, map[string]interface{}{"job_id": jobID, "stream": stream, "offset": offset, "next": next, "data": data, "eof": eof})
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
        ip := clientIP(r)
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, 403)
                return
        }
        if !ipAllowed(ip) {
                http.Error(w, `{"error":"ip_not_allowed"}`, 403)
                return
        }
        if !checkRateLimit(ip) {
                http.Error(w, `{"error":"rate_limited"}`, 429)
                return
        }
        writeJSON(w, map[string]string{"status": "ok", "time": time.Now().Format(time.RFC3339)})
}

func runHandler(w http.ResponseWriter, r *http.Request) {
        ip := clientIP(r)

        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, 403)
                return
        }
        if !ipAllowed(ip) {
                http.Error(w, `{"error":"ip_not_allowed"}`, 403)
                return
        }
        if !checkRateLimit(ip) {
                http.Error(w, `{"error":"rate_limited"}`, 429)
                return
        }

        body, _ := io.ReadAll(r.Body)
        var req JobRequest
        _ = json.Unmarshal(body, &req)

        if req.JobID == "" {
                http.Error(w, `{"error":"missing_job_id"}`, 400)
                return
        }
        if blockedCommand(req.Command) {
                http.Error(w, `{"error":"command_blocked"}`, 400)
                return
        }

        // cross-run dedup queueing
        if strings.TrimSpace(req.DedupKey) != "" {
                lp := dedupLockPath(req.DedupKey)
                active := readLock(lp)
                if active != "" && tmuxAlive(active) {
                        writeJSON(w, map[string]string{"job_id": req.JobID, "status": "already_running", "active_job_id": active})
                        return
                }
                removeLock(lp)
                if err := writeLock(lp, req.JobID); err != nil {
                        http.Error(w, `{"error":"lock_failed"}`, 500)
                        return
                }
        }

        if req.SkipIfRunning && tmuxAlive(req.JobID) {
                writeJSON(w, map[string]string{"job_id": req.JobID, "status": "already_running", "active_job_id": req.JobID})
                return
        }

        if err := launchTmux(req.JobID, req); err != nil {
                http.Error(w, `{"error":"failed_to_launch"}`, 500)
                return
        }
        writeJSON(w, map[string]string{"job_id": req.JobID, "status": "submitted"})
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, 403)
                return
        }

        jobID := strings.TrimPrefix(r.URL.Path, "/status/")
        jp := jobPath(jobID)

        st := strings.TrimSpace(readFile(filepath.Join(jp, "status")))
        ex := strings.TrimSpace(readFile(filepath.Join(jp, "exit")))

        var rcPtr *int
        if ex != "" {
                if n, err := strconv.Atoi(ex); err == nil {
                        rcPtr = &n
                }
        }

        if st == "" {
                if tmuxAlive(jobID) {
                        st = "running"
                } else {
                        st = "unknown"
                }
        }

        writeJSON(w, map[string]interface{}{"job_id": jobID, "status": st, "return_code": rcPtr})
}

func cancelHandler(w http.ResponseWriter, r *http.Request) {
        if err := validateToken(r); err != nil {
                http.Error(w, `{"error":"invalid_token"}`, 403)
                return
        }
        jobID := strings.TrimPrefix(r.URL.Path, "/cancel/")
        cancelJob(jobID)
        writeJSON(w, map[string]string{"job_id": jobID, "status": "cancelled"})
}

func runAgentServer() {
        _ = os.MkdirAll(jobDir, 0755)
        _ = loadConfig()
        go watchConfig()
        go cleanupLoop()

        http.HandleFunc("/ping", pingHandler)
        http.HandleFunc("/run", runHandler)
        http.HandleFunc("/status/", statusHandler)
        http.HandleFunc("/cancel/", cancelHandler)
        http.HandleFunc("/logs/", logsHandler)

        configLock.RLock()
        addr := fmt.Sprintf("%s:%d", config.Listen.Host, config.Listen.Port)
        cert := strings.TrimSpace(config.TLS.Cert)
        key := strings.TrimSpace(config.TLS.Key)
        configLock.RUnlock()

        if cert == "" || key == "" {
                log.Printf("[AGENT] TLS disabled (no cert/key). Serving HTTP on %s\n", addr)
                log.Fatal(http.ListenAndServe(addr, nil))
        } else {
                log.Printf("[AGENT] TLS enabled. Serving HTTPS on %s\n", addr)
                log.Fatal(http.ListenAndServeTLS(addr, cert, key, nil))
        }
}

func main() {
        cfgPath := flag.String("config", "/opt/airflow_agent/config.xml", "Path to config.xml")
        flag.Parse()

        configFile = *cfgPath
        baseDir = filepath.Dir(configFile)
        jobDir = filepath.Join(baseDir, "jobs")

        setupFileLogging()
        runAgentServer()
}
