// airflow_agent_windows.go
// Universal Airflow Remote Agent (Windows) - process-based "tmux equivalent"
// BaseDir: C:\airflow_agent
//
// Endpoints:
//   POST /ping
//   POST /run
//   GET  /status/{job_id}
//   POST /cancel/{job_id}
//
// Config (YAML) file path default: C:\airflow_agent\config.xml

package main

import (
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
	"syscall"
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

	Token string `yaml:"token"`

	AllowedIPs       []string `yaml:"allowed_ips"`
	CommandBlacklist []string `yaml:"command_blacklist"`
	RateLimit        RLConfig  `yaml:"rate_limit"`
	RetentionDays    int       `yaml:"retention_days"`
}

type RLConfig struct {
	WindowSeconds int `yaml:"window_seconds"`
	MaxRequests   int `yaml:"max_requests"`
}

type JobRequest struct {
	Command       string            `json:"command"`
	RunAsUser     string            `json:"run_as_user"` // NOT USED on Windows (see notes)
	JobID         string            `json:"job_id"`
	SkipIfRunning bool              `json:"skip_if_running"`
	FireAndForget bool              `json:"fire_and_forget"`
	Env           map[string]string `json:"env"`
	UseTmux       bool              `json:"use_tmux"` // ignored on Windows; always process-based
}

var (
	// Windows defaults
	baseDir = `C:\airflow_agent`
	jobDir  = `C:\airflow_agent\jobs`

	configFile = `C:\airflow_agent\config.xml`

	config     Config
	configLock sync.RWMutex

	DefaultAgentToken = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

	rateBucket = make(map[string][]int64) // ip → timestamps
	rateLock   sync.Mutex
)

// ---------------- CONFIG ----------------

func loadConfig() error {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}

	// defaults
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

// ---------------- SECURITY ----------------

func validateToken(r *http.Request) error {
	configLock.RLock()
	expected := config.Token
	configLock.RUnlock()

	provided := r.Header.Get("X-Agent-Token")
	if provided != expected {
		return errors.New("invalid token")
	}
	return nil
}

func clientIP(r *http.Request) string {
	// r.RemoteAddr looks like "IP:port"
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	// fallback
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

// ---------------- JOB TRACKING ----------------

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

func pidFile(jobID string) string    { return filepath.Join(jobPath(jobID), "pid") }
func statusFile(jobID string) string { return filepath.Join(jobPath(jobID), "status") }
func exitFile(jobID string) string   { return filepath.Join(jobPath(jobID), "exit") }

func isProcessRunning(pid int) bool {
	// On Windows: "tasklist /FI PID eq <pid>" and check output
	out, err := exec.Command("cmd.exe", "/C", fmt.Sprintf("tasklist /FI \"PID eq %d\"", pid)).CombinedOutput()
	if err != nil {
		return false
	}
	s := string(out)
	// If PID not found, tasklist prints "No tasks are running..."
	if strings.Contains(strings.ToLower(s), "no tasks are running") {
		return false
	}
	// Otherwise it lists the process
	return strings.Contains(s, strconv.Itoa(pid))
}

func jobRunning(jobID string) bool {
	p := strings.TrimSpace(readFile(pidFile(jobID)))
	if p == "" {
		return false
	}
	pid, err := strconv.Atoi(p)
	if err != nil {
		return false
	}
	return isProcessRunning(pid)
}

// Launch command as detached process:
// cmd.exe /C <command> with stdout/stderr redirected to files.
func launchJob(jobID string, req JobRequest) error {
	jp := jobPath(jobID)
	if err := os.MkdirAll(jp, 0755); err != nil {
		return err
	}

	outFile := filepath.Join(jp, "stdout.log")
	errFile := filepath.Join(jp, "stderr.log")

	writeFile(statusFile(jobID), "running")

	// Prepare environment
	env := os.Environ()
	for k, v := range req.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	// Use cmd.exe to interpret "sh ..." or ".bat" etc.
	// If your command is PowerShell-based, pass: powershell -NoProfile -ExecutionPolicy Bypass -File ...
	cmd := exec.Command("cmd.exe", "/C", req.Command)
	cmd.Env = env

	// Detach from parent (service)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
	}

	// Redirect logs
	stdout, err := os.OpenFile(outFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	stderr, err := os.OpenFile(errFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		_ = stdout.Close()
		return err
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return err
	}

	// Save PID
	writeFile(pidFile(jobID), fmt.Sprintf("%d", cmd.Process.Pid))

	// Wait in background to write exit status
	go func() {
		err := cmd.Wait()
		_ = stdout.Close()
		_ = stderr.Close()

		rc := 0
		if err != nil {
			// Try to get exit code
			if ee, ok := err.(*exec.ExitError); ok {
				rc = ee.ExitCode()
			} else {
				rc = 1
			}
		}
		writeFile(exitFile(jobID), fmt.Sprintf("%d", rc))
		writeFile(statusFile(jobID), "finished")
	}()

	return nil
}

func cancelJob(jobID string) {
	// Kill process tree with taskkill /T /F
	p := strings.TrimSpace(readFile(pidFile(jobID)))
	if p != "" {
		_ = exec.Command("cmd.exe", "/C", fmt.Sprintf("taskkill /PID %s /T /F", p)).Run()
	}
	writeFile(statusFile(jobID), "cancelled")
}

// ---------------- RETENTION CLEANUP ----------------

func cleanupLoop() {
	for {
		configLock.RLock()
		retention := config.RetentionDays
		configLock.RUnlock()

		cutoff := time.Now().Add(-time.Duration(retention) * 24 * time.Hour)

		entries, _ := os.ReadDir(jobDir)
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			jid := e.Name()
			jp := jobPath(jid)
			info, err := os.Stat(jp)
			if err != nil {
				continue
			}
			if info.ModTime().After(cutoff) {
				continue
			}

			st := strings.TrimSpace(readFile(statusFile(jid)))
			if st == "running" && jobRunning(jid) {
				continue
			}

			_ = os.RemoveAll(jp)
			log.Printf("[CLEANUP] Removed %s\n", jid)
		}

		time.Sleep(1 * time.Hour)
	}
}

// ---------------- HTTP ----------------

func writeJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(obj)
	_, _ = w.Write(b)
}

func pingHandler(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)

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

	writeJSON(w, map[string]string{"status": "ok", "time": time.Now().Format(time.RFC3339)})
}

func runHandler(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)

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

	body, _ := io.ReadAll(r.Body)
	var req JobRequest
	_ = json.Unmarshal(body, &req)

	if req.JobID == "" {
		http.Error(w, `{"error":"missing_job_id"}`, http.StatusBadRequest)
		return
	}
	if blockedCommand(req.Command) {
		http.Error(w, `{"error":"command_blocked"}`, http.StatusBadRequest)
		return
	}

	// Dedup: if running, don't start again
	if req.SkipIfRunning && jobRunning(req.JobID) {
		writeJSON(w, map[string]string{"job_id": req.JobID, "status": "already_running"})
		return
	}

	if err := launchJob(req.JobID, req); err != nil {
		log.Printf("[AGENT] ERROR launchJob: %v\n", err)
		http.Error(w, `{"error":"failed_to_launch"}`, http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{"job_id": req.JobID, "status": "submitted"})
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	if err := validateToken(r); err != nil {
		http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
		return
	}

	jobID := strings.TrimPrefix(r.URL.Path, "/status/")
	jp := jobPath(jobID)

	st := strings.TrimSpace(readFile(filepath.Join(jp, "status")))
	ex := strings.TrimSpace(readFile(filepath.Join(jp, "exit")))
	out := readFile(filepath.Join(jp, "stdout.log"))
	er := readFile(filepath.Join(jp, "stderr.log"))

	var rcPtr *int
	if ex != "" {
		if n, err := strconv.Atoi(ex); err == nil {
			rcPtr = &n
		}
	}

	if st == "" {
		// If job folder exists but status missing, infer:
		if jobRunning(jobID) {
			st = "running"
		} else {
			st = "unknown"
		}
	}

	writeJSON(w, map[string]interface{}{
		"job_id":      jobID,
		"status":      st,
		"return_code": rcPtr,
		"stdout":      out,
		"stderr":      er,
	})
}

func cancelHandler(w http.ResponseWriter, r *http.Request) {
	if err := validateToken(r); err != nil {
		http.Error(w, `{"error":"invalid_token"}`, http.StatusForbidden)
		return
	}
	jobID := strings.TrimPrefix(r.URL.Path, "/cancel/")
	cancelJob(jobID)
	writeJSON(w, map[string]string{"job_id": jobID, "status": "cancelled"})
}

func main() {
	cfgPath := flag.String("config", `C:\airflow_agent\config.xml`, "Path to config.xml")
	flag.Parse()

	configFile = *cfgPath
	baseDir = filepath.Dir(configFile)
	jobDir = filepath.Join(baseDir, "jobs")

	_ = os.MkdirAll(jobDir, 0755)

	if err := loadConfig(); err != nil {
		log.Printf("[AGENT] WARNING: could not load config.xml (%v). Using defaults.\n", err)
		// still allow run with defaults; fallback token is always enforced via validateToken()
		// but token is in config; if config failed, it stays empty → we must inject fallback:
		configLock.Lock()
		config.Token = DefaultAgentToken
		config.Listen.Host = "0.0.0.0"
		config.Listen.Port = 18443
		config.RateLimit.WindowSeconds = 60
		config.RateLimit.MaxRequests = 120
		config.RetentionDays = 60
		configLock.Unlock()
	}
	go watchConfig()
	go cleanupLoop()

	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/run", runHandler)
	http.HandleFunc("/status/", statusHandler)
	http.HandleFunc("/cancel/", cancelHandler)

	configLock.RLock()
	host := config.Listen.Host
	port := config.Listen.Port
	cert := strings.TrimSpace(config.TLS.Cert)
	key := strings.TrimSpace(config.TLS.Key)
	configLock.RUnlock()

	addr := fmt.Sprintf("%s:%d", host, port)

	if cert == "" || key == "" {
		log.Printf("[AGENT] TLS disabled (no cert/key). Serving HTTP on %s\n", addr)
		log.Fatal(http.ListenAndServe(addr, nil))
	} else {
		log.Printf("[AGENT] TLS enabled. Serving HTTPS on %s\n", addr)
		log.Printf("[AGENT] cert=%s key=%s\n", cert, key)
		log.Fatal(http.ListenAndServeTLS(addr, cert, key, nil))
	}
}
