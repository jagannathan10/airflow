// =========================================================================
//        Universal Airflow Remote Agent (TMUX-only) - Go Version
// =========================================================================
//  Features:
//    ✔ config.xml with auto reload every 30s (YAML)
//    ✔ TMUX long-running jobs
//    ✔ skip_if_running + dedup
//    ✔ CIDR-based IP allow-list
//    ✔ Token authentication (with fallback default token)
//    ✔ Command blacklist
//    ✔ Rate limiting (per-IP sliding window)
//    ✔ Run as root → su - <user>
//    ✔ Status & cancel API
//    ✔ Retention cleanup (60 days)
//    ✔ Parallel-safe (multiple task_ids)
//
//  Endpoints:
//    POST /run
//    GET  /status/<job_id>
//    POST /cancel/<job_id>
//    POST /ping
//
//  Build:
//      go build -o airflow-agent agent.go
//
//  Run:
//      ./airflow-agent
//
//  Systemd example provided separately.
// =========================================================================

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/fs"
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
)

// ============================================================================
// GLOBAL CONSTANTS
// ============================================================================
const (
	BaseDir        = "/opt/airflow_agent"
	ConfigFile     = BaseDir + "/config.xml"
	JobDir         = BaseDir + "/jobs"
	TmuxBin        = "/usr/bin/tmux"
	FallbackToken  = "fallback-token-change-this"
	ConfigReloadMs = 30000 // 30s
)

// ============================================================================
// CONFIG STRUCT
// ============================================================================
type Config struct {
	Listen struct {
		Host string `yaml:"host"`
		Port int    `yaml:"port"`
	} `yaml:"listen"`

	TLS struct {
		ServerCert string `yaml:"server_cert"`
		ServerKey  string `yaml:"server_key"`
	} `yaml:"tls"`

	Token string `yaml:"token"`

	AllowedIPs       []string `yaml:"allowed_ips"`
	CommandBlacklist []string `yaml:"command_blacklist"`

	RateLimit struct {
		WindowSeconds int `yaml:"window_seconds"`
		MaxRequests   int `yaml:"max_requests"`
	} `yaml:"rate_limit"`

	RetentionDays int `yaml:"retention_days"`
}

var (
	config     Config
	configLock sync.RWMutex
	rateBucket = make(map[string][]int64)
)

// ============================================================================
// CONFIG LOADING + AUTO RELOAD
// ============================================================================
func LoadConfig() {
	configLock.Lock()
	defer configLock.Unlock()

	data, err := ioutil.ReadFile(ConfigFile)
	if err != nil {
		log.Printf("[AGENT] Failed to read config.xml: %v", err)
		return
	}

	var newCfg Config
	if err := yaml.Unmarshal(data, &newCfg); err != nil {
		log.Printf("[AGENT] YAML parse error: %v", err)
		return
	}

	config = newCfg
	log.Println("[AGENT] config.xml loaded")
}

func AutoReloadConfig() {
	var lastModTime int64 = 0

	for {
		info, err := os.Stat(ConfigFile)
		if err == nil {
			mtime := info.ModTime().Unix()
			if mtime != lastModTime {
				lastModTime = mtime
				LoadConfig()
			}
		}
		time.Sleep(ConfigReloadMs * time.Millisecond)
	}
}

// ============================================================================
// UTIL HELPERS
// ============================================================================
func GetToken() string {
	configLock.RLock()
	defer configLock.RUnlock()

	if strings.TrimSpace(config.Token) != "" {
		return strings.TrimSpace(config.Token)
	}
	return FallbackToken
}

func IPAllowed(ip string) bool {
	configLock.RLock()
	defer configLock.RUnlock()

	if len(config.AllowedIPs) == 0 {
		return true
	}

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false
	}

	for _, cidr := range config.AllowedIPs {
		_, netw, err := net.ParseCIDR(cidr)
		if err == nil && netw.Contains(parsedIP) {
			return true
		}
	}

	return false
}

func CheckBlacklist(cmd string) bool {
	configLock.RLock()
	defer configLock.RUnlock()

	for _, bad := range config.CommandBlacklist {
		if strings.Contains(cmd, bad) {
			return false
		}
	}
	return true
}

func CheckRateLimit(ip string) bool {
	configLock.Lock()
	defer configLock.Unlock()

	now := time.Now().Unix()
	window := int64(config.RateLimit.WindowSeconds)
	maxReq := config.RateLimit.MaxRequests

	bucket := rateBucket[ip]
	newBucket := []int64{}
	for _, t := range bucket {
		if now-t <= window {
			newBucket = append(newBucket, t)
		}
	}
	rateBucket[ip] = newBucket

	if len(newBucket) >= maxReq {
		return false
	}

	rateBucket[ip] = append(rateBucket[ip], now)
	return true
}

// ============================================================================
// JOB HELPERS
// ============================================================================
func JobPath(jobID string) string {
	return filepath.Join(JobDir, jobID)
}

func WriteFile(path string, content string) {
	os.MkdirAll(filepath.Dir(path), 0755)
	ioutil.WriteFile(path, []byte(content), 0644)
}

func ReadFile(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(data)
}

func TmuxSession(jobID string) string {
	return "agent_" + jobID
}

func TmuxAlive(jobID string) bool {
	cmd := exec.Command(TmuxBin, "has-session", "-t", TmuxSession(jobID))
	err := cmd.Run()
	return err == nil
}

// ============================================================================
// RUN-AS-USER TMUX JOB
// ============================================================================
func BuildScript(jobID string, command string, runUser string) string {
	jobDir := JobPath(jobID)
	os.MkdirAll(jobDir, 0755)

	script := filepath.Join(jobDir, "run.sh")
	stdout := filepath.Join(jobDir, "stdout.log")
	stderr := filepath.Join(jobDir, "stderr.log")
	exitFile := filepath.Join(jobDir, "exit")
	statusFile := filepath.Join(jobDir, "status")

	WriteFile(statusFile, "starting")

	var runCmd string
	if runUser != "" {
		escaped := strings.ReplaceAll(command, "'", "'\"'\"'")
		runCmd = fmt.Sprintf("su - %s -c '%s'", runUser, escaped)
	} else {
		runCmd = command
	}

	content :=
		"#!/bin/bash\n" +
			"set -o pipefail\n" +
			fmt.Sprintf("%s >> \"%s\" 2>> \"%s\"\n", runCmd, stdout, stderr) +
			fmt.Sprintf("echo $? > \"%s\"\n", exitFile) +
			fmt.Sprintf("echo finished > \"%s\"\n", statusFile)

	WriteFile(script, content)
	os.Chmod(script, 0755)
	return script
}

func LaunchTmux(jobID, command, runUser string) {
	script := BuildScript(jobID, command, runUser)

	cmd := exec.Command(TmuxBin, "new-session", "-d",
		"-s", TmuxSession(jobID),
		"bash", "-lc", script,
	)
	cmd.Run()

	WriteFile(filepath.Join(JobPath(jobID), "status"), "running")
}

// ============================================================================
// API HANDLERS
// ============================================================================
type JobRequest struct {
	Command       string            `json:"command"`
	RunAsUser     string            `json:"run_as_user"`
	JobID         string            `json:"job_id"`
	SkipIfRunning bool              `json:"skip_if_running"`
	FireAndForget bool              `json:"fire_and_forget"`
	Env           map[string]string `json:"env"`
}

func HandlePing(w http.ResponseWriter, r *http.Request) {
	if !CheckRequestAuth(w, r) {
		return
	}
	RespondJSON(w, map[string]interface{}{
		"status": "ok",
		"time":   time.Now().UTC().String(),
	})
}

func HandleRun(w http.ResponseWriter, r *http.Request) {
	if !CheckRequestAuth(w, r) {
		return
	}

	clientIP := GetClientIP(r)
	if !IPAllowed(clientIP) {
		RespondError(w, 403, "ip_not_allowed")
		return
	}
	if !CheckRateLimit(clientIP) {
		RespondError(w, 429, "rate_limited")
		return
	}

	var req JobRequest
	json.NewDecoder(r.Body).Decode(&req)

	if !CheckBlacklist(req.Command) {
		RespondError(w, 400, "command_blocked")
		return
	}

	if req.SkipIfRunning && TmuxAlive(req.JobID) {
		RespondJSON(w, map[string]interface{}{
			"job_id": req.JobID,
			"status": "already_running",
		})
		return
	}

	LaunchTmux(req.JobID, req.Command, req.RunAsUser)

	RespondJSON(w, map[string]interface{}{
		"job_id": req.JobID,
		"status": "submitted",
	})
}

func HandleStatus(w http.ResponseWriter, r *http.Request) {
	if !CheckRequestAuth(w, r) {
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	jobID := parts[len(parts)-1]

	jobDir := JobPath(jobID)
	status := ReadFile(filepath.Join(jobDir, "status"))
	exitCodeStr := ReadFile(filepath.Join(jobDir, "exit"))
	stdout := ReadFile(filepath.Join(jobDir, "stdout.log"))
	stderr := ReadFile(filepath.Join(jobDir, "stderr.log"))

	var exitCode *int
	if strings.TrimSpace(exitCodeStr) != "" {
		i, err := strconv.Atoi(strings.TrimSpace(exitCodeStr))
		if err == nil {
			exitCode = &i
		}
	}

	RespondJSON(w, map[string]interface{}{
		"job_id":      jobID,
		"status":      strings.TrimSpace(status),
		"return_code": exitCode,
		"stdout":      stdout,
		"stderr":      stderr,
	})
}

func HandleCancel(w http.ResponseWriter, r *http.Request) {
	if !CheckRequestAuth(w, r) {
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	jobID := parts[len(parts)-1]

	exec.Command(TmuxBin, "kill-session", "-t", TmuxSession(jobID)).Run()
	WriteFile(filepath.Join(JobPath(jobID), "status"), "cancelled")

	RespondJSON(w, map[string]interface{}{
		"job_id": jobID,
		"status": "cancelled",
	})
}

// ============================================================================
// AUTH + UTILS
// ============================================================================
func CheckRequestAuth(w http.ResponseWriter, r *http.Request) bool {
	expected := GetToken()
	provided := r.Header.Get("X-Agent-Token")

	if provided != expected {
		RespondError(w, 403, "invalid_token")
		return false
	}
	return true
}

func GetClientIP(r *http.Request) string {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

func RespondJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	out, _ := json.Marshal(obj)
	w.WriteHeader(200)
	w.Write(out)
}

func RespondError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	out := fmt.Sprintf(`{"error":"%s"}`, msg)
	w.Write([]byte(out))
}

// ============================================================================
// RETENTION CLEANUP
// ============================================================================
func CleanupLoop() {
	for {
		configLock.RLock()
		days := config.RetentionDays
		configLock.RUnlock()

		if days <= 0 {
			days = 60
		}

		cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour)

		filepath.Walk(JobDir, func(path string, info fs.FileInfo, err error) error {
			if err != nil {
				return nil
			}

			if info.IsDir() && path != JobDir {
				if info.ModTime().Before(cutoff) {
					jobID := filepath.Base(path)
					exec.Command(TmuxBin, "kill-session", "-t", TmuxSession(jobID)).Run()
					os.RemoveAll(path)
					log.Printf("[CLEANUP] Removed %s", jobID)
				}
			}
			return nil
		})

		time.Sleep(1 * time.Hour)
	}
}

// ============================================================================
// MAIN ENTRY
// ============================================================================
func main() {
	// Ensure job directory exists
	os.MkdirAll(JobDir, 0755)

	// Load config initially
	LoadConfig()

	// Start reloader
	go AutoReloadConfig()

	// Start cleanup loop
	go CleanupLoop()

	http.HandleFunc("/ping", HandlePing)
	http.HandleFunc("/run", HandleRun)
	http.HandleFunc("/status/", HandleStatus)
	http.HandleFunc("/cancel/", HandleCancel)

	configLock.RLock()
	addr := fmt.Sprintf("%s:%d", config.Listen.Host, config.Listen.Port)
	cert := config.TLS.ServerCert
	key := config.TLS.ServerKey
	configLock.RUnlock()

	log.Printf("[AGENT] Starting on %s", addr)
	log.Fatal(http.ListenAndServeTLS(addr, cert, key, nil))
}
