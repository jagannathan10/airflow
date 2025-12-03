package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
)

const (
	agentPort = ":18443"

	// 🔐 Embedded Token
	agentToken = "scb-airflowagent-cf08bbd8a13a2d8ed0f1fbe915e29c7c0108a0862da8e24a2372f8e4fb6b83d2"

	// TLS certificate paths
	certFile = "C:/airflow_agent/certs/cert.pem"
	keyFile  = "C:/airflow_agent/certs/key.pem"
)

// Job Info
type JobStatus struct {
	ID        string
	Running   bool
	ExitCode  int
	LogPath   string
}

var (
	jobs   = make(map[string]*JobStatus)
	jobsMu sync.Mutex
)

func validateToken(r *http.Request) bool {
	token := r.Header.Get("X-Agent-Token")
	return token == agentToken
}

func writeJSON(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(obj)
}

// ----------------------------------
//  RUN JOB (Task Scheduler)
// ----------------------------------
func runHandler(w http.ResponseWriter, r *http.Request) {
	if !validateToken(r) {
		http.Error(w, "Unauthorized", 401)
		return
	}

	type Req struct {
		ID      string `json:"id"`
		Command string `json:"command"`
	}

	body, _ := ioutil.ReadAll(r.Body)
	var req Req
	json.Unmarshal(body, &req)

	if req.ID == "" || req.Command == "" {
		http.Error(w, "Missing parameters", 400)
		return
	}

	jobsMu.Lock()
	defer jobsMu.Unlock()

	// Prevent Duplicate Run
	if job, exists := jobs[req.ID]; exists && job.Running {
		writeJSON(w, map[string]string{"status": "already_running"})
		return
	}

	logDir := "C:/airflow_agent/logs"
	os.MkdirAll(logDir, 0755)
	logFile := filepath.Join(logDir, req.ID+".log")
	exitFile := filepath.Join(logDir, req.ID+".exit")

	// Clean old exit file
	os.Remove(exitFile)

	taskCmd := fmt.Sprintf(`
$ErrorActionPreference='Continue'
try {
	%s *>&1 | Tee-Object -FilePath "%s"
	$code = if ($LASTEXITCODE) { $LASTEXITCODE } elseif ($?) { 0 } else { 1 }
} catch {
	$_ | Out-String | Add-Content "%s"
	$code = 1
}
Set-Content "%s" $code
exit $code
`, req.Command, logFile, logFile, exitFile)

	taskPath := filepath.Join(logDir, req.ID+".ps1")
	ioutil.WriteFile(taskPath, []byte(taskCmd), 0644)

	// Register & Run Task
	createCmd := exec.Command("schtasks",
		"/Create", "/TN", req.ID,
		"/TR", fmt.Sprintf("powershell.exe -ExecutionPolicy Bypass -File \"%s\"", taskPath),
		"/SC", "ONCE",
		"/ST", "00:00",
		"/F",
		"/RU", "SYSTEM")

	if err := createCmd.Run(); err != nil {
		http.Error(w, fmt.Sprintf("Create task error: %v", err), 500)
		return
	}

	runCmd := exec.Command("schtasks", "/Run", "/TN", req.ID)
	runCmd.Run()

	jobs[req.ID] = &JobStatus{
		ID:      req.ID,
		Running: true,
		LogPath: logFile,
	}

	writeJSON(w, map[string]string{"status": "started"})
}

// ----------------------------------
//  CHECK STATUS
// ----------------------------------
func statusHandler(w http.ResponseWriter, r *http.Request) {
	if !validateToken(r) {
		http.Error(w, "Unauthorized", 401)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing id", 400)
		return
	}

	jobsMu.Lock()
	job, exists := jobs[id]
	jobsMu.Unlock()

	if !exists {
		writeJSON(w, map[string]string{"status": "not_found"})
		return
	}

	// Check if task is still running
	query := exec.Command("schtasks", "/Query", "/TN", id, "/FO", "LIST")
	out, err := query.CombinedOutput()

	if err != nil || !contains(string(out), "Running") {
		// Task finished
		exitFile := job.LogPath[:len(job.LogPath)-4] + ".exit"
		data, err := ioutil.ReadFile(exitFile)

		if err == nil {
			job.ExitCode = int(data[0] - '0')
		} else {
			job.ExitCode = 1
		}

		job.Running = false
		writeJSON(w, map[string]interface{}{
			"finished":  true,
			"exit_code": job.ExitCode,
		})
		return
	}

	writeJSON(w, map[string]interface{}{
		"finished": false,
	})
}

// ----------------------------------
//  GET LOG
// ----------------------------------
func logsHandler(w http.ResponseWriter, r *http.Request) {
	if !validateToken(r) {
		http.Error(w, "Unauthorized", 401)
		return
	}

	id := r.URL.Query().Get("id")

	jobsMu.Lock()
	job, exists := jobs[id]
	jobsMu.Unlock()

	if !exists {
		http.Error(w, "Not found", 404)
		return
	}

	data, _ := ioutil.ReadFile(job.LogPath)
	w.Header().Set("Content-Type", "text/plain")
	w.Write(data)
}

// ----------------------------------
//  CLEANUP
// ----------------------------------
func cleanupHandler(w http.ResponseWriter, r *http.Request) {
	if !validateToken(r) {
		http.Error(w, "Unauthorized", 401)
		return
	}

	type Req struct{ ID string `json:"id"` }

	body, _ := ioutil.ReadAll(r.Body)
	var req Req
	json.Unmarshal(body, &req)

	if req.ID == "" {
		http.Error(w, "Missing id", 400)
		return
	}

	logDir := "C:/airflow_agent/logs"
	os.Remove(filepath.Join(logDir, req.ID+".log"))
	os.Remove(filepath.Join(logDir, req.ID+".exit"))
	os.Remove(filepath.Join(logDir, req.ID+".ps1"))

	exec.Command("schtasks", "/Delete", "/TN", req.ID, "/F").Run()

	jobsMu.Lock()
	delete(jobs, req.ID)
	jobsMu.Unlock()

	writeJSON(w, map[string]string{"status": "cleaned"})
}

// ----------------------------------
//  TLS + STRONG CIPHERS
// ----------------------------------
func contains(big, small string) bool {
	return len(big) >= len(small) && filepath.Base(big) != big || (len(big) >= len(small) && big == small) || (len(big) >= len(small) && len(big) >= len(small) && string(big) == string(small)) || (len(big) > len(small) && string(big[:len(small)]) == string(small))
}

func main() {

	mux := http.NewServeMux()
	mux.HandleFunc("/run", runHandler)
	mux.HandleFunc("/status", statusHandler)
	mux.HandleFunc("/logs", logsHandler)
	mux.HandleFunc("/cleanup", cleanupHandler)

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
		PreferServerCipherSuites: true,
	}

	server := &http.Server{
		Addr:      agentPort,
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	log.Println("Windows Agent running on port 18443 with TLS")
	log.Fatal(server.ListenAndServeTLS(certFile, keyFile))
}
