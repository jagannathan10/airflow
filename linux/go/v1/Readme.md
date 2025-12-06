Nice, we’re at the packaging + hardening stage now 😎
Here’s a complete bundle:

airflow-agent.spec (RPM spec for RHEL 8/9)

airflow-agent.service (systemd)

airflow-agent.te (SELinux policy skeleton)

logrotate config for /opt/airflow_agent/jobs

All aligned with your Go agent and config.xml.

1) airflow-agent.spec — RPM spec (RHEL8/9)

Assumptions:

You’ll place a tarball airflow-agent-1.0.0.tar.gz in ~/rpmbuild/SOURCES/

Tarball root contains: agent.go, config.xml.example, systemd/airflow-agent.service, selinux/airflow-agent.te, logrotate/airflow-agent

Binary will be installed as /usr/sbin/airflow-agent

Runtime dir: /opt/airflow_agent

Name:           airflow-agent
Version:        1.0.0
Release:        1%{?dist}
Summary:        Universal Airflow Remote TMUX Agent

License:        MIT
URL:            https://example.internal/airflow-agent
Source0:        %{name}-%{version}.tar.gz

BuildArch:      x86_64

BuildRequires:  golang
BuildRequires:  systemd
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
# SELinux tooling (optional but recommended)
Requires(post): policycoreutils-python-utils, policycoreutils, selinux-policy-targeted

%description
Universal Airflow Remote Agent implemented in Go.

Features:
 - TMUX-only remote job execution for Airflow
 - YAML config.xml with hot reload
 - CIDR-based IP allow-list
 - Token authentication (with built-in fallback)
 - Command blacklist and rate limiting
 - Job status and cancel API
 - Retention cleanup of job folders

%prep
%autosetup -n %{name}-%{version}

%build
# Build static-ish Go binary
export GOPATH=%{_builddir}/gopath
mkdir -p "$GOPATH"
go build -o airflow-agent ./agent.go

%install
rm -rf %{buildroot}

# Install binary
install -D -m 0755 airflow-agent %{buildroot}%{_sbindir}/airflow-agent

# Runtime directory
install -d -m 0750 %{buildroot}/opt/airflow_agent
install -d -m 0750 %{buildroot}/opt/airflow_agent/jobs
install -d -m 0700 %{buildroot}/opt/airflow_agent/certs

# Default config.xml example (noreplace)
install -D -m 0640 config.xml.example %{buildroot}/opt/airflow_agent/config.xml

# Systemd unit
install -D -m 0644 systemd/airflow-agent.service \
    %{buildroot}%{_unitdir}/airflow-agent.service

# Logrotate config
install -D -m 0644 logrotate/airflow-agent \
    %{buildroot}%{_sysconfdir}/logrotate.d/airflow-agent

# SELinux policy source
install -D -m 0644 selinux/airflow-agent.te \
    %{buildroot}/usr/share/selinux/packages/airflow-agent.te

%post
# ----- Systemd -----
%systemd_post airflow-agent.service

# ----- SELinux policy module (optional) -----
    if [ -x /usr/sbin/selinuxenabled ] && selinuxenabled; then
        if [ -x /usr/bin/checkmodule ] && [ -x /usr/bin/semodule_package ]; then
            TMPDIR=$(mktemp -d /tmp/airflow-agent-selinux.XXXXXX)
            cp /usr/share/selinux/packages/airflow-agent.te "$TMPDIR"/
            pushd "$TMPDIR" >/dev/null 2>&1
            checkmodule -M -m -o airflow-agent.mod airflow-agent.te 2>/dev/null || :
            semodule_package -o airflow-agent.pp -m airflow-agent.mod 2>/dev/null || :
            semodule -i airflow-agent.pp 2>/dev/null || :
            popd >/dev/null 2>&1
            rm -rf "$TMPDIR"
        fi

    # Label /opt/airflow_agent as airflow_agent_data_t
    if command -v semanage >/dev/null 2>&1; then
        semanage fcontext -a -t airflow_agent_data_t "/opt/airflow_agent(/.*)?"
        restorecon -Rv /opt/airflow_agent >/dev/null 2>&1 || :
    fi

    # Allow port 18443 as http_port_t (or change type if desired)
    if command -v semanage >/dev/null 2>&1; then
        semanage port -a -t http_port_t -p tcp 18443 2>/dev/null || \
        semanage port -m -t http_port_t -p tcp 18443 2>/dev/null || :
    fi
fi

%preun
%systemd_preun airflow-agent.service

%postun
%systemd_postun_with_restart airflow-agent.service

%files
%license LICENSE
%doc README.md

%{_sbindir}/airflow-agent

%dir /opt/airflow_agent
%dir /opt/airflow_agent/jobs
%dir /opt/airflow_agent/certs
%config(noreplace) /opt/airflow_agent/config.xml

%{_unitdir}/airflow-agent.service
%config(noreplace) %{_sysconfdir}/logrotate.d/airflow-agent

/usr/share/selinux/packages/airflow-agent.te

%changelog
* Thu Dec 05 2025 You <you@example.com> 1.0.0-1
- Initial RPM for Universal Airflow Agent


Note:
Adjust LICENSE, README.md, and Source0 as per your repo layout.

2) systemd unit — systemd/airflow-agent.service

This assumes:

SELinux/ports: 18443 allowed as http_port_t

TLS certs from config.xml (Go agent uses them, not systemd)

[Unit]
Description=Universal Airflow Remote Go Agent (TMUX)
After=network.target
Wants=network-online.target

[Service]
User=root
Group=root
WorkingDirectory=/opt/airflow_agent

Environment="PYTHONUNBUFFERED=1"
Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"

ExecStart=/usr/sbin/airflow-agent

Restart=always
RestartSec=5
StartLimitIntervalSec=60
StartLimitBurst=5

# TMUX and su need full privileges
NoNewPrivileges=no
PrivateTmp=no

[Install]
WantedBy=multi-user.target

3) SELinux policy — selinux/airflow-agent.te

This is a starting policy to:

Define airflow_agent_t domain and airflow_agent_exec_t type

Treat /usr/sbin/airflow-agent as its executable

Allow network + TMUX execution

Label /opt/airflow_agent as airflow_agent_data_t

You’ll still likely refine this with audit2allow in your environment.

module airflow-agent 1.0;

require {
    type unreserved_port_t;
    type tmp_t;
    type var_t;
    type var_log_t;
    type bin_t;
    type usr_t;
    type init_t;
    type tmux_t;
    type tmux_exec_t;
    class capability { dac_override dac_read_search net_bind_service };
    class tcp_socket { name_bind name_connect };
    class process { transition sigchld signal };
    class file { read write getattr execute execute_no_trans open create append ioctl lock getattr };
    class dir { create read write remove_name add_name search getattr };
}

########################################
# Types
########################################

type airflow_agent_t;
type airflow_agent_exec_t;
type airflow_agent_data_t;

# Exec type for agent binary
files_type(airflow_agent_exec_t)
# Data type for /opt/airflow_agent
files_type(airflow_agent_data_t)

########################################
# Domain transition
########################################

# Treat airflow_agent_exec_t as a daemon
init_daemon_domain(airflow_agent_t, airflow_agent_exec_t)

########################################
# Basic permissions for data directory
########################################

allow airflow_agent_t airflow_agent_data_t:dir { create read write remove_name add_name search getattr };
allow airflow_agent_t airflow_agent_data_t:file { create read write append getattr open };

########################################
# Network: bind to high port (18443)
########################################

allow airflow_agent_t self:tcp_socket { name_bind name_connect };
allow airflow_agent_t unreserved_port_t:tcp_socket name_bind;

########################################
# Capabilities
########################################

allow airflow_agent_t self:capability { dac_override dac_read_search net_bind_service };

########################################
# Execute tmux
########################################

allow airflow_agent_t tmux_exec_t:file { read getattr execute open };
allow airflow_agent_t tmux_t:process { transition sigchld signal };


The RPM %post already compiles & installs this as airflow-agent.pp if SELinux is enabled and tools exist.

You’ll also need to label the binary:

# Example (also doable in %post or deploy script)
semanage fcontext -a -t airflow_agent_exec_t "/usr/sbin/airflow-agent"
restorecon -v /usr/sbin/airflow-agent

semanage fcontext -a -t airflow_agent_data_t "/opt/airflow_agent(/.*)?"
restorecon -Rv /opt/airflow_agent


(Part of this is already in %post, but you can refine it.)

4) Logrotate config — logrotate/airflow-agent

You said job folders may accumulate; this keeps stdout/stderr logs tidy while preserving job folders and exit codes.

/opt/airflow_agent/jobs/*/stdout.log /opt/airflow_agent/jobs/*/stderr.log {
    daily
    rotate 30
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
    sharedscripts
    su root root
}


If you ever introduce central logs (e.g. /opt/airflow_agent/agent.log), just add them here.

5) Example config.xml.example

Tie it back to what your Go agent expects:

# ======================================================================
#  Universal Airflow Remote Agent - Configuration File
# ======================================================================

listen:
  host: "0.0.0.0"
  port: 18443

tls:
  server_cert: "/opt/airflow_agent/certs/cert.pem"
  server_key: "/opt/airflow_agent/certs/key.pem"

# If empty/missing, agent uses built-in fallback token
token: "scb-airflowagent-CHANGE-ME-64-CHAR-HEX"

allowed_ips:
  - "10.0.0.0/8"
  - "192.168.120.137/32"

command_blacklist:
  - "shutdown"
  - "reboot"
  - "rm -rf /"
  - "mkfs "
  - ":(){ :|:& };:"

rate_limit:
  window_seconds: 60
  max_requests: 120

retention_days: 60

6) Build & Install Flow (quick recap)

On your RHEL8/9 build host:

# 1) Prepare SOURCES
mkdir -p ~/rpmbuild/{SOURCES,SPECS}
tar czf ~/rpmbuild/SOURCES/airflow-agent-1.0.0.tar.gz \
    agent.go config.xml.example \
    systemd/airflow-agent.service \
    selinux/airflow-agent.te \
    logrotate/airflow-agent \
    README.md LICENSE

# 2) Place spec
cp airflow-agent.spec ~/rpmbuild/SPECS/

# 3) Build RPM
rpmbuild -ba ~/rpmbuild/SPECS/airflow-agent.spec

# 4) Install on target
sudo rpm -Uvh ~/rpmbuild/RPMS/x86_64/airflow-agent-1.0.0-1*.rpm

# 5) Adjust config.xml (token, allowed_ips, TLS paths)
sudo vi /opt/airflow_agent/config.xml

# 6) (Optional) SELinux context checks and ports (if not already)
sudo restorecon -Rv /usr/sbin/airflow-agent /opt/airflow_agent
sudo semanage port -a -t http_port_t -p tcp 18443 || \
     sudo semanage port -m -t http_port_t -p tcp 18443

# 7) Enable & start
sudo systemctl daemon-reload
sudo systemctl enable --now airflow-agent.service
sudo systemctl status airflow-agent.service


If you want, next I can:

Generate a small Go CLI (cm-agentctl) to do:

/ping, /status/<job>, /cancel/<job>

/run for manual tests

Or extend the SELinux policy based on audit logs you paste (from ausearch -m avc / audit2allow).
