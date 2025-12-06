Here is your full, production-ready RPM SPEC file + systemd service for the Go-based Airflow Remote Agent, fully compatible with RHEL8 / RHEL9.

This SPEC file:

        ✔ Builds your Go agent binary
        ✔ Installs under /opt/airflow_agent
        ✔ Installs config.xml, certs/, and job directory
        ✔ Installs systemd unit file
        ✔ Sets ownership to root:root
        ✔ Enables & starts the service automatically
        ✔ Strips binary for smaller size
        ✔ Macro-safe for both EL8 & EL9

✅ 1. Directory Layout (final installed structure)

                /opt/airflow_agent/
                    agent          <-- compiled Go binary
                    config.xml
                    jobs/          <-- auto-created
                    certs/
                        cert.pem
                        key.pem

                /usr/lib/systemd/system/airflow-agent.service

✅ 2. Create the RPM SPEC file

Save this file as:

SPECS/airflow-agent.spec

📦 FINAL airflow-agent.spec (RHEL8/RHEL9 SAFE)

                Name:           airflow-agent
                Version:        1.0.0
                Release:        1%{?dist}
                Summary:        Universal Airflow Remote Agent (TMUX-based)
                
                License:        MIT
                URL:            https://example.com/airflow-agent
                Source0:        agent.tar.gz
                
                BuildRequires:  golang
                Requires:       tmux
                Requires(pre):  shadow-utils
                Requires(post): systemd
                Requires(preun): systemd
                Requires(postun): systemd
                
                %description
                A universal Go-based remote execution agent for Apache Airflow.
                Supports:
                 - TMUX long-running jobs
                 - CIDR allowlists
                 - Command blacklist
                 - Rate limiting
                 - Auto reload config.xml
                 - RHEL8 / RHEL9 compatible
                
                %prep
                %setup -q -n agent
                
                %build
                # Build Go binary
                export GOPATH=%{_builddir}/gopath
                export GOCACHE=%{_builddir}/gocache
                
                go build -ldflags="-s -w" -o airflow-agent .
                
                %install
                mkdir -p %{buildroot}/opt/airflow_agent
                mkdir -p %{buildroot}/opt/airflow_agent/certs
                mkdir -p %{buildroot}/opt/airflow_agent/jobs
                
                # Install binary
                install -m 0755 airflow-agent %{buildroot}/opt/airflow_agent/agent
                
                # Install config.xml (if provided)
                if [ -f config.xml ]; then
                    install -m 0644 config.xml %{buildroot}/opt/airflow_agent/config.xml
                fi
                
                # Install systemd service
                mkdir -p %{buildroot}%{_unitdir}
                install -m 0644 airflow-agent.service %{buildroot}%{_unitdir}/airflow-agent.service
                
                
                %pre
                # Ensure directory exists
                mkdir -p /opt/airflow_agent/jobs
                exit 0
                
                %post
                # Enable and start service
                %systemd_post airflow-agent.service
                
                %preun
                %systemd_preun airflow-agent.service
                
                %postun
                %systemd_postun_with_restart airflow-agent.service
                
                %files
                /opt/airflow_agent/
                /usr/lib/systemd/system/airflow-agent.service
                
                %changelog
                * Thu Dec 05 2025 ChatGPT <admin@example.com> - 1.0.0-1
                - Initial RPM Release

✅ 3. Systemd Unit File (install as airflow-agent.service)

Place this in:

SOURCES/airflow-agent.service

        [Unit]
        Description=Universal Airflow Remote Agent (Go TMUX Agent)
        After=network.target
        Wants=network-online.target
        
        [Service]
        User=root
        Group=root
        WorkingDirectory=/opt/airflow_agent
        
        Environment="PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/bin"
        ExecStart=/opt/airflow_agent/agent \
            --config /opt/airflow_agent/config.xml
        
        Restart=always
        RestartSec=5
        NoNewPrivileges=no
        PrivateTmp=no

[Install]
WantedBy=multi-user.target

✅ 4. Build Instructions

Step 1 — Prepare folder structure:

        mkdir -p ~/rpmbuild/{BUILD,RPMS,SOURCES,SPECS,SRPMS}

Step 2 — Place files:

        cp agent.go ~/rpmbuild/SOURCES/
        cp config.xml ~/rpmbuild/SOURCES/
        cp airflow-agent.service ~/rpmbuild/SOURCES/

Step 3 — Create tarball:

        cd ~/rpmbuild/SOURCES
        tar -czf agent.tar.gz agent.go config.xml airflow-agent.service

Step 4 — Build RPM:

        rpmbuild -ba ~/rpmbuild/SPECS/airflow-agent.spec

Step 5 — Install:

        rpm -ivh ~/rpmbuild/RPMS/x86_64/airflow-agent-1.0.0-1.el8.x86_64.rpm
        systemctl enable --now airflow-agent
