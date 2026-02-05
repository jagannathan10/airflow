#!/bin/bash
# fileName: setup_airflow_podman_3_1_6.sh
# Author: Airflow Engineering
# 
# History:
# --------
# Date		 		   Who		     			        Version				      Notes
# 02/Feb/2026          Jagannathan V                      1.0                     Podman-based Airflow setup with SELinux
# ------------------------------------------------------------------------------------------------------------------------

#set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
AIRFLOW_VERSION="3.1.6"
AIRFLOW_USER="airflow"
AIRFLOW_HOME="/apps/airflow"
CONTAINER_STORAGE="/apps/container"
AIRFLOW_NETWORK="airflow-pasta-net"
AIRFLOW_USER_HOME="/home/airflow"
AIRFLOW_UID="50000"
AIRFLOW_GID="50000"
# Custom XDG_RUNTIME_DIR location (default is /run/user/UID)
# Change this to your preferred location
CUSTOM_XDG_RUNTIME_DIR="/apps/airflow/runtime"
USE_CUSTOM_RUNTIME_DIR="true"  # Set to "false" to use default /run/user/50000

# Custom Artifactory URL for pip
ARTIFACTORY_URL="https://artifactory.global.standardchartered.com/artifactory/api/pypi/pypi/simple"

# Custom registry images
AIRFLOW_IMAGE="artifactory.global.standardchartered.com/apache/airflow:3.1.7"
POSTGRES_IMAGE="artifactory.global.standardchartered.com/gv-images-products/oss/postgres:18"
REDIS_IMAGE="artifactory.global.standardchartered.com/redis:6.2.6"

# Alternative images (fallback)
ALT_AIRFLOW_IMAGE="apache/airflow:3.1.7"
ALT_POSTGRES_IMAGE="postgres:18"
ALT_REDIS_IMAGE="redis:6.2.6"

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Detect OS
detect_os() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        OS_VERSION=$VERSION_ID
        log_info "Detected OS: $OS $OS_VERSION"
    else
        log_error "Cannot detect OS"
        exit 1
    fi
}

# Check sudo privileges
check_sudo() {
    if [ "$EUID" -ne 0 ]; then 
        log_error "Please run as root or with sudo"
        exit 1
    fi
    log_info "✓ Running with sudo privileges"
}

# Create /apps base directory if it doesn't exist
create_base_directory() {
    log_info "Checking /apps base directory..."
    
    if [ ! -d "/apps" ]; then
        log_info "Creating /apps directory..."
        mkdir -p /apps
        
        if [ $? -ne 0 ]; then
            log_error "Failed to create /apps directory"
            exit 1
        fi
        
        log_info "✓ Created /apps directory"
    else
        log_info "✓ /apps directory already exists"
    fi
    
    # Set initial permissions on /apps (root owned, world readable)
    chown root:root /apps
    chmod 755 /apps
    
    log_info "✓ Set permissions on /apps (755, root:root)"
}


# Install SELinux packages
install_selinux_packages() {
    log_info "Installing SELinux management packages..."
    
    if [[ "$OS" == "rhel" ]]; then
        if [[ "$OS_VERSION" == "8"* ]]; then
            log_info "Installing SELinux packages for RHEL 8..."
            yum install -y \
                policycoreutils \
                policycoreutils-python-utils \
                selinux-policy \
                selinux-policy-targeted \
                libselinux-utils \
                container-selinux \
                libselinux-python3 \
                python3-policycoreutils 2>&1 | tee -a /tmp/selinux_install.log || \
                log_warn "Some SELinux packages failed to install, check /tmp/selinux_install.log"
                
        elif [[ "$OS_VERSION" == "9"* ]]; then
            log_info "Installing SELinux packages for RHEL 9..."
            dnf install -y \
                policycoreutils \
                policycoreutils-python-utils \
                selinux-policy \
                selinux-policy-targeted \
                libselinux-utils \
                container-selinux \
                python3-libselinux \
                python3-policycoreutils 2>&1 | tee -a /tmp/selinux_install.log || \
                log_warn "Some SELinux packages failed to install, check /tmp/selinux_install.log"
        fi
    fi
    
    # Verify semanage is available
    if command -v semanage &> /dev/null; then
        log_info "✓ SELinux management tools installed successfully"
    else
        log_warn "semanage not available, will use fallback methods"
        return 1
    fi
}

# Check and configure SELinux
configure_selinux() {
    log_info "Configuring SELinux for Podman containers..."
    
    # Check if SELinux is enabled
    if ! command -v getenforce &> /dev/null; then
        log_warn "SELinux tools not found, skipping SELinux configuration"
        return 0
    fi
    
    SELINUX_STATUS=$(getenforce)
    log_info "SELinux status: $SELINUX_STATUS"
    
    if [ "$SELINUX_STATUS" != "Enforcing" ] && [ "$SELINUX_STATUS" != "Permissive" ]; then
        log_info "SELinux is disabled, skipping SELinux configuration"
        return 0
    fi
    
    log_info "SELinux is enabled, configuring container contexts..."
    
    # Install SELinux packages
    install_selinux_packages || {
        log_warn "SELinux package installation failed, using fallback methods"
        
        # Fallback: Use chcon only
        log_info "Using chcon for temporary context setting..."
        chcon -R -t container_file_t "$AIRFLOW_HOME" 2>/dev/null || true
        chcon -R -t container_file_t "$CONTAINER_STORAGE" 2>/dev/null || true
        
        log_warn "SELinux contexts are temporary. Install policycoreutils-python-utils for permanent contexts"
        return 0
    }
    
    # Now use semanage (should be available)
    log_info "Setting persistent SELinux contexts with semanage..."
    
    # Set SELinux file contexts
    semanage fcontext -a -t container_file_t "$AIRFLOW_HOME(/.*)?" 2>/dev/null || \
        semanage fcontext -m -t container_file_t "$AIRFLOW_HOME(/.*)?" 2>/dev/null || \
        log_warn "Failed to set context for $AIRFLOW_HOME"
    
    semanage fcontext -a -t container_file_t "$CONTAINER_STORAGE(/.*)?" 2>/dev/null || \
        semanage fcontext -m -t container_file_t "$CONTAINER_STORAGE(/.*)?" 2>/dev/null || \
        log_warn "Failed to set context for $CONTAINER_STORAGE"
    
    # Apply the contexts
    log_info "Applying SELinux contexts..."
    restorecon -Rv "$AIRFLOW_HOME" 2>/dev/null || log_warn "Failed to restore context for $AIRFLOW_HOME"
    restorecon -Rv "$CONTAINER_STORAGE" 2>/dev/null || log_warn "Failed to restore context for $CONTAINER_STORAGE"
    
    # Set SELinux booleans
    log_info "Setting SELinux booleans for containers..."
    setsebool -P container_manage_cgroup on 2>/dev/null || log_warn "Failed to set container_manage_cgroup"
    setsebool -P virt_sandbox_use_all_caps on 2>/dev/null || true
    setsebool -P virt_use_nfs on 2>/dev/null || true
    
    log_info "✓ SELinux configured successfully"
}

# Install Podman
install_podman() {
    log_info "Installing Podman and dependencies..."
    
    if command -v podman &> /dev/null; then
        log_info "Podman already installed: $(podman --version)"
        return 0
    fi
    
    if [[ "$OS" == "rhel" ]] && [[ "$OS_VERSION" == "8"* ]]; then
        yum install -y podman podman-docker container-selinux slirp4netns fuse-overlayfs
    elif [[ "$OS" == "rhel" ]] && [[ "$OS_VERSION" == "9"* ]]; then
        dnf install -y podman podman-docker container-selinux slirp4netns fuse-overlayfs
    else
        log_error "Unsupported OS for Podman installation"
        exit 1
    fi
    
    log_info "✓ Podman installed: $(podman --version)"
}

# Install OCI Runtime (crun)
install_oci_runtime() {
    log_info "Installing OCI runtime (crun)..."
    
    if command -v crun &> /dev/null; then
        log_info "crun already installed: $(crun --version | head -1)"
        return 0
    fi
    
    if [[ "$OS" == "rhel" ]]; then
        if [[ "$OS_VERSION" == "8"* ]]; then
            yum install -y crun runc
        elif [[ "$OS_VERSION" == "9"* ]]; then
            dnf install -y crun runc
        fi
    else
        log_error "Unsupported OS for crun installation"
        exit 1
    fi
    
    # Verify installation
    if command -v crun &> /dev/null; then
        log_info "✓ crun installed: $(crun --version | head -1)"
        log_info "✓ runc installed: $(runc --version | head -1)"
    else
        log_error "Failed to install crun"
        exit 1
    fi
}

# Create airflow user early in the process
create_airflow_user() {
    log_info "Creating airflow user with UID 50000 and GID 50000..."
    
    # Check if user already exists
    if id "$AIRFLOW_USER" &>/dev/null; then
        EXISTING_UID=$(id -u "$AIRFLOW_USER")
        EXISTING_GID=$(id -g "$AIRFLOW_USER")
		EXISTING_HOME=$(getent passwd "$AIRFLOW_USER" | cut -d: -f6)
        
        log_info "User $AIRFLOW_USER already exists with UID: $EXISTING_UID, GID: $EXISTING_GID"
        
        # Check if UID/GID match
        if [ "$EXISTING_UID" = "$AIRFLOW_UID" ] && [ "$EXISTING_GID" = "$AIRFLOW_GID" ]; then
            log_info "✓ User $AIRFLOW_USER already has correct UID ($AIRFLOW_UID) and GID ($AIRFLOW_GID)"
            # Check if home directory exists
            if [ ! -d "$EXISTING_HOME" ]; then
                log_warn "Home directory $EXISTING_HOME does not exist"
                log_info "Creating home directory..."
                mkdir -p "$EXISTING_HOME"
                cp -r /etc/skel/. "$EXISTING_HOME/"
                chown -R "$AIRFLOW_UID:$AIRFLOW_GID" "$EXISTING_HOME"
                chmod 700 "$EXISTING_HOME"
                log_info "✓ Created home directory: $EXISTING_HOME"
            fi
            
            return 0
        fi
        
        # UID/GID mismatch - need to fix
        log_warn "User $AIRFLOW_USER has incorrect UID/GID"
        log_warn "Current: UID=$EXISTING_UID, GID=$EXISTING_GID"
        log_warn "Expected: UID=$AIRFLOW_UID, GID=$AIRFLOW_GID"
        
        read -p "Do you want to change the UID/GID? This may take time. (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_error "Cannot proceed without correct UID/GID"
            exit 1
        fi
        
        # Change GID first
        log_info "Changing GID to $AIRFLOW_GID..."
        groupmod -g "$AIRFLOW_GID" "$AIRFLOW_USER" 2>&1 || {
            log_error "Failed to change GID"
            exit 1
        }
        
        # Change UID
        log_info "Changing UID to $AIRFLOW_UID..."
        usermod -u "$AIRFLOW_UID" "$AIRFLOW_USER" 2>&1 || {
            log_error "Failed to change UID"
            exit 1
        }
        
        # Fix ownership of existing files (this can take time)
        log_info "Updating file ownership (this may take several minutes)..."
        find /home/$AIRFLOW_USER -user $EXISTING_UID -exec chown $AIRFLOW_UID {} \; 2>/dev/null || true
        find /home/$AIRFLOW_USER -group $EXISTING_GID -exec chgrp $AIRFLOW_GID {} \; 2>/dev/null || true
        
        if [ -d "$AIRFLOW_HOME" ]; then
            find $AIRFLOW_HOME -user $EXISTING_UID -exec chown $AIRFLOW_UID {} \; 2>/dev/null || true
            find $AIRFLOW_HOME -group $EXISTING_GID -exec chgrp $AIRFLOW_GID {} \; 2>/dev/null || true
        fi
        
        if [ -d "$CONTAINER_STORAGE" ]; then
            find $CONTAINER_STORAGE -user $EXISTING_UID -exec chown $AIRFLOW_UID {} \; 2>/dev/null || true
            find $CONTAINER_STORAGE -group $EXISTING_GID -exec chgrp $AIRFLOW_GID {} \; 2>/dev/null || true
        fi
        
        log_info "✓ Changed user $AIRFLOW_USER to UID: $AIRFLOW_UID, GID: $AIRFLOW_GID"
        return 0
    fi
    
    # User doesn't exist - create new
    log_info "Creating new user $AIRFLOW_USER..."
    
    # Check if GID 50000 is already in use
    if getent group $AIRFLOW_GID &>/dev/null; then
        EXISTING_GROUP=$(getent group $AIRFLOW_GID | cut -d: -f1)
        log_warn "GID $AIRFLOW_GID is already used by group: $EXISTING_GROUP"
        
        if [ "$EXISTING_GROUP" != "$AIRFLOW_USER" ]; then
            log_info "Creating user and adding to existing group $EXISTING_GROUP"
            # Create user with existing group
            useradd -m -u "$AIRFLOW_UID" -g "$AIRFLOW_GID" -s /bin/bash -c "Airflow Service Account" "$AIRFLOW_USER" 2>&1 || {
                log_error "Failed to create user with existing group"
                exit 1
            }
        fi
    else
        # Create group first
        log_info "Creating group $AIRFLOW_USER with GID $AIRFLOW_GID..."
        groupadd -g "$AIRFLOW_GID" "$AIRFLOW_USER" 2>&1 || {
            log_error "Failed to create group"
            exit 1
        }
        
        # Create user with the new group and home directory
        log_info "Creating user $AIRFLOW_USER with UID $AIRFLOW_UID and home directory..."
        useradd -m -u "$AIRFLOW_UID" -g "$AIRFLOW_GID" -s /bin/bash -c "Airflow Service Account" "$AIRFLOW_USER" 2>&1 || {
            log_error "Failed to create user"
            exit 1
        }
    fi
    
    # Verify creation
    CREATED_UID=$(id -u "$AIRFLOW_USER")
    CREATED_GID=$(id -g "$AIRFLOW_USER")
    CREATED_HOME=$(getent passwd "$AIRFLOW_USER" | cut -d: -f6)
    
    if [ "$CREATED_UID" != "$AIRFLOW_UID" ] || [ "$CREATED_GID" != "$AIRFLOW_GID" ]; then
        log_error "User created with wrong UID/GID"
        log_error "Expected: UID=$AIRFLOW_UID, GID=$AIRFLOW_GID"
        log_error "Got: UID=$CREATED_UID, GID=$CREATED_GID"
        exit 1
    fi
    
    # Verify home directory exists
    if [ ! -d "$CREATED_HOME" ]; then
        log_error "Home directory was not created: $CREATED_HOME"
        log_info "Creating home directory manually..."
        mkdir -p "$CREATED_HOME"
        cp -r /etc/skel/. "$CREATED_HOME/"
        chown -R "$AIRFLOW_UID:$AIRFLOW_GID" "$CREATED_HOME"
        chmod 700 "$CREATED_HOME"
    fi
    
    log_info "✓ Created user $AIRFLOW_USER with:"
    log_info "  UID: $AIRFLOW_UID"
    log_info "  GID: $AIRFLOW_GID"
    log_info "  Home: $CREATED_HOME"
    log_info "  Shell: /bin/bash"
    
    # Set password (optional)
    echo "$AIRFLOW_USER:airflow123" | chpasswd
    log_info "✓ Set password for $AIRFLOW_USER"
    
    # Set up basic environment in home directory
    log_info "Setting up user environment..."
    
    # Create .bashrc additions
    cat >> "$CREATED_HOME/.bashrc" << 'EOFBASH'

# Airflow environment
export AIRFLOW_HOME=/apps/airflow
export PATH=$PATH:$AIRFLOW_HOME/scripts
export XDG_RUNTIME_DIR=/run/user/$(id -u)

# Aliases
alias airflow-start='cd /apps/airflow/podman && podman-compose up -d'
alias airflow-stop='cd /apps/airflow/podman && podman-compose down'
alias airflow-restart='cd /apps/airflow/podman && podman-compose restart'
alias airflow-logs='cd /apps/airflow/podman && podman-compose logs -f'
alias airflow-status='cd /apps/airflow/podman && podman-compose ps'
EOFBASH
    
    chown "$AIRFLOW_UID:$AIRFLOW_GID" "$CREATED_HOME/.bashrc"
    
    # Create .bash_profile if it doesn't exist
    if [ ! -f "$CREATED_HOME/.bash_profile" ]; then
        cat > "$CREATED_HOME/.bash_profile" << 'EOFPROFILE'
# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
    . ~/.bashrc
fi

# User specific environment and startup programs
PATH=$PATH:$HOME/bin
export PATH
EOFPROFILE
        chown "$AIRFLOW_UID:$AIRFLOW_GID" "$CREATED_HOME/.bash_profile"
    fi
    
    # Create .ssh directory
    mkdir -p "$CREATED_HOME/.ssh"
    chmod 700 "$CREATED_HOME/.ssh"
    chown "$AIRFLOW_UID:$AIRFLOW_GID" "$CREATED_HOME/.ssh"
    
    # Create .config directory for Podman
    mkdir -p "$CREATED_HOME/.config/containers"
    chmod 755 "$CREATED_HOME/.config"
    chmod 755 "$CREATED_HOME/.config/containers"
    chown -R "$AIRFLOW_UID:$AIRFLOW_GID" "$CREATED_HOME/.config"
    
    log_info "✓ User environment configured"
    
    # Display user info
    log_info "User details:"
    id "$AIRFLOW_USER"
    log_info "Home directory contents:"
    ls -la "$CREATED_HOME" | head -10
	[ -d "/run/user/$AIRFLOW_UID" ] || { mkdir -p "/run/user/$AIRFLOW_UID" && chown "$AIRFLOW_UID:$AIRFLOW_GID" "/run/user/$AIRFLOW_UID" && chmod 700 "/run/user/$AIRFLOW_UID"; }
	
}


# Configure pip to use custom Artifactory
configure_pip() {
    log_info "Configuring pip to use Artifactory..."
    
    # Configure pip globally for root
    mkdir -p /root/.pip
    cat > /root/.pip/pip.conf << EOF
[global]
index-url = $ARTIFACTORY_URL
trusted-host = artifactory.global.standardchartered.com
timeout = 60
EOF
    
    log_info "✓ Pip configured for root user"
}

# Configure pip for airflow user
configure_pip_for_airflow_user() {
    log_info "Configuring pip for $AIRFLOW_USER..."
    
    # Configure pip for airflow user
    su - "$AIRFLOW_USER" -c "mkdir -p ~/.pip"
    su - "$AIRFLOW_USER" -c "cat > ~/.pip/pip.conf << 'PIPEOF'
[global]
index-url = $ARTIFACTORY_URL
trusted-host = artifactory.global.standardchartered.com
timeout = 60
PIPEOF
"
    
    # Verify pip configuration
    su - "$AIRFLOW_USER" -c "cat ~/.pip/pip.conf" || log_warn "Failed to verify pip config for $AIRFLOW_USER"
    
    log_info "✓ Pip configured for $AIRFLOW_USER"
}

# Install Podman Compose for airflow user only
install_podman_compose() {
    log_info "Installing Podman Compose for $AIRFLOW_USER..."
    
    # Ensure airflow user exists before installation
    if ! id "$AIRFLOW_USER" &>/dev/null; then
        log_error "Airflow user does not exist. Cannot install podman-compose."
        exit 1
    fi
    
    # Install Python pip for airflow user if not present
    if ! su - "$AIRFLOW_USER" -c "command -v pip3 &> /dev/null"; then
        log_info "Installing Python pip..."
        if [[ "$OS_VERSION" == "8"* ]]; then
            yum install -y python3-pip python3-setuptools
        else
            dnf install -y python3-pip python3-setuptools
        fi
    fi
    
    # Configure pip for airflow user
    configure_pip_for_airflow_user
    
    # Check if podman-compose is already installed for airflow user
    if su - "$AIRFLOW_USER" -c "command -v podman-compose &> /dev/null"; then
        INSTALLED_VERSION=$(su - "$AIRFLOW_USER" -c "podman-compose --version" 2>&1 || echo "unknown")
        log_info "Podman Compose already installed for $AIRFLOW_USER: $INSTALLED_VERSION"
        return 0
    fi
    
    # Upgrade pip for airflow user using Artifactory
    log_info "Upgrading pip for $AIRFLOW_USER using Artifactory..."
    su - "$AIRFLOW_USER" -c "pip3 install --index-url '$ARTIFACTORY_URL' --upgrade pip setuptools wheel" || {
        log_warn "Failed to upgrade pip from Artifactory, trying without index-url..."
        su - "$AIRFLOW_USER" -c "pip3 install --upgrade pip setuptools wheel" || log_error "Failed to upgrade pip"
    }
    
    # Install podman-compose for airflow user only
    log_info "Installing podman-compose for $AIRFLOW_USER using Artifactory..."
    su - "$AIRFLOW_USER" -c "pip3 install --index-url '$ARTIFACTORY_URL' --user podman-compose" || {
        log_warn "Failed to install from Artifactory, trying PyPI..."
        su - "$AIRFLOW_USER" -c "pip3 install --user podman-compose" || {
            log_error "Failed to install podman-compose"
            return 1
        }
    }
    
    # Ensure ~/.local/bin is in PATH
    if ! su - "$AIRFLOW_USER" -c "echo \$PATH | grep -q '.local/bin'"; then
        log_info "Adding ~/.local/bin to $AIRFLOW_USER PATH..."
        su - "$AIRFLOW_USER" -c "echo 'export PATH=\$HOME/.local/bin:\$PATH' >> ~/.bashrc"
        su - "$AIRFLOW_USER" -c "echo 'export PATH=\$HOME/.local/bin:\$PATH' >> ~/.bash_profile"
    fi
    
    # Verify installation
    if su - "$AIRFLOW_USER" -c "command -v podman-compose &> /dev/null"; then
        COMPOSE_VERSION=$(su - "$AIRFLOW_USER" -c "podman-compose --version")
        log_info "✓ Podman Compose installed for $AIRFLOW_USER: $COMPOSE_VERSION"
        log_info "  Location: $(su - $AIRFLOW_USER -c 'which podman-compose')"
    else
        log_error "Failed to install podman-compose for $AIRFLOW_USER"
        log_info "Checking installation location..."
        su - "$AIRFLOW_USER" -c "ls -la ~/.local/bin/ | grep podman" || echo "Not found in ~/.local/bin"
        return 1
    fi
}

# Verify podman-compose accessibility
verify_podman_compose() {
    log_info "Verifying podman-compose availability for $AIRFLOW_USER..."
    
    # Check if podman-compose exists for airflow user
    if ! su - "$AIRFLOW_USER" -c "command -v podman-compose &> /dev/null"; then
        log_error "podman-compose not found for $AIRFLOW_USER"
        log_info "Attempting to reinstall..."
        install_podman_compose || {
            log_error "Failed to install podman-compose"
            exit 1
        }
    fi
    
    # Verify it works
    if su - "$AIRFLOW_USER" -c "podman-compose --version" &>/dev/null; then
        log_info "✓ podman-compose is accessible and working for $AIRFLOW_USER"
    else
        log_error "podman-compose installed but not working for $AIRFLOW_USER"
        log_info "PATH: $(su - $AIRFLOW_USER -c 'echo $PATH')"
        log_info "podman-compose location: $(su - $AIRFLOW_USER -c 'which podman-compose' || echo 'not found')"
        exit 1
    fi
}

# Configure subuid and subgid for rootless containers
configure_subuid_subgid() {
    log_info "Configuring subuid and subgid for rootless containers..."
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    AIRFLOW_GID=$(id -g "$AIRFLOW_USER")
    
    log_info "Airflow user UID: $AIRFLOW_UID, GID: $AIRFLOW_GID"
    
    # Configure /etc/subuid - need enough UIDs for container users (increased range)
    if ! grep -q "^${AIRFLOW_USER}:" /etc/subuid; then
        # Start from UID 100000, allocate 1065536 UIDs to accommodate high UID containers
        echo "${AIRFLOW_USER}:100000:1065536" >> /etc/subuid
        log_info "✓ Added ${AIRFLOW_USER} to /etc/subuid with range 100000:1065536"
    else
        # Check if range is sufficient
        CURRENT_RANGE=$(grep "^${AIRFLOW_USER}:" /etc/subuid | cut -d: -f3)
        if [ "$CURRENT_RANGE" -lt 1065536 ]; then
            log_warn "Current subuid range ($CURRENT_RANGE) may be insufficient"
            log_warn "Updating to larger range..."
            sed -i "/^${AIRFLOW_USER}:/d" /etc/subuid
            echo "${AIRFLOW_USER}:100000:1065536" >> /etc/subuid
            log_info "✓ Updated ${AIRFLOW_USER} in /etc/subuid"
        else
            log_info "✓ ${AIRFLOW_USER} already in /etc/subuid with sufficient range"
        fi
    fi
    
    # Configure /etc/subgid - need enough GIDs for container groups
    if ! grep -q "^${AIRFLOW_USER}:" /etc/subgid; then
        echo "${AIRFLOW_USER}:100000:1065536" >> /etc/subgid
        log_info "✓ Added ${AIRFLOW_USER} to /etc/subgid with range 100000:1065536"
    else
        # Check if range is sufficient
        CURRENT_RANGE=$(grep "^${AIRFLOW_USER}:" /etc/subgid | cut -d: -f3)
        if [ "$CURRENT_RANGE" -lt 1065536 ]; then
            log_warn "Current subgid range ($CURRENT_RANGE) may be insufficient"
            log_warn "Updating to larger range..."
            sed -i "/^${AIRFLOW_USER}:/d" /etc/subgid
            echo "${AIRFLOW_USER}:100000:1065536" >> /etc/subgid
            log_info "✓ Updated ${AIRFLOW_USER} in /etc/subgid"
        else
            log_info "✓ ${AIRFLOW_USER} already in /etc/subgid with sufficient range"
        fi
    fi
    
    # Display current configuration
    log_info "Current subuid configuration:"
    grep "^${AIRFLOW_USER}:" /etc/subuid
    log_info "Current subgid configuration:"
    grep "^${AIRFLOW_USER}:" /etc/subgid
    
    log_info "✓ Subuid/subgid configured"
}

# Configure rootless Podman
configure_rootless_podman() {
    log_info "Configuring rootless Podman for $AIRFLOW_USER..."
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    
    # Create and configure XDG_RUNTIME_DIR
    log_info "Setting up XDG_RUNTIME_DIR..."
    mkdir -p "/run/user/$AIRFLOW_UID"
    chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "/run/user/$AIRFLOW_UID"
    chmod 700 "/run/user/$AIRFLOW_UID"
    
    # Make XDG_RUNTIME_DIR persistent (add to PAM if not present)
    if ! grep -q "pam_systemd.so" /etc/pam.d/system-auth; then
        echo "session optional pam_systemd.so" >> /etc/pam.d/system-auth
    fi
    
    # Add to user's environment
    su - "$AIRFLOW_USER" -c "cat >> ~/.bashrc << 'BASHEOF'

# Podman rootless configuration
export XDG_RUNTIME_DIR=/run/user/\$(id -u)
export DBUS_SESSION_BUS_ADDRESS=unix:path=\$XDG_RUNTIME_DIR/bus

# Add local bin to PATH
export PATH=\$HOME/.local/bin:\$PATH

# Pip configuration
export PIP_INDEX_URL='$ARTIFACTORY_URL'
export PIP_TRUSTED_HOST='artifactory.global.standardchartered.com'
BASHEOF
"
    
    su - "$AIRFLOW_USER" -c "cat >> ~/.bash_profile << 'PROFILEOF'

# Podman rootless configuration
export XDG_RUNTIME_DIR=/run/user/\$(id -u)
export DBUS_SESSION_BUS_ADDRESS=unix:path=\$XDG_RUNTIME_DIR/bus

# Add local bin to PATH
export PATH=\$HOME/.local/bin:\$PATH

# Pip configuration
export PIP_INDEX_URL='$ARTIFACTORY_URL'
export PIP_TRUSTED_HOST='artifactory.global.standardchartered.com'
PROFILEOF
"
    
    # Create containers configuration
    log_info "Creating containers configuration..."
    su - "$AIRFLOW_USER" -c "mkdir -p ~/.config/containers"
    
    cat > "/home/$AIRFLOW_USER/.config/containers/containers.conf" << 'EOF'
[containers]
default_capabilities = [
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "NET_BIND_SERVICE",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYS_CHROOT"
]
netns = "private"
userns = "auto"
utsns = "private"

[engine]
cgroup_manager = "systemd"
events_logger = "file"
runtime = "crun"
num_locks = 2048

[network]
network_backend = "netavark"

[secrets]
driver = "file"
EOF
    
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "/home/$AIRFLOW_USER/.config/containers/containers.conf"
    
    # Create storage configuration
    cat > "/home/$AIRFLOW_USER/.config/containers/storage.conf" << EOF
[storage]
driver = "overlay"
runroot = "$CONTAINER_STORAGE/runroot"
graphroot = "$CONTAINER_STORAGE/storage"

[storage.options]
additionalimagestores = []
pull_options = {enable_partial_images = "false", use_hard_links = "false", ostree_repos=""}

[storage.options.overlay]
mountopt = "nodev,metacopy=on"
mount_program = "/usr/bin/fuse-overlayfs"
EOF
    
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "/home/$AIRFLOW_USER/.config/containers/storage.conf"
    
    # Create storage directory
    mkdir -p "$CONTAINER_STORAGE/storage"
    chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "$CONTAINER_STORAGE/storage"
    
    # Enable lingering for the user (allows containers to run when user is not logged in)
    log_info "Enabling user lingering..."
    loginctl enable-linger "$AIRFLOW_USER" || log_warn "Failed to enable lingering"
    
    # Run podman system migrate
    log_info "Running podman system migrate..."
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman system migrate" 2>&1 || {
        log_warn "Podman system migrate failed, trying to reset..."
        su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman system reset --force" 2>&1 || true
        su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman system migrate" 2>&1 || log_warn "Manual migration may be needed"
    }
    
    # Create helper script for running podman commands
    su - "$AIRFLOW_USER" -c "mkdir -p ~/.local/bin"
    su - "$AIRFLOW_USER" -c "cat > ~/.local/bin/podman-env << 'HELPEREOF'
#!/bin/bash
export XDG_RUNTIME_DIR=/run/user/\$(id -u)
export DBUS_SESSION_BUS_ADDRESS=unix:path=\$XDG_RUNTIME_DIR/bus
exec \"\$@\"
HELPEREOF
"
    su - "$AIRFLOW_USER" -c "chmod +x ~/.local/bin/podman-env"
    
    log_info "✓ Rootless Podman configured"
}

# Create directory structure
create_directories() {
    log_info "Creating directory structure..."
    
    # Create main directories
    mkdir -p "$AIRFLOW_HOME"/{dags,logs,plugins,utils,scripts,config}
    mkdir -p "$CONTAINER_STORAGE"/{volumes,data}
    
    # Create Podman compose directory
    mkdir -p "$AIRFLOW_HOME/podman"
    
    # Set ownership
    chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME"
    chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "$CONTAINER_STORAGE"
    
    # Set permissions (SELinux friendly)
    chmod 755 "$AIRFLOW_HOME"
    chmod 755 "$CONTAINER_STORAGE"
    
    log_info "✓ Directory structure created"
}

# Create volume directories
create_volume_directories() {
    log_info "Creating volume directories..."
    
    mkdir -p "$CONTAINER_STORAGE/volumes"/{postgres,redis,airflow}
    chown -R "$AIRFLOW_USER:$AIRFLOW_USER" "$CONTAINER_STORAGE/volumes"
    chmod -R 755 "$CONTAINER_STORAGE/volumes"
    
    log_info "✓ Volume directories created"
}

# Create .env file
create_env_file() {
    log_info "Creating environment files..."
    
    cat > "$AIRFLOW_HOME/.env" << 'EOF'
# Airflow Environment Variables
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Redis (no password for internal network)
REDIS_HOST=redis
REDIS_PORT=6379

# Custom
PYTHONPATH=/opt/airflow:/opt/airflow/dags:/opt/airflow/utils
APP_ENVIRONMENT=production
EOF
    
    cat > "$AIRFLOW_HOME/.env.secure" << 'EOF'
# Secure tokens (do not commit to version control)
AGENT_TOKEN=testtoken-123
AIRFLOW__API__SECRET_KEY=scbsecret123
AIRFLOW__CORE__FERNET_KEY=scbsecret123
EOF
    
    # Set permissions
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/.env"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/.env.secure"
    chmod 644 "$AIRFLOW_HOME/.env"
    chmod 600 "$AIRFLOW_HOME/.env.secure"
    
    log_info "✓ Environment files created"
}


# Create Podman network with bridge driver
create_podman_network() {
    log_info "Creating Podman network..."
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    
    # Check if network already exists
    if su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network exists $AIRFLOW_NETWORK" 2>/dev/null; then
        log_info "Network $AIRFLOW_NETWORK already exists"
        
        # Display network info
        su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network inspect $AIRFLOW_NETWORK" | grep -E 'name|driver|subnet|gateway' || true
        
        return 0
    fi
    
    # Create network with bridge driver (pasta not available)
    log_info "Creating network with bridge driver..."
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network create \
        --driver bridge \
        --subnet 10.89.0.0/24 \
        --gateway 10.89.0.1 \
        $AIRFLOW_NETWORK" 2>&1 | tee -a /tmp/network_create.log
    
    if [ $? -eq 0 ]; then
        log_info "✓ Network $AIRFLOW_NETWORK created with bridge driver"
    else
        log_error "Failed to create network"
        cat /tmp/network_create.log
        return 1
    fi
    
    # Display network info
    log_info "Network configuration:"
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network inspect $AIRFLOW_NETWORK" > /tmp/network_info.log 2>&1
    grep -E 'name|driver|subnet|gateway' /tmp/network_info.log || cat /tmp/network_info.log
    
    log_info "✓ Network created successfully"
}

# Create Podman Compose file with pasta networking
create_podman_compose() {
    log_info "Creating Podman Compose file with pasta networking..."
    
    cat > "$AIRFLOW_HOME/podman/podman-compose.yml" << EOF
version: '3.8'

x-airflow-common: &airflow-common
  image: artifactory.global.standardchartered.com/apache/airflow:3.1.7
  environment:
    LD_PRELOAD: ""
    LD_LIBRARY_PATH: ""
  env_file:
    - /apps/airflow/.env
    - /apps/airflow/.env.secure
  volumes:
    - /apps/airflow/dags:/opt/airflow/dags:Z,U
    - /apps/airflow/logs:/opt/airflow/logs:Z,U
    - /apps/airflow/plugins:/opt/airflow/plugins:Z,U
    - /apps/airflow/utils:/opt/airflow/utils:Z,U
    - /apps/airflow/config:/opt/airflow/config:Z,U
    - airflow-data:/opt/airflow:Z,U
  user: "50000:0"
  networks:
    - airflow-pasta-net
  security_opt:
    - label=disable

services:
  postgres:
    image: artifactory.global.standardchartered.com/gv-images-products/oss/postgres:18
    container_name: airflow-postgres
    hostname: postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
      PGDATA: /var/lib/postgresql/data/pgdata
      POSTGRES_HOST_AUTH_METHOD: trust
      POSTGRES_INITDB_ARGS: "--encoding=UTF8 --locale=C"
      LD_PRELOAD: ""
      LD_LIBRARY_PATH: ""
    volumes:
      - postgres-data:/var/lib/postgresql/data:Z
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow -d airflow || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 30s
    restart: unless-stopped
    networks:
      airflow-pasta-net:
        ipv4_address: 10.89.0.10
    security_opt:
      - label=disable

  redis:
    image: artifactory.global.standardchartered.com/redis:6.2.6
    container_name: airflow-redis
    hostname: redis
    environment:
      LD_PRELOAD: ""
      LD_LIBRARY_PATH: ""
    command: redis-server --appendonly yes --protected-mode no
    volumes:
      - redis-data:/data:Z
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 20
      start_period: 20s
    restart: unless-stopped
    networks:
      airflow-pasta-net:
        ipv4_address: 10.89.0.11
    security_opt:
      - label=disable

  airflow-init:
    image: $AIRFLOW_IMAGE
    container_name: airflow-init
    hostname: airflow-init
    environment:
      LD_PRELOAD: ""
      LD_LIBRARY_PATH: ""
      _AIRFLOW_DB_MIGRATE: "true"
    env_file:
      - /apps/airflow/.env
      - /apps/airflow/.env.secure
    volumes:
      - /apps/airflow/dags:/opt/airflow/dags:Z
      - /apps/airflow/logs:/opt/airflow/logs:Z
      - /apps/airflow/plugins:/opt/airflow/plugins:Z
      - /apps/airflow/utils:/opt/airflow/utils:Z
      - /apps/airflow/config:/opt/airflow/config:Z
      - airflow-data:/opt/airflow:Z
    user: "50000:0"
    networks:
      - airflow-pasta-net
    security_opt:
      - label=disable
    entrypoint: /bin/bash
    command:
      - -c
      - |
        set -e
        echo '=========================================='
        echo 'Waiting for PostgreSQL...<REMOVE LOGIC>'
        echo '=========================================='
              
        echo ''
        echo '=========================================='
        echo '✓ PostgreSQL is ready!'
        echo '=========================================='
        echo ''
        
        echo '=========================================='
        echo 'Initializing Airflow Database...'
        echo '=========================================='
        echo ''
        
        echo 'Running airflow db migrate...'
        airflow db migrate
        
        if [ \$? -eq 0 ]; then
          echo ''
          echo '=========================================='
          echo '✓ Database Initialization Complete!'
          echo '=========================================='
          echo ''
        else
          echo ''
          echo '=========================================='
          echo '✗ Database Initialization Failed!'
          echo '=========================================='
          exit 1
        fi
    restart: "no"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy

  airflow-api-server:
    <<: *airflow-common
    container_name: airflow-api-server
    hostname: airflow-api-server
    command: api-server
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD-SHELL", "curl --fail http://localhost:8080/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow-scheduler
    hostname: airflow-scheduler
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", "pgrep -f 'airflow scheduler' || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-worker:
    <<: *airflow-common
    container_name: airflow-worker
    hostname: airflow-worker
    environment:
      LD_PRELOAD: ""
      LD_LIBRARY_PATH: ""
      DUMB_INIT_SETSID: "0"
    command: celery worker
    healthcheck:
      test: ["CMD-SHELL", "celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d celery@\$\$HOSTNAME || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-triggerer:
    <<: *airflow-common
    container_name: airflow-triggerer
    hostname: airflow-triggerer
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", "pgrep -f 'airflow triggerer' || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  flower:
    <<: *airflow-common
    container_name: airflow-flower
    hostname: flower
    command: celery flower
    ports:
      - "5555:5555"
    healthcheck:
      test: ["CMD-SHELL", "curl --fail http://localhost:5555/ || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 90s
    restart: unless-stopped
    depends_on:
      airflow-init:
        condition: service_completed_successfully

volumes:
  postgres-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /apps/container/volumes/postgres
  redis-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /apps/container/volumes/redis
  airflow-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /apps/container/volumes/airflow

networks:
  airflow-pasta-net:
    external: true
EOF
    
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/podman/podman-compose.yml"
    
    log_info "✓ Podman Compose file created with pasta networking"
}

# Update .env file for pasta networking
create_env_file() {
    log_info "Creating environment files..."
    
    cat > "$AIRFLOW_HOME/.env" << 'EOF'
# Airflow Environment Variables
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Custom
PYTHONPATH=/opt/airflow:/opt/airflow/dags:/opt/airflow/utils
APP_ENVIRONMENT=production
EOF
    
    cat > "$AIRFLOW_HOME/.env.secure" << 'EOF'
# Secure tokens (do not commit to version control)
AGENT_TOKEN=testtoken-123
AIRFLOW__API__SECRET_KEY=change-this-to-random-secret-key
AIRFLOW__CORE__FERNET_KEY=change-this-to-fernet-key
EOF
    
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/.env"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/.env.secure"
    chmod 644 "$AIRFLOW_HOME/.env"
    chmod 600 "$AIRFLOW_HOME/.env.secure"
    
    log_info "✓ Environment files created"
}

copy_utils_files() {
    log_info "Copying utils files from script directory..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    UTILS_SOURCE="$SCRIPT_DIR/utils"
    DAGS_SOURCE="$SCRIPT_DIR/dags"
    
    # Check if utils directory exists in script directory
    if [ -d "$UTILS_SOURCE" ]; then
        log_info "Found utils directory at: $UTILS_SOURCE"
        
        # Create utils directory structure
        mkdir -p "$AIRFLOW_HOME/utils"
        
        # Copy all Python files from utils
        cp -v "$UTILS_SOURCE"/*.py "$AIRFLOW_HOME/utils/" 2>/dev/null || log_warn "No Python files found in $UTILS_SOURCE"
        
        # Ensure __init__.py exists
        touch "$AIRFLOW_HOME/utils/__init__.py"
        
        # Set ownership
        chown -R "$AIRFLOW_USER":"$AIRFLOW_USER" "$AIRFLOW_HOME/utils"
        
        log_info "✓ Utils files copied successfully"
        ls -la "$AIRFLOW_HOME/utils/"
    else
        log_warn "Utils directory not found at: $UTILS_SOURCE"
        log_info "Creating empty utils directory..."
        mkdir -p "$AIRFLOW_HOME/utils"
        touch "$AIRFLOW_HOME/utils/__init__.py"
        chown -R "$AIRFLOW_USER":"$AIRFLOW_USER" "$AIRFLOW_HOME/utils"
    fi
}

# Copy DAG files
copy_dag_files() {
    log_info "Copying DAG files from script directory..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    DAGS_SOURCE="$SCRIPT_DIR/dags"
    
    # Check if dags directory exists in script directory
    if [ -d "$DAGS_SOURCE" ]; then
        log_info "Found dags directory at: $DAGS_SOURCE"
        
        # Create dags directory
        mkdir -p "$AIRFLOW_HOME/dags"
        
        # Copy all Python files from dags
        cp -v "$DAGS_SOURCE"/*.py "$AIRFLOW_HOME/dags/" 2>/dev/null || log_warn "No DAG files found in $DAGS_SOURCE"
        
        # Set ownership
        chown -R "$AIRFLOW_USER":"$AIRFLOW_USER" "$AIRFLOW_HOME/dags"
        
        log_info "✓ DAG files copied successfully"
        ls -la "$AIRFLOW_HOME/dags/"
    else
        log_warn "DAGs directory not found at: $DAGS_SOURCE"
        log_info "Creating empty dags directory..."
        mkdir -p "$AIRFLOW_HOME/dags"
        chown -R "$AIRFLOW_USER":"$AIRFLOW_USER" "$AIRFLOW_HOME/dags"
    fi
}

# Create management scripts (continuing in next part due to length...)

# Create start script
create_start_script() {
    log_info "Creating start script..."
    
    cat > "$AIRFLOW_HOME/scripts/start_airflow.sh" << 'EOF'
#!/bin/bash
# Start Airflow with Podman Compose

set -e

COMPOSE_FILE="/apps/airflow/podman/podman-compose.yml"
export XDG_RUNTIME_DIR=/run/user/$(id -u)

echo "Starting Airflow services with Podman Compose..."

cd /apps/airflow/podman

# Pull latest images
echo "Pulling images..."
podman-compose pull || echo "Warning: Some images may have failed to pull"

# Start services
echo "Starting services..."
podman-compose up -d

# Wait for services to be healthy
echo "Waiting for services to start..."
sleep 30

# Check status
podman-compose ps

echo ""
echo "✓ Airflow services started successfully!"
echo ""
echo "Access Points:"
echo "  - Airflow UI:  http://localhost:8080 (admin/admin)"
echo "  - Flower UI:   http://localhost:5555"
echo ""
echo "Check logs: podman-compose logs -f"
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/start_airflow.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/start_airflow.sh"
    
    log_info "✓ Start script created"
}

create_stop_script() {
    log_info "Creating stop script..."
    
    cat > "$AIRFLOW_HOME/scripts/stop_airflow.sh" << 'EOF'
#!/bin/bash
# Stop Airflow with Podman Compose

set -e

export XDG_RUNTIME_DIR=/run/user/$(id -u)

echo "Stopping Airflow services..."

cd /apps/airflow/podman
podman-compose down

echo "✓ Airflow services stopped"
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/stop_airflow.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/stop_airflow.sh"
    
    log_info "✓ Stop script created"
}

create_restart_script() {
    log_info "Creating restart script..."
    
    cat > "$AIRFLOW_HOME/scripts/restart_airflow.sh" << 'EOF'
#!/bin/bash
# Restart Airflow with Podman Compose

set -e

export XDG_RUNTIME_DIR=/run/user/$(id -u)

echo "Restarting Airflow services..."

cd /apps/airflow/podman
podman-compose restart

echo "✓ Airflow services restarted"
echo ""
echo "Check status: podman-compose ps"
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/restart_airflow.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/restart_airflow.sh"
    
    log_info "✓ Restart script created"
}

create_status_script() {
    log_info "Creating status script..."
    
    cat > "$AIRFLOW_HOME/scripts/status_airflow.sh" << 'EOF'
#!/bin/bash
# Check Airflow status

set -e

export XDG_RUNTIME_DIR=/run/user/$(id -u)

echo "=========================================="
echo "  Airflow Podman Status"
echo "=========================================="
echo ""

cd /apps/airflow/podman

echo "Container Status:"
podman-compose ps

echo ""
echo "Network Info:"
podman network inspect airflow-network --format "{{.Name}}: {{.Driver}}" 2>/dev/null || echo "Network not found"

echo ""
echo "Volume Usage:"
du -sh /apps/container/volumes/* 2>/dev/null || echo "Volumes not yet created"

echo ""
echo "SELinux Status:"
getenforce 2>/dev/null || echo "SELinux tools not available"

echo ""
echo "SELinux Contexts:"
ls -Z /apps/airflow/dags 2>/dev/null | head -3 || echo "Unable to check contexts"

echo ""
echo "Recent Logs (last 10 lines from scheduler):"
podman-compose logs --tail=10 airflow-scheduler 2>/dev/null || echo "Scheduler not running"
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/status_airflow.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/status_airflow.sh"
    
    log_info "✓ Status script created"
}

create_logs_script() {
    log_info "Creating logs script..."
    
    cat > "$AIRFLOW_HOME/scripts/logs_airflow.sh" << 'EOF'
#!/bin/bash
# View Airflow logs

export XDG_RUNTIME_DIR=/run/user/$(id -u)

SERVICE="${1:-airflow-scheduler}"

cd /apps/airflow/podman

if [ -z "$1" ]; then
    echo "Showing all services logs..."
    podman-compose logs -f
else
    echo "Showing logs for: $SERVICE"
    podman-compose logs -f "$SERVICE"
fi
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/logs_airflow.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/logs_airflow.sh"
    
    log_info "✓ Logs script created"
}

create_verify_script() {
    log_info "Creating verification script..."
    
    cat > "$AIRFLOW_HOME/scripts/verify_setup.sh" << 'EOF'
#!/bin/bash
# Verify Airflow Podman Setup

echo "=========================================="
echo "  Airflow Podman Setup Verification"
echo "=========================================="
echo ""

# Check user
echo "1. Checking user configuration..."
id airflow
echo ""

# Check subuid/subgid
echo "2. Checking subuid/subgid..."
grep "^airflow:" /etc/subuid
grep "^airflow:" /etc/subgid
echo ""

# Check XDG_RUNTIME_DIR
echo "3. Checking XDG_RUNTIME_DIR..."
AIRFLOW_UID=$(id -u airflow)
ls -ld /run/user/$AIRFLOW_UID
echo ""

# Check podman
echo "4. Checking Podman..."
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman --version"
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman info --format '{{.Host.IDMappings}}'"
echo ""

# Check podman-compose
echo "5. Checking podman-compose..."
sudo -u airflow which podman-compose
sudo -u airflow podman-compose --version
echo ""

# Check images
echo "6. Checking pulled images..."
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman images"
echo ""

# Check network
echo "7. Checking network..."
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network ls"
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman network exists airflow-network" && echo "✓ Network exists" || echo "✗ Network missing"
echo ""

# Check containers
echo "8. Checking containers..."
cd /apps/airflow/podman
sudo -u airflow bash -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman-compose ps"
echo ""

# Check SELinux
echo "9. Checking SELinux..."
getenforce
ls -Zd /apps/airflow /apps/container
echo ""

echo "=========================================="
echo "  Verification Complete"
echo "=========================================="
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/verify_setup.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/verify_setup.sh"
    
    log_info "✓ Verification script created"
}

# Configure firewall
configure_firewall() {
    log_info "Configuring firewall..."
    
    if command -v firewall-cmd &> /dev/null; then
        firewall-cmd --permanent --add-port=8080/tcp || true  # Airflow UI
        firewall-cmd --permanent --add-port=5555/tcp || true  # Flower UI
        firewall-cmd --reload || true
        log_info "✓ Firewall configured"
    else
        log_warn "firewall-cmd not found, skipping firewall configuration"
    fi
}

# Create systemd service for auto-start
create_systemd_service() {
    log_info "Creating systemd service..."
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    
    cat > /etc/systemd/system/airflow-podman.service << EOF
[Unit]
Description=Airflow Podman Compose Services
After=network.target
Wants=network.target

[Service]
Type=oneshot
RemainAfterExit=yes
User=$AIRFLOW_USER
Group=$AIRFLOW_USER
Environment="XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID"
WorkingDirectory=/apps/airflow/podman
ExecStart=/home/$AIRFLOW_USER/.local/bin/podman-compose up -d
ExecStop=/home/$AIRFLOW_USER/.local/bin/podman-compose down
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF
    
    systemctl daemon-reload
    systemctl enable airflow-podman.service
    
    log_info "✓ Systemd service created and enabled"
}

# Pull images
pull_images() {
    log_info "Pulling container images..."
    
    # Verify podman-compose is accessible
    verify_podman_compose
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    USE_ALTERNATIVE=false
    
    log_info "Pulling Airflow image..."
    if ! su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $AIRFLOW_IMAGE" 2>&1 | tee -a /tmp/airflow_pull.log; then
        log_warn "Failed to pull Airflow image from artifactory, trying alternative..."
        if su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $ALT_AIRFLOW_IMAGE" 2>&1; then
            AIRFLOW_IMAGE="$ALT_AIRFLOW_IMAGE"
            USE_ALTERNATIVE=true
            log_info "✓ Using alternative Airflow image: $ALT_AIRFLOW_IMAGE"
        else
            log_error "Failed to pull Airflow images"
            exit 1
        fi
    fi
    
    log_info "Pulling PostgreSQL image..."
    if ! su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $POSTGRES_IMAGE" 2>&1 | tee -a /tmp/postgres_pull.log; then
        log_warn "Failed to pull PostgreSQL image from artifactory, trying alternative..."
        if su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $ALT_POSTGRES_IMAGE" 2>&1; then
            POSTGRES_IMAGE="$ALT_POSTGRES_IMAGE"
            USE_ALTERNATIVE=true
            log_info "✓ Using alternative PostgreSQL image: $ALT_POSTGRES_IMAGE"
        else
            log_error "Failed to pull PostgreSQL images"
            exit 1
        fi
    fi
    
    log_info "Pulling Redis image..."
    if ! su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $REDIS_IMAGE" 2>&1 | tee -a /tmp/redis_pull.log; then
        log_warn "Failed to pull Redis image from artifactory, trying alternative..."
        if su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman pull $ALT_REDIS_IMAGE" 2>&1; then
            REDIS_IMAGE="$ALT_REDIS_IMAGE"
            USE_ALTERNATIVE=true
            log_info "✓ Using alternative Redis image: $ALT_REDIS_IMAGE"
        else
            log_error "Failed to pull Redis images"
            exit 1
        fi
    fi
    
    # Update compose file if using alternative images
    if [ "$USE_ALTERNATIVE" = true ]; then
        log_info "Updating compose file with alternative images..."
        sed -i "s|artifactory.global.standardchartered.com/airflow/airflow:3.1.6.0ae41381b05bfafa204d91333bc1a7c21cde5715|$AIRFLOW_IMAGE|g" "$AIRFLOW_HOME/podman/podman-compose.yml"
        sed -i "s|artifactory.global.standardchartered.com/gv-images-products/oss/postgres:18|$POSTGRES_IMAGE|g" "$AIRFLOW_HOME/podman/podman-compose.yml"
        sed -i "s|artifactory.global.standardchartered.com/redis/redis:5992638|$REDIS_IMAGE|g" "$AIRFLOW_HOME/podman/podman-compose.yml"
        log_info "✓ Compose file updated with alternative images"
    fi
    
    # List pulled images
    log_info "Successfully pulled images:"
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman images" | grep -E 'REPOSITORY|airflow|postgres|redis'
    
    log_info "✓ Images pulled"
}

# Start Airflow services
start_airflow_services() {
    log_info "Starting Airflow services..."
    
    # Verify podman-compose is accessible
    verify_podman_compose
    
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    
    log_info "Starting services as $AIRFLOW_USER..."
    if ! su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && cd $AIRFLOW_HOME/podman && podman-compose up -d" 2>&1 | tee -a /tmp/airflow_start.log; then
        log_error "Failed to start Airflow services"
        log_info "Check logs at: /tmp/airflow_start.log"
        
        # Show more debug info
        log_info "Podman info:"
        su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman info" | head -30
        
        log_info "Container status:"
        su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman ps -a"
        
        exit 1
    fi
    
    log_info "Waiting for services to start (60 seconds)..."
    sleep 60
    
    # Check service status
    log_info "Checking service status..."
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && cd $AIRFLOW_HOME/podman && podman-compose ps" || log_warn "Unable to check service status"
    
    log_info "✓ Airflow services started"
}

# Verify SELinux contexts
verify_selinux_contexts() {
    log_info "Verifying SELinux contexts..."
    
    if command -v getenforce &> /dev/null; then
        SELINUX_STATUS=$(getenforce)
        
        if [ "$SELINUX_STATUS" = "Enforcing" ] || [ "$SELINUX_STATUS" = "Permissive" ]; then
            log_info "Checking SELinux contexts for key directories..."
            
            ls -Zd "$AIRFLOW_HOME" || log_warn "Unable to check $AIRFLOW_HOME context"
            ls -Zd "$CONTAINER_STORAGE" || log_warn "Unable to check $CONTAINER_STORAGE context"
            
            log_info "✓ SELinux context verification complete"
        fi
    fi
}

# Create credentials script
create_credentials_script() {
    log_info "Creating credentials helper script..."
    
    cat > "$AIRFLOW_HOME/scripts/get_credentials.sh" << 'EOF'
#!/bin/bash
# Get Airflow Admin Credentials

export XDG_RUNTIME_DIR=/run/user/$(id -u)

echo "=========================================="
echo "  Airflow Admin Credentials"
echo "=========================================="
echo ""

cd /apps/airflow/podman

# Extract password from logs
ADMIN_PASS=$(podman logs airflow-api-server 2>&1 | grep -oP "Password for user 'admin': \K\w+" | head -1)

if [ -n "$ADMIN_PASS" ]; then
    echo "Username: admin"
    echo "Password: $ADMIN_PASS"
    echo ""
    echo "Login at: http://localhost:8080"
else
    echo "Admin password not yet generated."
    echo "Wait for api-server to start, then run this script again."
    echo ""
    echo "Or check logs manually:"
    echo "  podman logs airflow-api-server | grep 'Password for user'"
fi

echo ""
EOF
    
    chmod +x "$AIRFLOW_HOME/scripts/get_credentials.sh"
    chown "$AIRFLOW_USER:$AIRFLOW_USER" "$AIRFLOW_HOME/scripts/get_credentials.sh"
    
    log_info "✓ Credentials script created"
}

# Display summary
display_summary() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Airflow Podman Setup Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "${BLUE}Configuration:${NC}"
    echo -e "  Executor:        CeleryExecutor"
    echo -e "  Database:        PostgreSQL 18"
    echo -e "  Message Broker:  Redis"
    echo -e "  Airflow Version: $AIRFLOW_VERSION"
    echo -e "  SELinux:         Enabled with proper contexts"
    echo -e "  Pip Index:       Artifactory"
    echo -e "  Auth Manager:    Simple Auth Manager"
    echo ""
    echo -e "${BLUE}Access Points:${NC}"
    echo -e "  Airflow UI:  ${YELLOW}http://localhost:8080${NC}"
    echo -e "  Flower UI:   ${YELLOW}http://localhost:5555${NC}"
    echo -e "  PostgreSQL:  ${YELLOW}localhost:5432${NC}"
    echo -e "  Redis:       ${YELLOW}localhost:6379${NC}"
    echo ""
    echo -e "${BLUE}Getting Admin Credentials:${NC}"
    echo -e "  Run: ${YELLOW}sudo -u airflow podman logs airflow-api-server 2>&1 | grep -A 2 'Simple auth manager'${NC}"
    echo -e "  Or:  ${YELLOW}cd /apps/airflow/podman && podman-compose logs airflow-api-server | grep -A 2 'Password for user'${NC}"
    echo ""
    echo -e "  ${YELLOW}Look for line like:${NC}"
    echo -e "  ${GREEN}Simple auth manager | Password for user 'admin': QAVuDDEwGQudGHqw${NC}"
    echo ""
    echo -e "${BLUE}Management Scripts (run as airflow user):${NC}"
    echo -e "  Start:    ${YELLOW}/apps/airflow/scripts/start_airflow.sh${NC}"
    echo -e "  Stop:     ${YELLOW}/apps/airflow/scripts/stop_airflow.sh${NC}"
    echo -e "  Restart:  ${YELLOW}/apps/airflow/scripts/restart_airflow.sh${NC}"
    echo -e "  Status:   ${YELLOW}/apps/airflow/scripts/status_airflow.sh${NC}"
    echo -e "  Logs:     ${YELLOW}/apps/airflow/scripts/logs_airflow.sh [service]${NC}"
    echo ""
    echo -e "${BLUE}Systemd Service:${NC}"
    echo -e "  Start:    ${YELLOW}systemctl start airflow-podman${NC}"
    echo -e "  Stop:     ${YELLOW}systemctl stop airflow-podman${NC}"
    echo -e "  Status:   ${YELLOW}systemctl status airflow-podman${NC}"
    echo ""
    echo -e "${BLUE}Verification:${NC}"
    echo -e "  Run: ${YELLOW}sudo bash /apps/airflow/scripts/verify_setup.sh${NC}"
    echo ""
    
    # Try to extract admin password from logs
    echo -e "${BLUE}Checking for admin credentials...${NC}"
    sleep 5
    AIRFLOW_UID=$(id -u "$AIRFLOW_USER")
    ADMIN_PASS=$(su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && podman logs airflow-api-server 2>&1" | grep -oP "Password for user 'admin': \K\w+" | head -1 || echo "")
    
    if [ -n "$ADMIN_PASS" ]; then
        echo -e "  ${GREEN}✓ Found admin password:${NC} ${YELLOW}$ADMIN_PASS${NC}"
        echo -e "  Username: ${YELLOW}admin${NC}"
        echo -e "  Password: ${YELLOW}$ADMIN_PASS${NC}"
    else
        echo -e "  ${YELLOW}⚠ Admin password not yet generated${NC}"
        echo -e "  ${YELLOW}Wait for api-server to fully start, then check logs:${NC}"
        echo -e "  ${YELLOW}podman logs airflow-api-server | grep 'Password for user'${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}Container Status:${NC}"
    su - "$AIRFLOW_USER" -c "export XDG_RUNTIME_DIR=/run/user/$AIRFLOW_UID && cd $AIRFLOW_HOME/podman && podman-compose ps" || echo "  Unable to check status"
    echo ""
}

# Main execution
main() {
    log_info "========================================"
    log_info "  Airflow Podman Installation Script"
    log_info "  Supports: RHEL 8 and RHEL 9"
    log_info "  SELinux: Enabled"
    log_info "  Pip Index: Artifactory"
    log_info "========================================"
    echo ""
    
    detect_os
    check_sudo
	create_base_directory
	create_airflow_user
    configure_pip
    configure_selinux
    install_podman
    install_oci_runtime
    install_podman_compose
    verify_podman_compose
    configure_subuid_subgid
    configure_rootless_podman
    create_directories
    create_volume_directories
    create_env_file
    create_podman_compose
    create_podman_network
    copy_utils_files
    copy_dag_files
    create_start_script
    create_stop_script
    create_restart_script
    create_status_script
    create_logs_script
	create_credentials_script
    create_verify_script
    configure_firewall
    create_systemd_service
    pull_images
    start_airflow_services
    verify_selinux_contexts
    display_summary
    
    echo ""
    log_info "✓ Installation complete!"
    log_info "Run verification: sudo bash /apps/airflow/scripts/verify_setup.sh"
    echo ""
}

# Run main function
main "$@"