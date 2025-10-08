# Start from a lightweight Python 3.11 image
FROM python:3.11-slim

# ----------------------------------------------------------
# Install system dependencies
# ----------------------------------------------------------
# Includes:
# - Java runtime (required by Nextflow)
# - Docker CLI (so Nextflow can launch containers via -profile docker)
# - Git (used by Nextflow to pull pipelines)
# - curl, ca-certificates, gnupg (for secure downloads)
# - SSH client
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless \
      curl \
      git \
      ca-certificates \
      openssh-client \
      gnupg \
  && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------
# Install Docker CLI (to allow Nextflow to run Docker containers)
# ----------------------------------------------------------
# This sets up the official Docker repository and installs the CLI.
# The container uses the host's Docker socket (/var/run/docker.sock)
# to communicate with the host Docker daemon. (Docker-outside-of-Docker)
# -m 0755 = make it readable by everyone, writable by root.
 # apt will use this key to verify Docker packages are real and safe.
RUN install -m 0755 -d /etc/apt/keyrings \ 
 && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
 && chmod a+r /etc/apt/keyrings/docker.gpg \
 && . /etc/os-release \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list \
 && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli \
 && rm -rf /var/lib/apt/lists/*

# Create key folder → download Docker’s trust key → register Docker repo → update apt → install Docker CLI → clean up.



# Create a non-root user (azureuser) for security and reproducibility
# creates a new user called azureuser, gives them a home folder, and sets their shell to bash.
# safer than running everything as root.
RUN useradd -ms /bin/bash azureuser

# Switch to the new user
USER azureuser

# Set working directory (this is where your code will run)
WORKDIR /workspace

# Add Nextflow to PATH and define the version to install
# Adds ~/.local/bin to the PATH so commands there can be run; sets the Nextflow version variable.
ENV PATH="/home/azureuser/.local/bin:${PATH}" \
    NXF_VER=25.04.7

# ----------------------------------------------------------
# Install Nextflow (workflow engine)
# ----------------------------------------------------------
# Downloads the Nextflow binary, moves it into the user’s PATH,
# and ensures it’s executable.
# so you can run nextflow inside the container without root.
RUN curl -fsSL https://get.nextflow.io | bash -s - -v ${NXF_VER} \
 && mkdir -p /home/azureuser/.local/bin \
 && mv nextflow /home/azureuser/.local/bin/nextflow \
 && chmod +x /home/azureuser/.local/bin/nextflow

# ----------------------------------------------------------
# Install Python dependencies (as root)
# ----------------------------------------------------------
# Switch back to root to install Python packages globally.
# temporarily switch to root.
USER root
# copy your requirements.txt and install all Python deps globally.
COPY requirements.txt /workspace/requirements.txt
RUN pip install --no-cache-dir -r /workspace/requirements.txt

# ----------------------------------------------------------
# Copy project files (fallback code)
# ----------------------------------------------------------
# fallback copy so the image can run even without bind mounts (Compose will override this with .:/workspace).
# The files copied here will be replaced at runtime if a bind mount is used
# (e.g. via Docker Compose: `.:/workspace`)
COPY . /workspace

# Fix permissions so the non-root user can access everything
# So the non-root user can read/write the code and generated files.
RUN chown -R azureuser:azureuser /workspace

# Switch back to the non-root user
# switch back to the safer non-root user for running the app.
USER azureuser
