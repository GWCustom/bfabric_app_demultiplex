# Start from a lightweight Python 3.11 image
FROM python:3.11-slim

# ----------------------------------------------------------
# System deps: Java (Nextflow), curl, git, openssh, gnupg, certs
# ----------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-21-jre-headless \
      curl \
      git \
      ca-certificates \
      openssh-client \
      gnupg \
  && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------
# (REMOVED) Docker-CLI – nicht mehr nötig
# ----------------------------------------------------------
# ✂️ Den kompletten Block mit docker-ce-cli und keyrings entfernen

# ----------------------------------------------------------
# Non-root user
# ----------------------------------------------------------
RUN useradd -ms /bin/bash azureuser
USER azureuser
WORKDIR /workspace

# ----------------------------------------------------------
# Nextflow
# ----------------------------------------------------------
ENV PATH="/home/azureuser/.local/bin:${PATH}" \
    NXF_VERSION=24.04.2
RUN curl -sL https://get.nextflow.io | bash \
 && mkdir -p /home/azureuser/.local/bin \
 && mv nextflow /home/azureuser/.local/bin/nextflow \
 && chmod +x /home/azureuser/.local/bin/nextflow

# ----------------------------------------------------------
# Micromamba (Conda ohne Root) + Nextflow-Env-Variablen
# ----------------------------------------------------------
USER root
RUN curl -L https://micro.mamba.pm/api/micromamba/linux-64/1.5.8 -o /usr/local/bin/micromamba \
 && chmod +x /usr/local/bin/micromamba
USER azureuser

# Nextflow auf micromamba hinweisen + Caches in Workspace legen
ENV NXF_MAMBA_CLI=micromamba \
    NXF_CONDA_CACHEDIR=/workspace/.conda \
    MAMBA_ROOT_PREFIX=/opt/micromamba
# optional: Cache-Verzeichnisse anlegen (gehören azureuser)
RUN mkdir -p /workspace/.conda

# ----------------------------------------------------------
# Python deps
# ----------------------------------------------------------
# ... vorher: USER root
USER root

# Wenn requirements.txt im Projektroot liegt:
COPY requirements.txt /workspace/requirements.txt
COPY bfabric-web-apps /workspace/vendor/bfabric-web-apps

RUN pip install --no-cache-dir -r /workspace/requirements.txt

# ----------------------------------------------------------
# Project files (fallback) + Rechte
# ----------------------------------------------------------
USER root
COPY . /workspace
RUN chown -R azureuser:azureuser /workspace
USER azureuser
