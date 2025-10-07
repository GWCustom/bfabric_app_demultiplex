<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/GWCustom/bfabric_app_demultiplex">
    <img src="https://drive.google.com/uc?export=view&id=1_RekqDx9tOY-4ziZLn7cG9sozMXIhrfE" alt="Logo" width="80" height="50.6">
  </a>

<h3 align="center">NF-Core Demultiplex App</h3>

<p align="center">
  A B-Fabric WebApp for invoking NF-Core demultiplexing workflows, tightly integrated with B-Fabric.
  <br />
  <br />
  <a href="https://github.com/GWCustom/bfabric_app_demultiplex/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
  ·
  <a href="https://github.com/GWCustom/bfabric_app_demultiplex/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
</p>
</div>

> **Note**: This app is based on the [bfabric-web-app-template](https://github.com/GWCustom/bfabric-web-app-template), and built using the [bfabric-web-apps](https://github.com/GWCustom/bfabric-web-apps) Python library.

---

## About

The **Demultiplex App** is a proof-of-concept Dash web application demonstrating integration between [B-Fabric](https://fgcz-bfabric.uzh.ch/bfabric/), the [NF-Core Demultiplex pipeline](https://nf-co.re/demultiplex/1.5.4/), and a Redis-based job queue.

Its primary purpose is to validate the flexibility and generality of the new `bfabric_web_apps` framework for creating web-based data processing interfaces. This specific implementation automates demultiplexing of short-read sequencing data using the `nf-core/demultiplex` workflow.

> Note: This app is not production-hardened for all sequencing instruments or use cases — it serves as a template and demonstrator.

---

## Features

- Full integration with B-Fabric API for sample metadata and storage
- Sample sheet editing via Dash UI
- Asynchronous job execution using Redis queues
- Custom resource path mapping and dataset creation
- Based on `redis_index.py` from [`bfabric-web-app-template`](https://github.com/GWCustom/bfabric-web-app-template)

### NF-Core Workflow Modules Used

- `checkqc`
- `bcl2fastq`
- `kraken`
- `falco`
- `fastp`
- `md5sum`
- `multiqc`

![NF-Core Pipeline Overview](https://github.com/nf-core/demultiplex/raw/master/docs/demultiplex.png)

---

## Architecture Overview

The app follows a three-tier design:

- **UI Server** – Dash frontend hosted locally
- **Compute Server** – Executes the Nextflow pipeline via Redis
- **B-Fabric** – LIMS integration for metadata and result registration

---

## Component Overview

- **[GetDataFromBfabric.py](https://github.com/GWCustom/bfabric_app_demultiplex/blob/main/GetDataFromBfabric.py)**  
  Retrieves sample metadata and generates pipeline-ready samplesheets.

- **[GetDataFromUser.py](https://github.com/GWCustom/bfabric_app_demultiplex/blob/main/GetDataFromUser.py)**  
  Provides interactive Dash UI for users to edit and review samplesheets.

- **[ExecuteRunMainJob.py](https://github.com/GWCustom/bfabric_app_demultiplex/blob/main/ExecuteRunMainJob.py)**  
  Prepares job data, constructs command-line execution, and handles Redis queuing.

---

## Built With

- [Python](https://www.python.org/)
- [Dash](https://dash.plotly.com/)
- [Plotly](https://plotly.com/)
- [Flask](https://flask.palletsprojects.com/)
- [bfabric-web-apps](https://github.com/GWCustom/bfabric-web-apps)

---

## Quickstart

### 1. Clone the Repository

```bash
git clone https://github.com/GWCustom/bfabric_app_demultiplex.git
cd bfabric_app_demultiplex
```

### 2. Create and Activate a Virtual Environment

#### Using `virtualenv` (Linux/Mac):

```bash
python3 -m venv venv
source venv/bin/activate
```

#### Using `virtualenv` (Windows):

```bash
python -m venv venv
venv\Scripts\activate
```

#### Or use `conda`:

```bash
conda create -n demultiplex-app pip
conda activate demultiplex-app
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up `.bfabricpy.yml`

Place this config file in your home directory (`~/.bfabricpy.yml`):

```yaml
GENERAL:
  default_config: PRODUCTION

PRODUCTION:
  login: your_username
  password: your_password
  base_url: https://your-bfabric-api-endpoint
```

### 5. Create Your `.env` File

The app uses a `.env` file to store environment variables required for running nextflow.
An example file (`.env.example`) is included in the repository.

Create your own `.env` file by copying the example:

```bash
cp .env.example .env
```

Then open `.env` in a text editor and adjust the values to match your environment.

### 6. Run the App

```bash
python3 redis_index.py
```

Then open [http://localhost:8050](http://localhost:8050) in your browser.

---

## Docker Deployment

You can deploy the **Demultiplex App** using Docker Compose, which automatically sets up all required services.

> **Security Note:**  
> The `worker` container runs as **root** and mounts the **Docker socket**.  
> This setup is required for Nextflow but grants the container **elevated privileges** on the host.  
> Only use this configuration in **trusted environments**.

---

### 1. Clone the Repository

If you haven’t already:

```bash
git clone https://github.com/GWCustom/bfabric_app_demultiplex.git
cd bfabric_app_demultiplex
```

---

### 2. Configure `.bfabricpy.yml`

Before launching the containers, ensure you have your B-Fabric API credentials configured in `~/.bfabricpy.yml`:

```yaml
GENERAL:
  default_config: PRODUCTION

PRODUCTION:
  login: your_username
  password: your_password
  base_url: https://your-bfabric-api-endpoint
```

---

### 3. Create Your `.env` File

The app uses a `.env` file to store environment variables required by Docker Compose.
An example file (`.env.example`) is included in the repository.

Create your own `.env` file by copying the example:

```bash
cp .env.example .env
```

Then open `.env` in a text editor and adjust the values to match your environment.

> **Important:** Comment out the `REDIS_HOST` line so the app can connect to the Redis service correctly within Docker Compose.
> When Redis runs as part of the same Compose network, it is automatically reachable via the service name `redis`.

---

### 4. Review and Adjust Configuration Files

Before running the containers, you must **update file paths and settings** in four places to match your environment.
These paths point to local folders, users, and binaries that must exist on your server.

---

#### **A. `index.py`**

In `index.py`, there are **three paths** that need to be updated.
Replace each of these with the **correct paths on your server**, ensuring:

* `base_dir` points to the main project directory containing your sample sheets
* `output_dir` points to the folder where you want to save pipeline outputs
* `NEXTFLOW_BIN` points to the Nextflow executable inside the container

> Make sure these paths are consistent with the volumes you mount in your `docker-compose.yml`.

---

#### **B. `NFC_DMX.config`**

You also need to configure the **Nextflow configuration file** used by the pipeline: `NFC_DMX.config`.
You **must adjust the `workDir` path** to match your local environment.

Open the file `NFC_DMX.config` and locate the following line near the bottom:

```groovy
workDir = "/home/azureuser/APPLICATION/200611_A00789R_0071_BHHVCCDRXX/work"
```
---

#### **C. `Dockerfile`**

In the `Dockerfile`, you can **adjust the user** if your environment requires a different username:

```dockerfile
RUN useradd -ms /bin/bash azureuser
USER azureuser
WORKDIR /workspace
```

If you change the username (e.g. from `azureuser` to `myuser`), make sure to update:

* All path references (e.g. `/home/azureuser/...`)
* The mounted paths in your `docker-compose.yml`


---

#### **D. `docker-compose.yml`**

Finally, review the `docker-compose.yml` and update all path-related entries under `environment:` and `volumes:`.

**Key environment variables to update**

Make sure the following environment variables match your local environment.
They define where the app reads input data, writes output results, and locates the Nextflow binary.

```yaml
environment:
  BASE_DIR: "/home/azureuser/APPLICATION/200611_A00789R_0071_BHHVCCDRXX/"   # Input directory
  OUTPUT_DIR: "/home/azureuser/STORAGE/OUTPUT_TEST"                         # Output directory
  NEXTFLOW_BIN: "/home/azureuser/.local/bin/nextflow"                       # Path to Nextflow binary on the host
  NXF_HOME: "/workspace/.nextflow"                                          # Nextflow cache directory inside the container
```

**Key volumes to update**

Make sure the following mounted paths match your local environment.
They define where the app reads input data, writes output results, and accesses credentials.

```yaml
volumes:
  - /home/azureuser/APPLICATION:/home/azureuser/APPLICATION    # Input directory
  - /home/azureuser/STORAGE:/home/azureuser/STORAGE            # Output directory
  - /home/azureuser/.bfabricpy.yml:/home/azureuser/.bfabricpy.yml:ro  # B-Fabric credentials
  - /home/azureuser/.ssh:/home/azureuser/.ssh:ro               # SSH keys
  - /home/azureuser/.ssh:/root/.ssh:ro
```

> Make sure to adjust the paths in both the web and worker services.
---

### 5. Build and Start the Containers

#### **A. Build the Images**

Run the following command to build all service images:

```bash
docker compose build
```

---

#### **B. Start the Services**

Once the build is complete, start all services:

```bash
docker compose up
```

---

### 6. Access the App

Once the containers are running, open your browser and navigate to:

```
http://localhost:8050
```

The Dash UI should now be live and connected to Redis.

---

### 7. Stop the Containers

To stop all running containers:

```bash
docker compose down
```

> This stops and removes the containers but keeps volumes and images intact.

---

## License

Distributed under the MIT License. See [LICENSE](https://github.com/GWCustom/bfabric_app_demultiplex/blob/main/LICENSE) for details.

---

## Contact

GWC GmbH - [GitHub](https://github.com/GWCustom)  
Griffin White - [LinkedIn](https://www.linkedin.com/in/griffin-white-3aa20918a/)  
Marc Zuber - [LinkedIn](https://www.linkedin.com/in/marc-zuber-1161b3305/)
