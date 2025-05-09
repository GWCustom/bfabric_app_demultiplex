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

![Architecture Diagram](https://i.imgur.com/JgOI3Xx.jpeg)

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

%%%bash
git clone https://github.com/GWCustom/bfabric_app_demultiplex.git
cd bfabric_app_demultiplex
%%%

### 2. Create and Activate a Virtual Environment

#### Using `virtualenv` (Linux/Mac):

%%%bash
python3 -m venv venv
source venv/bin/activate
%%%

#### Using `virtualenv` (Windows):

%%%bash
python -m venv venv
venv\Scripts\activate
%%%

#### Or use `conda`:

%%%bash
conda create -n demultiplex-app pip
conda activate demultiplex-app
%%%

### 3. Install Dependencies

%%%bash
pip install -r requirements.txt
%%%

### 4. Set Up `.bfabricpy.yml`

Place this config file in your home directory (`~/.bfabricpy.yml`):

%%%yaml
GENERAL:
  default_config: PRODUCTION

PRODUCTION:
  login: your_username
  password: your_password
  base_url: https://your-bfabric-api-endpoint
%%%

### 5. Run the App

%%%bash
python3 redis_index.py
%%%

Then open [http://localhost:8050](http://localhost:8050) in your browser.

---

## License

Distributed under the MIT License. See [LICENSE](https://github.com/GWCustom/bfabric_app_demultiplex/blob/main/LICENSE) for details.

---

## Contact

GWC GmbH - [GitHub](https://github.com/GWCustom)  
Griffin White - [LinkedIn](https://www.linkedin.com/in/griffin-white-3aa20918a/)  
Marc Zuber - [LinkedIn](https://www.linkedin.com/in/marc-zuber-1161b3305/)
