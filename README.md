# **TuneGenie – Spotify Playlist Recommender & Analyzer** 🎧  

Welcome to **TuneGenie**, a fully-automated **ELT** pipeline that ingests real-time data from the [Spotify Web API](https://developer.spotify.com/documentation/web-api) → lands it in **Snowflake** → models it with **SQL / dbt** → orchestrates every step with **Apache Airflow**.  
The goal: build reliable music-analytics tables and power playlist & track-recommendation experiments.

---

## 📑 Table of Contents  
1. [Overview](#overview)  
2. [Architecture Diagram](#architecture-diagram)  
3. [Project Components](#project-components)  
4. [Getting Started](#getting-started)  
5. [Running the Pipeline](#running-the-pipeline)  
6. [Example Outputs](#example-outputs)  
7. [License](#license)  

---

## Overview  

> **Why?** I’m a daily Spotify listener & data-stack tinkerer. This repo shows how modern tooling turns raw API JSON into analytics-ready marts and recommendation features—all reproducibly and in the cloud.

* **Extract** – Python crawler streams track / artist / audio-feature payloads.  
* **Load** – Snowpipe Streaming (or Kafka Connector) hydrates a `RAW_*` schema in Snowflake.  
* **Transform** – dbt models build `STG`, `DIM`, and `FCT` layers.  
* **Orchestrate** – Airflow DAGs schedule everything & handle retries / logging.  
* **Analyze** – Notebooks & dashboards explore genre trends, audio-feature clusters, and recommend songs.

---

## Architecture Diagram  

![TuneGenie ELT Architecture](docs/img/tunegenie_architecture.png) <!-- Replace with the real PNG path once committed -->

---

## Project Components  

| Stage | Folder | Key Files / Tech | What It Does |
|-------|--------|------------------|--------------|
| **1. Extract** | [`ingestion/`](ingestion/) | [`crawl.py`](ingestion/crawl.py) → [Spotipy](https://spotipy.readthedocs.io/) | Calls Spotify API for **tracks, artists, audio features** and publishes JSON to Kafka (or local staging). |
| **2. Load** | [`streaming/`](streaming/) | [`kafka.yaml`](streaming/kafka.yaml), Snowflake Kafka Connector | Streams each message into `RAW_TRACKS`, `RAW_ARTISTS`, … tables. |
| **3. Transform** | [`dbt/`](dbt/) | [`models/staging/`](dbt/models/staging/), [`models/marts/`](dbt/models/marts/) | Cleans, de-dupes, casts types, enriches features → `dim_artist`, `fct_top_tracks`, etc. |
| **4. Orchestrate** | [`airflow/`](airflow/) | [`dags/spotify_elt_dag.py`](airflow/dags/spotify_elt_dag.py), [`docker-compose.yaml`](airflow/docker-compose.yaml) | Hourly DAG: `crawl → load monitor → dbt run → dbt test`. |
| **5. Analytics / Recs** | [`notebooks/`](notebooks/) | `playlist_recommender.ipynb` | K-means on audio vectors, cosine similarity by user taste, etc. |

---

## Getting Started  

### Prerequisites  

| Tool | Link | Notes |
|------|------|-------|
| **Python 3.10 +** | <https://www.python.org/downloads/> | Local dev & dbt. |
| **Docker Desktop** | <https://docs.docker.com/desktop/> | Spins up Kafka + Airflow + Connector. |
| **Spotify Developer App** | <https://developer.spotify.com/dashboard> | Grab `client_id` & `client_secret`. |
| **Snowflake Account** | <https://signup.snowflake.com/> | Free trial works fine. |
| **dbt-Snowflake** | <https://docs.getdbt.com/docs/get-started> | Installed inside Airflow image or host venv. |

```bash
# clone & bootstrap
git clone https://github.com/<your-user>/tunegenie.git
cd tunegenie

# copy example env and fill in secrets
cp .env.example .env
# edit .env -> SPOTIFY, SNOWFLAKE, KAFKA creds

# build & start streaming + airflow stacks
docker compose -f streaming/kafka.yaml up -d
docker compose -f airflow/docker-compose.yaml up -d

