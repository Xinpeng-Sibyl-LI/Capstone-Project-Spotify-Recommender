# **TuneGenie – Spotify Playlist Recommender & Analyzer** 🎧  

Welcome to my Spotify ELT Pipeline repository! 🎶

As both a data enthusiast and a daily Spotify listener, I created this project to explore the power of modern data platforms while diving into music analytics. This project showcases how to build a fully automated ELT (Extract, Load, Transform) pipeline using real-time data from the Spotify API, orchestrated with Apache Airflow and powered by Snowflake.


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

This project outlines the design and implementation of a cloud-based ELT pipeline that transforms raw Spotify data into a format ready for recommendation systems and analytical insights. The pipeline consists of multiple stages including data extraction, loading into Snowflake, transformation using SQL, and feature engineering — all automated via Airflow.


---

## Architecture Diagram  

![TuneGenie ELT Architecture](docs/img/tunegenie_architecture.png) <!-- Replace with the real PNG path once committed -->

---

## Project Components  

**1. Data Extraction**

Extract Spotify data using a Python script (`crawl.py`) from:

- Track metadata
- Audio features
- Artists & albums

**2. Data Loading (ELT)**

Raw structured data is loaded into **Snowflake**’s **Raw Data Layer**, enabling scalable, cloud-based storage and analysis.

**3. Data Transformation**

Using SQL inside Snowflake, the data is cleaned and enriched:

- Deduplication, null handling
- Type casting & encoding
- Feature enrichment (e.g., audio stats, genre behavior)

**4. Workflow Orchestration**

Using **Apache Airflow**, the pipeline is scheduled and automated with:

- Clear DAGs and task dependencies
- Logging and retry logic

**5. Analytics & Recommendation**

- Playlist trend analysis
- Genre & artist clustering
- Content-based and collaborative recommendation logic


---

## Getting Started  

### Prerequisites  

- Python 3.8+
- Spotify Developer Credentials
- Snowflake Account
- Airflow (installed locally or via Docker)
- Python packages: `requests`, `pandas`, `snowflake-connector-python`, `spotipy`, etc.

