# Music Streaming Data Model Documentation

## Overview
This data model follows a medallion architecture pattern with clear separation of concerns across four distinct layers: Raw, Staging, Intermediate, and Mart. The model is designed to process music streaming data from platforms like Spotify, transforming raw API data and user listening events into business-ready analytics tables.

## Architecture Layers

### 🏗️ Raw Layer (Bronze)
The raw layer contains unprocessed data directly from source systems:

- **RAW_LISTENING_HISTORY**: User play events with timestamps, device info, and metadata  
- **RAW_TOP_TRACKS**: Track catalog data from Spotify API with JSON metadata  
- **RAW_TOP_ARTISTS**: Artist catalog data including popularity scores and follower counts  

**Purpose:** Data lake storage with full fidelity of source systems

---

### 🧹 Staging Layer (Silver)
The staging layer applies data quality rules and standardization:

#### stg_listening_history
- **Purpose:** Cleans and standardizes listening events  
- **Key Transformations:**
  - Type casting (Snowflake uppercase columns → lowercase)  
  - Null handling and data validation  
  - Genre array processing with fallback logic  
  - Device and language standardization  
- **Materialization:** View (lightweight, always fresh)

#### stg_top_tracks
- **Purpose:** Processes track catalog with deduplication  
- **Key Transformations:**
  - JSON parsing for album data (`JSON_DATA:"album":"name"`)  
  - Boolean conversion for explicit content flag  
  - Row-level deduplication using window functions  
- **Materialization:** View with deduplication logic

#### stg_top_artists
- **Purpose:** Clean artist master data  
- **Key Transformations:**
  - Standardizes artist attributes  
  - Implements SCD Type 1 (latest record wins)  
  - Preserves genre arrays for downstream processing  
- **Materialization:** View with deduplication

---

### 🔗 Intermediate Layer (Silver)
The intermediate layer creates reusable business logic components:

#### int_listening_history_enriched
- **Purpose:** Core fact table joining listening events with dimensional data  
- **Business Logic:**
  - Left joins listening history with track and artist details  
  - Preserves all listening events even if catalog data is missing  
  - Creates a wide denormalized table for analytics  
- **Materialization:** Table (performance optimization for downstream models)

#### int_tracks_with_artists
- **Purpose:** Dimensional model joining tracks with their artists  
- **Business Logic:**
  - Simple join between track and artist catalogs  
  - Supports multiple marts without duplicating join logic  
  - Foundation for catalog-based analytics  
- **Materialization:** Table (reused by multiple marts)

---

### 📊 Mart Layer (Gold)
The mart layer provides business-ready analytics tables:

#### mart_user_daily_plays ⭐ (Incremental)
- **Purpose:** Daily user listening behavior metrics  
- **Key Features:**
  - **Incremental Processing:** Only processes new/changed data using `last_play_ts`  
  - **Partitioning:** Date-based partitioning for query performance  
- **Metrics Calculated:**
  - Play counts and unique content consumption  
  - Listening duration and intensity (plays per hour)  
  - Track popularity analysis  
  - Data quality scoring  
- **Business Value:** Powers user engagement dashboards and retention analysis

#### mart_artist_summary
- **Purpose:** Artist performance analytics and classification  
- **Source:** `int_tracks_with_artists` (requires track-level data for aggregations)  
- **Key Features:**
  - **Statistical Metrics:** Average, min, max, standard deviation of track popularity  
  - **Artist Tiering:** Automated classification based on follower counts  
  - **Consistency Rating:** Measures variation in track performance across artist's catalog  
  - **Track Aggregations:** Count of tracks, popularity ranges, performance consistency  
- **Business Value:** A&R team insights, artist development tracking, catalog analysis

#### mart_top_tracks
- **Purpose:** Trending content discovery and ranking  
- **Source:** `int_tracks_with_artists` (needs both track and artist data for dual ranking)  
- **Key Features:**
  - **Ranking Logic:** Dual sort by track and artist popularity  
  - **Top 100 Filter:** Focused on most relevant content  
  - **Popularity Ranges:** Categorical grouping using custom macro  
  - **Artist Context:** Includes artist popularity and follower counts for context  
- **Business Value:** Playlist curation, content recommendation engines, trending analysis

---

## Data Flow & Dependencies

### Layer-by-Layer Data Flow
```plaintext
RAW SOURCES                STAGING                 INTERMEDIATE               MARTS
┌─────────────────┐       ┌─────────────────┐    ┌────────────────────┐    ┌──────────────────┐
│raw_listening_   │──────▶│stg_listening_   │───┐│int_listening_      │───▶│mart_user_daily_  │
│history          │       │history          │   ││history_enriched    │    │plays             │
└─────────────────┘       └─────────────────┘   │└────────────────────┘    └──────────────────┘
                                                │
┌─────────────────┐       ┌─────────────────┐   │┌────────────────────┐    ┌──────────────────┐
│raw_top_tracks   │──────▶│stg_top_tracks   │───┼│int_tracks_with_    │───▶│mart_artist_      │
└─────────────────┘       └─────────────────┘   ││artists             │   ▶│summary           │
                                                ││                    │    └──────────────────┘
┌─────────────────┐       ┌─────────────────┐   │└────────────────────┘    ┌──────────────────┐
│raw_top_artists  │──────▶│stg_top_artists  │───┘                         │mart_top_tracks   │
└─────────────────┘       └─────────────────┘                            ▶└──────────────────┘
