# 🏀 Seattle Community Sports Pipeline

## 🚀 TL;DR

End-to-end **data platform + product** that:

* Scrapes and normalizes Seattle community center sports schedules
* Stores **quarterly snapshots** with historical tracking
* Exposes data via a **FastAPI backend**
* Powers an interactive **Streamlit dashboard** (maps, tiles, search)
* Captures **user-reported issues tied to data records**

**Tech Stack:** Python, PostgreSQL, FastAPI, Streamlit, Prefect
**Core Strengths:** Data normalization, time-aware modeling, geospatial UI, observability

---

## 🌐 Live Demo

* Dashboard: *[Add Streamlit URL]*
* API: *[Add Render URL]/programs*

---

## 📸 Demo

*Add screenshots here*

* Dashboard view
* Map view (community centers)
* Program tiles
* Schedule details

---

## 💡 Problem

Seattle community centers publish sports schedules across **many independent pages** with:

* inconsistent formats
* mixed seasonal data
* no unified search

Users must manually browse multiple sites to answer simple questions like:

* What’s available today?
* Where can I play badminton or basketball?
* Which programs are current vs outdated?

---

## 🛠 What This Project Does

This system turns fragmented web data into a **usable product**:

1. Discovers all community centers with valid websites
2. Scrapes and stores raw HTML (for traceability)
3. Parses and normalizes messy schedule data
4. Extracts **season metadata** (e.g., Spring 2026)
5. Stores structured data in PostgreSQL with **quarterly snapshots**
6. Serves data through FastAPI endpoints
7. Displays results in an interactive Streamlit dashboard
8. Captures **user feedback linked to data records**

---

## 🧩 Architecture

```text
Seattle Open Data / Center Websites
        ↓
fetch_centers.py
        ↓
scraper (requests + parsing)
        ↓
raw HTML storage
        ↓
parser + transform (Pandas)
        ↓
PostgreSQL (snapshots + programs + reports)
        ↓
FastAPI
        ↓
Streamlit Dashboard
```

---

## 🛠 Tech Stack

* **Data Ingestion:** Python, Requests, BeautifulSoup
* **Processing:** Pandas
* **Database:** PostgreSQL
* **API:** FastAPI
* **Frontend:** Streamlit
* **Orchestration:** Prefect
* **Visualization:** PyDeck (maps)

---

## 🗄 Data Model

### `programs`

Stores normalized sessions:

* center, program
* day_of_week, start_time, end_time
* age_min, age_max
* snapshot_id, quarter, year
* program_uid, session_uid

---

### `snapshots`

Captures seasonal context:

* center
* season_label (e.g., Spring 2026)
* season_start_date, season_end_date
* quarter, year

---

### `reports`

Stores user feedback:

* center, program
* issue_type, description
* program_uid, session_uid
* snapshot_id

---

## ⚙️ Key Challenges Solved

### Data Complexity

* Fragmented sources across many websites
* Inconsistent HTML structures and formats

### Temporal Modeling

* Seasonal labels ≠ calendar quarters
* Built snapshot system for historical tracking

### Pipeline Reliability

* Prevented duplicate loads
* Designed safe reruns per quarter

### Product Layer

* Built UI over unstable source data
* Linked user feedback to underlying records

---

## ⚡ Quick Start

```bash
pip install -r requirements.txt

# Run full pipeline
python -m pipeline.orchestrator

# Start API
uvicorn api.main:app --reload

# Start dashboard
streamlit run dashboard/app.py
```

---

## 🔄 Orchestration

Pipeline is orchestrated using **Prefect**:

* fetch centers
* scrape websites
* parse HTML
* load into database

### Scheduled cadence (intended)

* January 1
* April 1
* July 1
* October 1

---

## 📊 Current Features

* Search and filtering (program, center, day, quarter)
* Program tiles with pagination
* “Today’s programs” summary
* Interactive maps:

  * simple view
  * density/insight view
* Schedule grouping
* User issue reporting
* State-aware UI interactions

---

## 🚧 Future Improvements

* Admin interface for resolving reports
* Automated testing for parser edge cases
* Data quality monitoring / anomaly detection
* Historical comparisons across quarters
* Mobile responsiveness improvements
* Migration to custom frontend (React)

---

## 💡 Why This Project Stands Out

* Works with **messy real-world data**, not clean datasets
* Implements **time-aware snapshot modeling**
* Connects backend pipeline → API → UI
* Adds **user feedback loop tied to data records**
* Balances **data engineering + product thinking**

---

## 🎯 What This Demonstrates

* End-to-end data pipeline design
* Backend API development
* Database modeling for temporal data
* Frontend interaction design
* Observability (logging + orchestration)

---

## 🧠 Summary

This is not just a scraper.

It is a **location-based civic data product** built on top of a repeatable pipeline that:

* handles unstable source data
* preserves historical context
* powers a user-facing application
* enables feedback-driven data improvement

---

## 📁 Repository Structure

```text
seattle-sports-pipeline/
├── api/
├── dashboard/
├── data/
├── pipeline/
├── scraper/
├── logs/
├── requirements.txt
└── README.md
```

---

## 📌 Author

Built by Venkateswarlu Yelchuri

---
