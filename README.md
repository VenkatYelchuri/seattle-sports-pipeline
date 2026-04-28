# Seattle Community Sports Pipeline

Seattle community centers publish drop-in sports schedules across many separate center pages, often with inconsistent formatting, changing seasonal windows, and little support for unified search. This project turns that fragmented information into a usable product: a quarterly data pipeline, a searchable API, and a Streamlit dashboard for discovering drop-in programs across Seattle.

The goal is not just to scrape pages once. The goal is to build a repeatable system that can:
- discover all relevant community centers
- scrape and normalize messy schedule data
- preserve historical snapshots over time
- support user-facing search and filtering
- collect data-quality feedback from end users

## Product Problem

### Real-world use case

A Seattle resident wants to find drop-in badminton, basketball, volleyball, or youth open gym opportunities without opening dozens of separate community center pages. The resident also wants to know:
- what is available today
- which centers offer a given activity
- whether a program belongs to the current seasonal schedule
- whether the information is still correct

On the data side, a product owner or analyst wants:
- a repeatable way to collect schedules every quarter
- historical data retention instead of overwriting prior runs
- a way to trace user-reported issues back to the underlying dataset

## What This Project Does

The system currently:

1. Downloads Seattle community center source data and filters to relevant centers with websites.
2. Scrapes center schedule pages and stores raw HTML.
3. Parses program rows from those pages and normalizes days, times, and age ranges.
4. Extracts season metadata such as `Spring 2026: April 1 - June 21`.
5. Loads cleaned data into PostgreSQL with quarterly snapshot history.
6. Serves the data through FastAPI endpoints.
7. Displays the data in a Streamlit dashboard with search, map views, tiles, schedules, and issue reporting.
8. Stores report metadata so user feedback can be tied back to program-level and snapshot-level records.

## Architecture

### High-level flow

```text
Seattle Open Data / Center Websites
        |
        v
fetch_centers.py
        |
        v
run_scraper.py / scraper.py
        |
        v
raw HTML files
        |
        v
parser.py + transform.py
        |
        v
programs_cleaned.csv
        |
        v
load_to_db.py
        |
        +--> snapshots table
        +--> programs table
        +--> reports table
        |
        v
FastAPI
        |
        v
Streamlit dashboard
```

### Repository structure

```text
seattle-sports-pipeline/
├── api/
│   └── main.py
├── dashboard/
│   └── app.py
├── data/
│   ├── raw/
│   │   └── html/
│   └── processed/
├── db/
├── logs/
├── pipeline/
│   ├── load_to_db.py
│   ├── logger.py
│   ├── orchestrator.py
│   ├── run_parser.py
│   └── transform.py
├── scraper/
│   ├── fetch_centers.py
│   ├── parser.py
│   ├── run_scraper.py
│   └── scraper.py
├── .env.example
├── requirements.txt
└── README.md
```

## Data Model

### `programs`

Stores normalized drop-in sessions such as:
- center
- program
- day_of_week
- start_time
- end_time
- age_min
- age_max
- snapshot_id
- quarter
- year
- scraped_at
- `program_uid`
- `session_uid`

### `snapshots`

Stores one snapshot per center per quarter so the product can preserve published seasonal context:
- center
- season_label
- season_name
- season_start_date
- season_end_date
- quarter
- year
- scraped_at

This separates "what schedule period the center published" from "what quarter the pipeline ran".

### `reports`

Stores user-submitted issue reports and now supports contextual fields such as:
- center
- program
- issue_type
- description
- program_uid
- session_uid
- snapshot_id
- quarter
- year

This makes reports more useful for future triage, admin tooling, and data correction workflows.

## Key Challenges

### 1. Source data is fragmented

Each community center can publish its schedule differently. Some pages contain current schedules, old schedules, navigation links, or future schedule menus on the same page.

### 2. HTML is messy and inconsistent

Program rows may appear in different table structures, with inconsistent time formats, age formats, and day abbreviations.

### 3. Seasonal schedule labels do not map cleanly to calendar quarters

Example:
- `Spring 2026: April 1 - June 21`

This is not equivalent to the full quarter `2026-Q2`, so relying only on quarter labels would lose important context.

### 4. Repeat pipeline runs must be safe

Appending the same quarter repeatedly caused duplicate-key conflicts when the uniqueness model in the database did not align with the intended quarterly snapshot behavior.

### 5. User-reported issues need traceability

A product-quality feedback form is only truly useful if reports can be tied back to the program data that generated them.

### 6. Streamlit interaction state can be tricky

Interactive dashboards built in Streamlit can have state timing issues:
- dialogs opening one rerun too late
- selected schedule views sticking around after filters change
- forms not clearing cleanly after submit

## Setbacks Encountered and Solutions Implemented

### Duplicate key failures during DB load

#### Problem

The loader originally appended rows directly into `programs`, and rerunning a quarter caused conflicts against an existing uniqueness rule.

#### Solution

- Added a quarterly-aware uniqueness strategy.
- Replaced current-quarter data before reloading.
- Added a `snapshots` table so quarter-level and season-level history can coexist.

### Seasonal metadata was getting lost

#### Problem

The original parser captured program rows but did not preserve the schedule label shown on the page.

#### Solution

- Extended the parser to extract season labels and date windows.
- Propagated `season_label`, `season_name`, `season_start_date`, and `season_end_date` into the load path.
- Stored them in `snapshots`.

### No stable identity for tiles and reports

#### Problem

The UI had program tiles and report forms, but there was no durable link between a user’s report and the underlying data.

#### Solution

- Added `program_uid` for program-level grouping.
- Added `session_uid` for row-level identity.
- Extended the reports payload and schema to store contextual identifiers.

### FastAPI assumptions did not match legacy schema

#### Problem

The API briefly assumed columns such as `programs.id` existed in the current database when they did not.

#### Solution

- Made queries more backward-compatible.
- Added schema-healing behavior on API startup for newer report and identity fields.

### Report form behavior in Streamlit

#### Problems

- Tile dialog did not open on first click.
- Bottom report form threw widget-state exceptions after submit.
- Dialogs/forms did not always close cleanly after success.

#### Solutions

- Forced rerun after setting dialog-open state.
- Changed inline form clearing to happen safely on the next rerun.
- Cleared form state after successful submission.
- Closed dialogs cleanly after submit.

### Dashboard usability issues

#### Problems

- Program Tiles became cluttered.
- Schedule details stayed visible after filters changed.
- Map interaction could interfere with page flow.
- Default map zoom did not always show all centers.

#### Solutions

- Added tile pagination.
- Reset selected program detail when filters change.
- Moved the map behind a `Show map` toggle.
- Increased map height for a more vertical experience.
- Calculated a fitted default map viewport based on visible center bounds.

## Current Dashboard Features

The dashboard now includes:
- quarter selector
- search and filter controls
- tile pagination
- program tiles with per-tile issue reporting
- today’s programs summary
- grouped schedule view
- simple and density map views
- bottom global report form
- state resets for cleaner interaction behavior

## API

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/programs` | List programs with optional filters for `program`, `center`, `day`, and `quarter` |
| GET | `/quarters` | List available quarters in the database |
| POST | `/reports` | Submit a data issue or missing program report |
| GET | `/reports` | List existing reports |
| GET | `/health` | Health check |

### Example use cases

- power the Streamlit dashboard
- query current-quarter programs
- analyze quarter-over-quarter schedule changes
- review user-submitted issues with attached identifiers

## Orchestration

The pipeline is orchestrated through Prefect in [pipeline/orchestrator.py].

The scheduled flow:
- fetches centers
- scrapes websites
- parses HTML
- loads into PostgreSQL

The intended cadence is quarterly:
- January 1
- April 1
- July 1
- October 1

Timezone:
- `America/Los_Angeles`

## Local Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Create `.env` from `.env.example` and set:

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/sports
```

If the password contains special characters like `@`, encode them in the URL. Example:

```env
DATABASE_URL=postgresql://postgres:YOUR_ENCODED_PASSWORD@localhost:5432/sports
```

### 3. Run the pipeline

Full pipeline:

```powershell
.\venv\Scripts\python.exe -m pipeline.orchestrator
```

Step-by-step:

```powershell
.\venv\Scripts\python.exe -m scraper.fetch_centers
.\venv\Scripts\python.exe -m scraper.run_scraper
.\venv\Scripts\python.exe -m pipeline.run_parser
.\venv\Scripts\python.exe -m pipeline.load_to_db
```

### 4. Start the API

```powershell
.\venv\Scripts\uvicorn.exe api.main:app --reload
```

### 5. Start the dashboard

```powershell
.\venv\Scripts\streamlit.exe run dashboard/app.py
```

## What Has Been Built So Far

### Data ingestion

- center discovery from Seattle source data
- HTML scraping per center
- raw HTML retention for debugging and reprocessing

### Data normalization

- day expansion such as `Mon-Thur` to full weekday rows
- time normalization across inconsistent formats
- age normalization into minimum/maximum structure
- deduplication before DB load

### Historical storage

- quarterly snapshots
- season metadata retention
- safe reruns for the same quarter

### Product layer

- searchable API
- interactive Streamlit dashboard
- map view, tile view, today view, and grouped schedule view
- user report capture tied to record identifiers

## Remaining Gaps / Future Work

Some realistic next product steps:

- add admin tooling for reviewing and resolving reports
- add screenshot assets to the README
- improve mobile responsiveness further
- support richer historical comparisons between quarters
- introduce automated tests for parser edge cases and API schema compatibility
- add data-quality dashboards or anomaly detection
- move from Streamlit to a custom frontend if deeper device responsiveness is needed

## Why This Matters

This project is more than a scraper. It is a small location-based civic sports discovery product backed by a repeatable data pipeline. The main challenge is not simply collecting rows from HTML. The challenge is building a stable product around unstable source data:
- preserving season context
- supporting historical runs
- preventing duplicate loads
- keeping the UI usable
- connecting user feedback back to the data model

That is the core value of this repository.


