\# FinTech Analytics Platform



> \*\*End-to-end payment fraud analytics pipeline\*\* built with dbt Core, BigQuery, Python, and GitHub Actions CI/CD.  

> 5M+ synthetic transactions · Medallion architecture · Incremental fact tables · Automated data quality tests



\---



\## 🎥 Demo

<!-- Record a 3-min Loom walkthrough and paste the link here -->

📹 \[Loom Walkthrough](#) | 📊 \[Live Looker Studio Dashboard](#)



\---



\## Architecture



```

Python Generator

(5M transactions)

&#x20;      │

&#x20;      ▼

&#x20; GCS Bucket (raw CSVs)

&#x20;      │

&#x20;      ▼

BigQuery — raw dataset

&#x20;      │

&#x20;      ▼

&#x20; dbt Staging     ◄── source freshness checks, type casting

&#x20;      │

&#x20;      ▼

&#x20; dbt Intermediate ◄── business logic, fraud signals, joins

&#x20;      │

&#x20;      ▼

&#x20; dbt Marts        ◄── incremental fact table, dimensions, summary

&#x20;      │

&#x20;      ▼

Looker Studio Dashboard

```



CI/CD: Every pull request to `main` triggers GitHub Actions → `dbt compile` + `dbt build --select staging` + PR comment with test results.



\---



\## Tech Stack



| Layer | Tool | Notes |

|---|---|---|

| Cloud DWH | BigQuery (GCP free tier) | Partitioned + clustered fact table |

| Transformation | dbt Core 1.8 | Staging / Intermediate / Mart layers |

| Data Generation | Python + Faker | 5M rows, \~2.5GB |

| Orchestration | GitHub Actions | CI on every PR |

| BI | Looker Studio | Native BQ connector |

| Packages | dbt\_utils, audit\_helper | Date spine, range tests |



\---



\## Project Structure



```

fintech-analytics-dbt/

├── data\_generator/

│   └── generate\_data.py          # Synthetic data generator

│

└── dbt\_project/

&#x20;   ├── dbt\_project.yml

&#x20;   ├── packages.yml

&#x20;   ├── models/

&#x20;   │   ├── staging/

&#x20;   │   │   ├── sources.yml        # Source definitions + freshness

&#x20;   │   │   ├── schema.yml         # Column tests + docs

&#x20;   │   │   ├── stg\_transactions.sql

&#x20;   │   │   ├── stg\_customers.sql

&#x20;   │   │   ├── stg\_merchants.sql

&#x20;   │   │   └── stg\_cards.sql

&#x20;   │   ├── intermediate/

&#x20;   │   │   ├── int\_transactions\_enriched.sql

&#x20;   │   │   ├── int\_customer\_spend\_profile.sql

&#x20;   │   │   └── int\_merchant\_risk\_score.sql

&#x20;   │   └── marts/

&#x20;   │       ├── schema.yml

&#x20;   │       ├── fct\_transactions.sql      ← incremental, partitioned, clustered

&#x20;   │       ├── fct\_fraud\_summary.sql     ← powers dashboard

&#x20;   │       ├── dim\_customers.sql

&#x20;   │       └── dim\_merchants.sql

&#x20;   ├── tests/

&#x20;   │   ├── assert\_fraud\_rate\_below\_threshold.sql

&#x20;   │   └── assert\_no\_future\_transactions.sql

&#x20;   └── .github/workflows/

&#x20;       └── dbt\_ci.yml

```



\---



\## Quick Start



\### Prerequisites

\- Python 3.10+

\- GCP account (free tier is sufficient)

\- dbt Core: `pip install dbt-bigquery`

\- GCS bucket + BigQuery project created



\### 1 — Generate synthetic data

```bash

cd data\_generator

pip install faker pandas numpy tqdm

python generate\_data.py

\# Output: ./output/ — 4 CSV files, \~2.5GB total

```



\### 2 — Load to GCS + BigQuery

```bash

\# Upload to GCS

gsutil -m cp output/\*.csv gs://<YOUR\_BUCKET>/raw/



\# Load each table into BigQuery raw dataset

for table in customers merchants cards transactions; do

&#x20; bq load --autodetect --source\_format=CSV \\

&#x20;   <PROJECT\_ID>:raw.$table \\

&#x20;   gs://<YOUR\_BUCKET>/raw/$table.csv

done

```



\### 3 — Configure dbt

```bash

\# Copy profiles.yml to \~/.dbt/ and fill in your GCP project ID

cp dbt\_project/profiles.yml \~/.dbt/profiles.yml



\# Place your service account key at

\~/.dbt/gcp\_sa\_key.json



\# Test connection

cd dbt\_project

dbt debug

```



\### 4 — Run dbt

```bash

dbt deps          # Install packages

dbt run           # Build all models

dbt test          # Run all tests

dbt docs generate # Build documentation site

dbt docs serve    # Open DAG in browser at localhost:8080

```



\---



\## Key Design Decisions



\*\*Why incremental on `fct\_transactions`?\*\*  

At 5M rows, a full refresh scans the entire table on every run. Incremental with a 3-day lookback window processes only new/updated partitions, reducing BigQuery scan costs by \~95% on daily runs.



\*\*Why a separate `fct\_fraud\_summary`?\*\*  

Looker Studio queries run against this pre-aggregated table rather than the 5M-row fact table. Dashboard page loads scan kilobytes instead of gigabytes — a pattern directly applicable to production BI on large datasets.



\*\*Why synthetic data?\*\*  

Fintech data contains PII and is subject to strict data residency regulations. Generating realistic synthetic data with controlled fraud base rates (2%) and behavioural patterns demonstrates domain knowledge while avoiding any compliance concerns — the right instinct for production data engineering.



\*\*Why custom singular tests?\*\*  

`assert\_fraud\_rate\_below\_threshold.sql` catches data drift and pipeline issues that schema-level tests miss. It embeds a business rule (fraud rate should not exceed 5%) directly into the test suite, making the pipeline self-documenting.



\---



\## Business Questions Answered



\- What is the daily fraud rate trend, and is it spiking?

\- Which merchant categories have the highest fraud volume?

\- Which customers have elevated fraud rates by age band and country?

\- What share of fraud transactions are cross-border?

\- Which merchants are statistically high-risk (95th percentile fraud rate)?

\- At what hours of day do fraudulent transactions peak?



\---



\## GitHub Actions CI/CD



On every PR to `main`:

1\. Authenticates to GCP using a service account secret

2\. Runs `dbt compile` — catches all Jinja/SQL syntax errors

3\. Runs `dbt build --select staging` — builds + tests staging layer

4\. Posts a test results summary comment on the PR

5\. Cleans up the ephemeral CI dataset to avoid storage costs



\---



\## Certifications \& Learning Path

\- \[dbt Fundamentals](https://courses.getdbt.com/courses/fundamentals) — free, \~4 hrs

\- \[BigQuery for Data Analysts](https://cloudskillsboost.google/) — free on Google Cloud Skills Boost

\- Google Cloud Professional Data Engineer — recommended next cert



