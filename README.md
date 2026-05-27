# 🏪 Real-Time Retail Intelligence Hub
**Microsoft Fabric End-to-End Portfolio Project**

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-DP--600-0078D4?style=flat&logo=microsoft)](https://learn.microsoft.com/en-us/fabric/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> A production-grade analytics solution combining incremental data migration from on-premises SQL Server with real-time IoT event streaming — built entirely on Microsoft Fabric.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     TRACK A — BATCH MIGRATION                   │
│                                                                 │
│  SQL Server          On-Premises       Microsoft Fabric         │
│  AdventureWorks ───► Gateway      ───► Data Factory Pipeline    │
│  (SQLEXPRESS)        (Basic Auth)      (Watermark Incremental)  │
│                                              │                  │
│                                         Bronze Layer            │
│                                    (Files/bronze/*.parquet)     │
│                                              │                  │
│                                       Spark Notebook            │
│                                    (Small_Spark_Env)            │
│                                              │                  │
│                                    Gold Delta Tables            │
│                               gold_fact_sales │ gold_dim_date   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                   TRACK B — REAL-TIME STREAMING                 │
│                                                                 │
│  Bicycles Sample ───► Eventstream ───► Eventhouse (KQL DB)      │
│  (IoT Events)         es_pos_         eh_retail_intelligence    │
│                       transactions         │                    │
│                                       bike_events table         │
│                                      (574,320+ rows)            │
│                                            │                    │
│                                    Real-Time Dashboard          │
│                             (3 KQL tiles, auto-refresh)         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      SEMANTIC LAYER & REPORTING                 │
│                                                                 │
│  gold_fact_sales ───► Semantic Model ───► Power BI Report       │
│  gold_dim_date        (Direct Lake)       (5 visuals + RLS)     │
│                       sm_retail_          rpt_retail_           │
│                       intelligence        intelligence          │
└─────────────────────────────────────────────────────────────────┘
```

---

## ✅ What Was Built

### Track A — Incremental Batch Migration
| Component | Detail |
|-----------|--------|
| Source | AdventureWorks2022 on SQL Server Express (`DESKTOP-HEUL5IB\SQLEXPRESS`) |
| Gateway | On-premises data gateway with Basic (SQL) auth |
| Pipeline | `pl_adventureworks_incremental` — watermark-based on `ModifiedDate` |
| Bronze layer | `Files/bronze/sales_incremental.parquet` — 121,317 rows |
| Watermark table | Maintained via Dataflow Gen2 in `retail_lh` Tables |
| Gold layer | `gold_fact_sales` (121,317 rows) + `gold_dim_date` (1,124 rows) via Spark |
| Spark env | Custom `Small_Spark_Env` — required to avoid SA North 430 throttling errors |

### Track B — Real-Time Streaming
| Component | Detail |
|-----------|--------|
| Source | Bicycles sample data (urban mobility / IoT simulation) |
| Eventstream | `es_pos_transactions` |
| Eventhouse | `eh_retail_intelligence` → `bike_events` table (574,320+ rows) |
| KQL Querysets | `qs_availability_trend`, `qs_top_stations`, `qs_availability_change` |
| Dashboard | `Real-Time Bike Intelligence Dashboard` — 3 tiles, auto-refresh enabled |

### Semantic Model & Reporting
| Component | Detail |
|-----------|--------|
| Semantic model | `sm_retail_intelligence` — Direct Lake mode |
| Relationship | `gold_fact_sales[OrderDate]` → `gold_dim_date[date]` (many-to-one) |
| DAX measures | `Total Revenue`, `Revenue YTD`, `MoM Growth %`, `Top Product` |
| Power BI report | `rpt_retail_intelligence` — 5 visuals, Executive theme |
| RLS | `RegionManager` role — `[TerritoryID] = 6` filter on `gold_fact_sales` |

---

## 📁 Repo Structure

```
retail-intelligence-fabric/
├── pipelines/
│   └── pl_adventureworks_incremental.json   # Data Factory pipeline export
├── notebooks/
│   └── gold_layer_transform.ipynb           # Spark medallion transformation
├── kql/
│   ├── availability_trend.kql
│   ├── top_stations.kql
│   └── availability_change.kql
├── screenshots/
│   └── dashboard.png                        # Real-Time Dashboard screenshot
└── README.md
```

---

## 🔑 Key Technical Decisions

**Why watermark-based incremental load?**
Full table scans on AdventureWorks at scale would be inefficient. Watermarking on `ModifiedDate` ensures only changed rows are pulled on each pipeline run — production-ready pattern for large OLTP sources.

**Why Dataflow Gen2 for the watermark table (not Spark)?**
The watermark control table is a small metadata store with simple read/write semantics. Dataflow Gen2 handles this with low overhead; Spark would be overkill and add environment startup latency.

**Why custom Spark environment (`Small_Spark_Env`)?**
The South Africa North Fabric node experiences HTTP 430 (Too Many Requests) errors during peak hours under the default Spark pool. A dedicated custom environment with pinned runtime settings resolved this reliably.

**Why Direct Lake mode for the semantic model?**
Direct Lake reads delta parquet files directly from the Lakehouse without import or DirectQuery overhead — delivering near-real-time data freshness at in-memory query speed. Ideal for gold delta tables.

**Why Basic auth on the gateway (not Windows auth)?**
The on-premises SQL Server uses a dedicated `fabricuser` SQL login. Windows auth would require domain trust between the gateway machine and Fabric service principal — unnecessary complexity for a dev environment.

---

## 📊 KQL Queries

### Availability Trend
```kql
bike_events
| where ingestion_time() > ago(1h)
| summarize avg_available = avg(todouble(num_bikes_available)) by bin(ingestion_time(), 5m)
| render timechart
```

### Top Stations by Activity
```kql
bike_events
| summarize event_count = count() by station_id
| top 10 by event_count desc
```

### Availability Change Detection
```kql
bike_events
| where ingestion_time() > ago(30m)
| summarize latest = arg_max(ingestion_time(), num_bikes_available) by station_id
| where todouble(num_bikes_available) < 2
| project station_id, num_bikes_available
```

---

## 🛠️ How to Reproduce

1. Restore `AdventureWorks2022` on a local SQL Server Express instance
2. Install Microsoft On-Premises Data Gateway; configure with Basic SQL auth
3. Create a Fabric workspace and Lakehouse (`retail_lh`)
4. Import the pipeline JSON from `/pipelines/` and configure gateway connection
5. Run the pipeline — parquet lands in `Files/bronze/`
6. Run the Spark notebook from `/notebooks/` using a custom Spark environment
7. Create Eventstream with Bicycles sample → route to Eventhouse
8. Build semantic model in Direct Lake mode on gold delta tables
9. Add DAX measures and RLS role as documented above
10. Build Power BI report from semantic model

---

## 🎥 Walkthrough Video
(https://www.loom.com/share/d2323d70b04d419d9c268160067dfa53)

---

## 👤 Author
**Gilbert Kiptoo Lelon** | DP-600 Certified | Nairobi, Kenya
[LinkedIn](https://linkedin.com/in/gilbertkiptoo/) · [Portfolio](https://lelongilbert7-gif.github.io) · [BluePeak Analytics](https://github.com/lelongilbert7-gif)
