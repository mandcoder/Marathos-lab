# Marathos

A data platform and pipeline project built on Databricks, focused on Swedish hosted ultra marathon races.

## Scope

This project focuses on building a data platform and pipeline for Marathos, with the analysis and dashboard scoped to Swedish hosted ultra marathon races. The Swedish races dataset provides a well-defined and manageable subset of the global data, making it suitable for demonstrating the pipeline capabilities and business insights.

**In scope:**

* Full ETL pipeline from raw CSV ingestion to gold serving layer
* Data cleaning and validation for the complete global dataset
* Dimensional modeling with fact and dimension tables
* Dashboard and Genie space focused on Swedish hosted races

**Out of scope (time constraints):**

* Deeper analysis of edge cases in the global dataset
* Advanced outlier detection in athlete performance times
* Inconsistent event naming conventions across all countries

## Tech stack

* **Databricks** — Lakehouse platform
* **PySpark** — data transformation
* **Delta Live Tables (DLT)** — declarative pipeline orchestration
* **Unity Catalog** — data governance and cataloging
* **Delta Lake** — storage layer
* **SQL** — modeling and analysis
* **Databricks Dashboard** — data visualization and Genie space

## Architecture

Raw CSV data is ingested and processed through a medallion architecture:

```
Raw CSV → Bronze → Silver → Gold
```

* **Bronze** — raw ingestion of the global ultra marathon dataset
* **Silver** — cleaned and validated data
* **Gold** — dimensional model (fact and dimension tables), serving layer for the dashboard

The pipeline is orchestrated with Delta Live Tables and cataloged in Unity Catalog.

## Repository structure

```
marathos_lab/
├── dimensional_modeling/   # Fact and dimension table definitions
├── explorations/           # Ad-hoc data exploration
├── transformations/        # DLT pipeline transformations
└── utils/                  # Shared helper code
```

## Result

A Databricks Dashboard and Genie space presenting insights on Swedish hosted ultra marathon races, built on top of the gold-layer dimensional model. company called Marathos, which hosts marathons all over the world.
