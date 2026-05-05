# Production-Grade Data Lakehouse Orchestration with dbt and Databricks

### Project Overview
This repository contains a high-performance Data Lakehouse implementation built on Databricks (Unity Catalog) and dbt. The project features a **metadata-driven ingestion engine** and a sophisticated CI/CD ecosystem designed to balance rigorous data quality with cloud cost optimization.

### Core Architecture
The project implements the **Medallion Architecture**, ensuring a structured and scalable data evolution:
*   **Bronze (Raw):** Metadata-driven ingestion from S3 buckets using `COPY INTO`. Orchestrated via custom dbt macros for seamless scalability.
*   **Silver (Cleansed):** Cleansed and conformed tables with enforced schema validation, dbt tests, and **incremental materialization**.
*   **Gold (Curated):** High-level business aggregates and health metrics (e.g., Service Success Rate, Latency) optimized for BI consumption.

---

### Technical Deep Dive: Engineering Excellence

#### 1. Metadata-Driven Ingestion (Orchestrator Pattern)
Unlike hardcoded loading scripts, this project uses a custom-built ingestion engine:
*   **Universal Macro:** A single entry point handles JSON/CSV formats, dynamic schema creation, and `COPY INTO` execution.
*   **Centralized Metadata:** New S3 sources are integrated by updating a master macro configuration, reducing manual SQL boilerplate.
*   **Lineage Integrity:** Seamlessly integrated with `sources.yml` to maintain full visibility from S3 objects to final metrics.

#### 2. Slim CI with Manifest Deferral
To optimize GitHub Actions execution time and Databricks DBU consumption:
*   **State Management:** Artifacts (`manifest.json`) are cached to identify modified models via the `state:modified+` selector.
*   **Production Deferral:** The CI environment uses the `--defer` flag to reference upstream production tables directly, eliminating the need for full data duplication.
*   **Namespace Isolation:** Each PR runs in an ephemeral schema (`ci_schema_${run_id}`), which is automatically dropped post-execution.

#### 3. Performance & Cost Optimization
*   **Incremental Materialization:** Silver layer models utilize the `merge` strategy to process only new delta changes, significantly lowering compute costs.
*   **CI Data Sampling:** Custom Jinja logic limits data volume during CI runs (e.g., `maxFiles = 5`), ensuring rapid testing cycles.

#### 4. Automated Governance & Observability
*   **Data Quality Gates:** Blocking tests (uniqueness, `accepted_range`, and custom business logic) ensure only high-quality code reaches `main`.
*   **Automated Documentation:** The CI pipeline automatically generates and deploys dbt documentation to **GitHub Pages**.
*   **SQL Linting:** Enforcement of code standards via **SQLFluff**.
*   **Real-time Alerting:** Integration with a **Telegram Bot** for instant build status notifications.

---

### Technical Stack
*   **Storage & Compute:** Databricks (Unity Catalog), Delta Lake, AWS S3
*   **Transformation:** dbt-core (v1.11+)
*   **Orchestration & CI/CD:** GitHub Actions
*   **Quality Control:** SQLFluff, dbt-tests, dbt-utils
*   **Documentation:** GitHub Pages
*   **Monitoring:** Telegram Bot API

---

### Pipeline Workflow
1.  **Code Push:** Triggers the GitHub Action workflow.
2.  **Linting:** SQLFluff validates SQL syntax and style.
3.  **Metadata Ingestion:** `dbt run-operation load_all_sources` prepares the Bronze layer.
4.  **Slim Build & Test:** `dbt build --select state:modified+ --defer` processes changes in an isolated CI schema.
5.  **Docs Deployment:** Upon success, dbt documentation is updated and pushed to GitHub Pages.
6.  **Cleanup:** Ephemeral CI schemas are dropped via `dbt run-operation drop_ci_schema`.

---

### Project Impact
*   **Efficiency:** ~80% reduction in CI execution time through Slim CI and data sampling.
*   **Scalability:** Integration of new S3 sources takes minutes due to the macro-orchestrator.
*   **Reliability:** 100% visibility into data lineage and automated quality enforcement.