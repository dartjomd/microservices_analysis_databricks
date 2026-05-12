# Production-Grade Data Lakehouse Orchestration with dbt and Databricks

### Project Overview
This repository contains a high-performance Data Lakehouse implementation built on Databricks (Unity Catalog) and dbt. The project features a **metadata-driven ingestion engine** and a sophisticated CI/CD ecosystem designed to balance rigorous data quality with cloud cost optimization.

### Core Architecture
The project implements the **Medallion Architecture**, ensuring a structured and scalable data evolution:
* **Bronze (Raw):** Metadata-driven ingestion from S3 buckets using `COPY INTO`. Orchestrated via custom dbt macros for seamless scalability.
* **Silver (Cleansed):** Cleansed and conformed tables with enforced schema validation, dbt tests, and **incremental materialization**.
* **Gold (Curated):** High-level business aggregates and health metrics (e.g., Service Success Rate, Latency) optimized for BI consumption.

---

### Technical Deep Dive: Engineering Excellence

#### 1. Metadata-Driven Ingestion (Orchestrator Pattern)
Unlike hardcoded loading scripts, this project uses a custom-built ingestion engine:
* **Universal Macro:** A single entry point handles JSON/CSV formats, dynamic schema creation, and `COPY INTO` execution.
* **Centralized Metadata:** New S3 sources are integrated by updating a master macro configuration, reducing manual SQL boilerplate.
* **Lineage Integrity:** Seamlessly integrated with `sources.yml` to maintain full visibility from S3 objects to final metrics.

#### 2. Slim CI with Manifest Deferral
To optimize GitHub Actions execution time and Databricks DBU consumption:
* **State Management:** Artifacts (`manifest.json`) are cached to identify modified models via the `state:modified+` selector.
* **Production Deferral:** The CI environment uses the `--defer` flag to reference upstream production tables directly, eliminating the need for full data duplication.
* **Namespace Isolation:** Each PR runs in an ephemeral schema (`ci_schema_${run_id}`), which is automatically dropped post-execution.

#### 3. Security & Access Management (Unity Catalog)
The project utilizes a modern **Zero-Trust** security model for automated operations:
* **M2M Authentication:** Automated workflows interact with Databricks via **OAuth 2.0 Client Credentials**. This eliminates the use of long-lived Personal Access Tokens (PATs).
* **Service Principal Governance:** All CI/CD operations are executed by a dedicated `github_actions_sp`.
* **Granular Permissions:**
    * **External Locations:** The Service Principal is granted `READ FILES` on specific S3 paths to execute `COPY INTO` через Unity Catalog.
    * **Catalog Ownership:** Managed through an ownership-based model in Unity Catalog, allowing the Service Principal to autonomously manage the lifecycle of CI schemas.
    * **Cross-Catalog Access:** The CI bot is granted `USE CATALOG` and `SELECT` privileges on Production to support **Slim CI Deferral** logic without compromising data integrity.

#### 4. Performance & Cost Optimization
* **Incremental Materialization:** Silver layer models utilize the `merge` strategy to process only new delta changes, significantly lowering compute costs.
* **CI Data Sampling:** Custom Jinja logic limits data volume during CI runs (e.g., `maxFiles = 5`), ensuring rapid testing cycles.

#### 5. Automated Governance & Observability
* **Data Quality Gates:** Blocking tests (uniqueness, `accepted_range`, and custom business logic) ensure only high-quality code reaches `main`.
* **Automated Documentation:** The CI pipeline automatically generates and deploys dbt documentation to **GitHub Pages**.
* **SQL Linting:** Enforcement of code standards via **SQLFluff** with dbt-templater integration.
* **Real-time Alerting:** Integration with a **Telegram Bot** for instant build status notifications.

#### 6. Operational Observability & FinOps Layer
To maintain full transparency over pipeline health and cloud expenditures, the project includes a custom-built monitoring solution:
* **Audit Logging Engine:** A dedicated Silver-layer model (`fct_dbt_audit_logs`) that captures granular execution metadata by incrementally ingesting Databricks system tables (`system.query.history`).
* **Performance Analytics (Gold Mart):** A sophisticated analytical mart (`mart_dbt_performance`) that provides a 360-degree view of the Lakehouse performance.
    * **Advanced RegEx Parsing:** Dynamically extracts full 3-tier model names (catalog.schema.table) from raw SQL statements to reconstruct the lineage of every execution.
    * **Deduplication Logic:** Intelligently merges metrics from dbt's internal temporary tables (`__dbt_tmp`) into their parent models for accurate "true" execution tracking.
    * **Noise Filtering:** Implements a "Production-Only" filter to exclude ephemeral CI/CD schemas and developer noise, focusing strictly on business-critical assets.
* **Resource Governance:** Allows for real-time identification of "expensive" models, enabling proactive optimization (FinOps) and ensuring adherence to internal SLAs.

---

### Technical Stack
* **Storage & Compute:** Databricks (Unity Catalog), Delta Lake, AWS S3
* **Transformation:** dbt-core (v1.11+)
* **Monitoring & Observability:** Databricks System Tables, Regex-based Log Parsing, Databricks SQL Dashboards
* **Authentication:** OAuth 2.0 (Service Principals)
* **Orchestration & CI/CD:** GitHub Actions
* **Quality Control:** SQLFluff, dbt-tests, dbt-utils
* **Documentation:** GitHub Pages
* **Alerting:** Telegram Bot API

---

### Pipeline Workflow
1.  **Code Push:** Triggers the GitHub Action workflow.
2.  **Linting:** **SQLFluff** validates SQL syntax and style using a `dummy` token to bypass live DB connection for maximum speed.
3.  **Metadata Ingestion:** `dbt run-operation load_all_sources --target ci` prepares the Bronze layer in an isolated environment.
4.  **Slim Build & Test:** `dbt build --select state:modified+ --defer --target ci` processes changes using OAuth-backed authentication.
5.  **Source Freshness:** Validates SLA for upstream S3 sources using the `loaded_at_field` metadata.
6.  **Docs Deployment:** Upon success, dbt documentation is updated and pushed to GitHub Pages.
7.  **Cleanup:** Ephemeral CI schemas are dropped via `dbt run-operation drop_ci_schema --target ci` to maintain workspace hygiene.

---

### Project Impact
* **Efficiency:** ~80% reduction in CI execution time through Slim CI and data sampling.
* **Security:** Transitioned from legacy PATs to **OAuth 2.0**, significantly reducing the risk of credential leakage.
* **Scalability:** Integration of new S3 sources takes minutes due to the macro-orchestrator.
* **Reliability:** 100% visibility into data lineage and automated quality enforcement.