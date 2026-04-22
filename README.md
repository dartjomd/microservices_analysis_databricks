# Production-Grade Data Lakehouse Orchestration with dbt and Databricks

### Project Overview
This repository contains a high-performance Data Lakehouse implementation built on Databricks (Unity Catalog) and dbt. The core focus of this project is the engineering of a sophisticated CI/CD ecosystem that balances rigorous data quality standards with cloud cost optimization.

### Core Architecture
The project follows the Medallion Architecture (Bronze, Silver, Gold) to ensure structured data evolution and clear lineage:
* **Bronze:** Raw data ingestion with minimal transformation.
* **Silver:** Cleansed and conformed tables with enforced schema validation and dbt tests.
* **Gold:** High-level business aggregates and metrics optimized for BI consumption.

### Technical Deep Dive: Slim CI and Performance Engineering
The standout feature of this repository is the advanced CI/CD pipeline managed via GitHub Actions.

#### Slim CI with Manifest Deferral
To avoid the overhead of rebuilding the entire Lakehouse on every code change, the pipeline utilizes a Slim CI strategy:
* **State Management:** Artifacts (manifest.json) are cached and compared across runs.
* **Incremental Testing:** The pipeline identifies and executes only modified models and their immediate downstream dependencies using the `state:modified+` selector.
* **Namespace Isolation:** Every PR run is executed in a dynamically generated, ephemeral schema (`ci_schema_${run_id}`) to ensure environment isolation and prevent collision.
* **Production Deferral:** Through the `--defer` flag, the CI environment references upstream tables directly from the production `silver` catalog without data duplication, significantly reducing compute costs.

#### Automated Governance and Quality Gates
* **SQL Linting:** Enforcement of code standards via SQLFluff (dbt-templater) to ensure maintainability and readability.
* **Source Freshness:** Automated monitoring of data latency to prevent downstream processing of stale information.
* **Fail-Fast Validation:** A mandatory suite of dbt data tests (uniqueness, nullability, and custom business logic) serves as a blocking gate for all merges.
* **Observability:** Integration with a custom Telegram Alerting Bot provides real-time notifications for pipeline status, execution metadata, and failure logs.

### Technical Stack
* **Compute:** Databricks SQL Warehouses
* **Storage:** Delta Lake (Unity Catalog)
* **Transformation:** dbt-core
* **CI/CD:** GitHub Actions
* **Quality Control:** SQLFluff, dbt-tests
* **Monitoring:** Telegram API

### Pipeline Workflow
1.  **Code Push:** Triggers the GitHub Action workflow.
2.  **Linting Phase:** SQLFluff validates SQL syntax and style.
3.  **State Restoration:** The latest production manifest is retrieved from the cache.
4.  **Slim Build:** dbt builds and tests only the delta changes in an isolated CI schema.
5.  **Post-Build:** Schema cleanup is performed via dbt run-operations.
6.  **Merge:** Upon push to the main branch, a new production-ready manifest is generated and cached for subsequent runs.

---

### Project Impact
* **Optimization:** Reduced CI execution time from minutes to seconds by isolating modified nodes.
* **Security:** Achieved zero-risk deployment through full environment isolation.
* **Reliability:** Eliminated manual testing through 100% automation of data quality and code linting.