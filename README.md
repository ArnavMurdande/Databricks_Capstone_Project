# 🏥 Hospital Patient Journey & Operations Lakehouse

A production-grade Medallion Lakehouse architected on **Azure Databricks** and **Unity Catalog** to ingest, cleanse, model, and analyze patient encounter lifecycles, bed utilization, and clinical turnaround metrics.

---

## 📌 Architecture Highlights
- **Bronze Layer**: Incremental, restart-safe ingestion of encounter micro-batches and event feeds using **Auto Loader (`cloudFiles`)** with schema evolution and metadata audit tracking.
- **Silver Layer**: Data quality validation, quarantine routing for defective/unknown records, deterministic deduplication, state transitions (ADMITTED → TRANSFERRED → DISCHARGED).
- **Gold Layer**: Star schema / conformed dimensional models publishing core hospital operations KPIs: **Length of Stay (LOS)**, **Bed Utilization**, and **Triage Turnaround Times**.
- **Governance & CI/CD**: Governed via **Unity Catalog** least-privilege RBAC, modular Databricks Workflows DAG orchestration, Delta Lake time-travel audits, and Git branch protection.

---

## 🛠️ Tech Stack
- **Compute & Platform**: Azure Databricks, Unity Catalog, Delta Lake
- **Storage & Ingestion**: Azure Data Lake Storage (ADLS) Gen2, Auto Loader, Azure SQL Database
- **Languages & Frameworks**: PySpark, Spark SQL, Python
- **Analytics & Ops**: Databricks Workflows, MLflow, AI/BI Dashboards
