# CommonShare Supplier Sustainability Data Engineering Project

**Author:** Ali Rahiqi  
**Date:** 22/10/2025  
**Tools & Tech:** PySpark, MinIO, Dremio, Nessie, Iceberg, Parquet, SQL , Airflow , Superset 

---

## 1 Project Overview

This project ingests, cleans, and models supplier sustainability data for **CommonShare** using a Lakehouse architecture with MinIO, Dremio, Nessie, and Iceberg.  

**Source data:**  
- `companies.csv` → company master data  
- `certifications.csv` → certification standards  
- `company_certificates.csv` → certificate assignments to companies  

**Objectives:**  
- Clean and standardize data  
- Build dimensional model for analysis  
- Ensure data quality and governance  

---

## 2 Data Ingestion & Cleaning

**Steps performed:**  
1. Load all three CSVs into Lakehouse .  
2. Remove records with null `name` (company) or `certificate_id`.  
3. Normalize `country` codes to ISO2.  
4. Derive `active_status` column: `True` if `valid_to >= today`.  

**Key trade-offs:**  
- PySpark was chosen for scalability with daily incoming files.  

---

## 3 Dimensional Modeling

**Tables created:**  

| Table | Description | Keys | Derived Columns |
|-------|------------|------|----------------|
| **DimCompany** | Company master | `company_id` (PK), `record_id` (surrogate) | N/A |
| **DimCertificate** | Certification master | `certification_id` (PK), `record_id` | `validity_days` = DATEDIFF(`valid_to`, `valid_from`) |
| **FactCompanyCertificate** | Linking table | `record_id` (PK), `company_id` (FK), `certification_id` (FK) | `active_status` |

- Surrogate keys ensure stable joins.  
- Fact table stores references to dimensions and includes `active_status` for metrics.  

---

### 3.1 Visual Schema of Dimensions and Fact Table

      +-----------------+           +-------------------+
      |   DimCompany    |           |  DimCertificate   |
      |-----------------|           |------------------|
      | record_id (PK)  |           | record_id (PK)   |
      | company_id      |           | certification_id |
      | name            |           | standard_name    |
      | website         |           | issuing_body     |
      | country         |           | valid_from       |
      | industry        |           | valid_to         |
      | parent_id       |           | validity_days    |
      +--------+--------+           +---------+--------+
               |                              |
               |                              |
               +-------------+----------------+
                             |
                             v
                   +--------------------------+
                   | FactCompanyCertificate   |
                   |--------------------------|
                   | record_id (PK)           |
                   | company_id (FK)          |
                   | certification_id (FK)    |
                   | facility_location        |
                   | scope                    |
                   | status                   |
                   | active_status            |
                   +--------------------------+

---

## 4 ERP / Problem-Solving Context

**Scenario:**  
During integration with a partner ERP (Dynamics 365 F&O), some facility records from `company_certificates` show mismatched parent companies.  

**Tasks & Solutions:**  

**1. Potential causes:**  
- **Data lineage issue:** incorrect or missing `parent_id`.  
- **Business logic mismatch:** parent company inactive or removed in ERP, but old certificates still reference it.  

**2. Validation query (SQL/PySpark):**  

```sql
SELECT cc.company_id, cc.certificate_id, cc.facility_location
FROM company_certificates cc
LEFT JOIN companies c
    ON cc.company_id = c.company_id
WHERE c.company_id IS NULL
   OR c.active_status = FALSE;
````

### 3 Automation Proposal

- Implement a **data quality rule** within the Fabric pipeline to validate parent-child relationships and active status.  
- Schedule **daily validation** using **Azure DevOps CI/CD** pipelines.  
- Automatically generate alerts for any **mismatched or inactive records**.  

---

## 4 Governance & Optimization

- **Schema consistency:** Enforced through Iceberg table definitions and strict type checks.  
- **Data quality:** Null checks, ISO2 country code normalization, and derived `active_status` for accurate reporting.  
