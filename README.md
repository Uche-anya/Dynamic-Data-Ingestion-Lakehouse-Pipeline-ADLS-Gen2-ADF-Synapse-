# Dynamic-Data-Ingestion-Lakehouse-Pipeline-ADLS-Gen2-ADF-Synapse

# 🔁 Dynamic Data Ingestion & Lakehouse Pipeline (GitHub → ADLS Gen2 → Synapse)

This project implements a **modern data lakehouse pipeline** on Azure, ingesting raw CSV files from a public GitHub repository into **Azure Data Lake Storage Gen2 (Bronze)**, transforming them using **Azure Databricks (Silver)**, and exposing curated datasets as **external tables and views in Synapse SQL (Gold)** for analytics and BI tools like Power BI.

---

## 🔧 Technologies Used

| Tool/Service           | Role                                          |
|------------------------|-----------------------------------------------|
| Azure Data Factory     | Dynamic ingestion from GitHub to ADLS Bronze  |
| Azure Data Lake Gen2   | Storage for Bronze, Silver, Gold zones        |
| Azure Databricks       | Data transformation and Parquet conversion    |
| Azure Synapse SQL      | External tables over Silver/Gold Parquet data |
| Parquet + Snappy       | Optimized storage format                      |

---

## ⚙️ Step-by-Step Pipeline

### 🔹 Step 1: Ingest from GitHub → Bronze (ADF)

- Pipeline: `dynamic-pipeline-git-raw`
- Uses `Lookup`, `ForEach`, and `Copy Data` to dynamically pull data from a GitHub repo.

**📄 JSON File Example:**
```json
[
  {
    "p_rel_url": "Uche-anya/Dynamic-Data-Ingestion-Lakehouse-Pipeline-ADLS-Gen2-ADF-Synapse-/main/dataset/AdventureWorkProduct_Categories.csv",  
    "p_sink_folder": "AdventureWorks_Product_Categories",
    "p_sink_file": "AdventureWorks_Product_Categories.csv"
  },
]
``` 
**Destination Path:** `abfss://bronze@<your_storage_account>.dfs.core.windows.net/<tableName>/`


### 🔹 Step 2: Bronze → Silver Transformations (Databricks)

- Authenticates using **Service Principal (OAuth config)**.
- Reads raw **CSV files** from the **Bronze** layer.
- Applies transformations like:
  - Date parsing
  - Column renaming
  - Full name creation
  - SKU formatting
- Writes results as **Parquet** into the **Silver** layer.

**📁 Silver Output Path:** `abfss://silver@<your_storage_account>.dfs.core.windows.net/<tableName>/`


### 🔹 Step 3: Silver → Gold External Tables in Synapse SQL


### 📊 Optional Enhancements

- [ ] Add partitioning by year/month in Silver
- [ ] Add data quality checks
- [ ] Integrate Power BI dashboard
- [ ] Automate deployment with GitHub Actions


### 🛡️ Security Notes

- Store secrets (client IDs, passwords) in **Azure Key Vault** or **Databricks Secret Scopes**.
- **Never hardcode credentials** in notebooks or scripts.
- Always use **Managed Identity** where possible for secure authentication.


---


---

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.







