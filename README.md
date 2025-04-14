
# 📦 Capstone Sales Data Engineering Project

## 📝 Project Overview

This project is designed to automate the ingestion, validation, and processing of order-related data from multiple third-party sources using **Azure Data Factory**, **Databricks**, and **Azure SQL Database**. The primary business requirement is to generate insights about:

> ✅ **How many orders are placed by each customer and how much they have spent.**

---

## 🚀 End-to-End Workflow

1. **Order Data Ingestion**
   - A third-party system ingest order related `.csv` files daily in the **ADLS Gen2** landing zone.

2. **Trigger Execution**
   - Arrival of new files triggers an **Azure Data Factory pipeline** and dynamically filename will pass to Pipeline from trigger Body.

3. **Copy Activity**
   - The pipeline copies `order_items` data (in `json` format) from **Amazon S3** to **ADLS Gen2** (`DelimitedText` format).

4. **Data Validation in Databricks**
   - Validates the CSV orders file:
     - ✅ **No duplicate `order_id`s**
     - ✅ **`order_status` must be valid** (checked dynamically against a SQL table)
   - Based on validation:
     - ✅ Passed → moved to `staging`
     - ❌ Failed → moved to `discarded`

5. **Final Reporting**
   - Joins data from:
     - `orders` (from ADLS Gen2)
     - `order_items` (from Amazon S3)
     - `customers` (from Azure SQL DB)
   - Generates customer-wise report.

---

## 📊 Final Output: Business Metrics

**Objective:**  
Generate a customer-wise report with total number of orders and total amount spent.

**SQL Query Used:**
```sql
SELECT
  customers.customer_id,
  customers.customer_fname,
  customers.customer_lname,
  customers.customer_city,
  customers.customer_state,
  customers.customer_zipcode,
  COUNT(order_id) AS num_orders_placed,
  ROUND(SUM(order_items.order_item_subtotal), 2) AS total_amount_spent
FROM
  customers,
  stagingorders,
  order_items
WHERE
  customers.customer_id = stagingorders.customer_id
  AND stagingorders.order_id = order_items.order_item_order_id
GROUP BY
  customers.customer_id, customers.customer_fname, customers.customer_lname,
  customers.customer_city, customers.customer_state, customers.customer_zipcode
ORDER BY
  num_orders_placed DESC;
```

---

## 🏗️ Azure Architecture & Components

### 📁 ADLS Gen2 Storage

- **Storage Account:** `cpsalesstorage`
- **Container:** `sales`
- **Folders:** `landing`, `staging`, `discarded`, `order_items`

### 🔄 Azure Data Factory

- **Name:** `capstone-sales-project-DF`
- **Pipeline:** `pl_datavalidation_cp`
  - Activities:
    - `copydatafromS3toADLS`
    - `validationNotebook_cp`
- **Trigger:** `adls_trigger_cp`

### 🔒 Azure Key Vault

- **Name:** `cp-sales-secretekey`
- **Secrets:**
  - `storagesecrete` – ADLS Storage Key
  - `SQLcredentials` – SQL Authentication
  - `databrickssecrete` – Databricks Token

---

## 🧬 Linked Services & Datasets

| Type             | Name                      | Format        |
|------------------|---------------------------|---------------|
| ADLS Gen2        | `ls_ADLSGen2_cp`          | CSV           |
| Amazon S3        | `ls_AmazonS3_cp`          | JSON          |
| Azure Databricks | `ls_AzureDatabricks_cp`   | N/A           |
| Azure SQL DB     | `ls_AzureSqlDatabase_cp`  | Tabular       |
| Key Vault        | `ls_AzureKeyVault_cp`     | Secrets Mgmt  |

## 🔎 Databricks Integration

- **Workspace:** `capstone-sales-project-databricks-ws`
- **Cluster:** `cp-databricks-cluster`
- **Notebook Name:** `cp-sales-logic-notebook`
- **Notebook Pipeline:** `pl_databrick_notebook_cp`
- **Scope Name:** `DatabricksScope`

---

## 🧾 Database Schema

### 1. `order_items`
| Column Name              |
|--------------------------|
| order_item_id            |
| order_item_order_id      |
| order_item_product_id    |
| order_item_quantity      |
| order_item_subtotal      |
| order_item_product_price |

### 2. `customers`
| Column Name         |
|---------------------|
| customer_id         |
| customer_fname      |
| customer_lname      |
| customer_email      |
| customer_password   |
| customer_street     |
| customer_city       |
| customer_state      |
| customer_zipcode    |

### 3. `stagingorders`
| Column Name     |
|-----------------|
| order_id        |
| order_date      |
| customer_id     |
| order_status    |

### 4. `valid_order_status`
- Stores dynamically changing valid statuses such as:
  - `ON HOLD`, `PAYMENT_REVIEW`, `PROCESSING`, `CLOSED`, `SUSPECTED_FRAUD`, `COMPLETE`, `PENDING`, `CANCELED`, `PENDING_PAYMENT`

---

## 🔐 Secure JDBC Connection

```python
url = "jdbc:sqlserver://{}.database.windows.net:{};database={};user={};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(
  dbServer, dbPort, dbName, dbUser
)
```

---

## 🧩 Folder Structure

```bash
/sales
│
├── landing/         # Raw order files from 3rd-party (.csv)
├── order_items/     # JSON data from Amazon S3 converted to .csv
├── staging/         # Validated files
└── discarded/       # Invalid/rejected files
```

---
**As this represents my first data engineering project, I have not yet incorporated advanced optimization techniques, particularly in the areas of PySpark performance tuning 🐍, pipeline efficiency 🚀, and query optimization ⚡. I welcome any constructive feedback or expert recommendations to further enhance the scalability, performance, and overall efficiency of the pipeline 🔧.**
