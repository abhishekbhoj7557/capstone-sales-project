
# 🚀 Azure Data Engineering Project: Customer Order Analytics Pipeline

## 📘 Project Overview

This project showcases a real-world end-to-end **Azure Data Engineering** solution that automates the validation, processing, and transformation of **daily order data** from multiple third-party sources. The goal is to generate business insights showing **how many orders are placed and how much is spent by each customer**.

---

## 📂 Data Sources

1. **Order Data (CSV)** – Daily drops by 3rd-party into ADLS Gen2 `landing/` folder.
2. **Order Items (JSON)** – Stored in Amazon S3: `/salescpproject/order_items/order_items.json`
3. **Customers (Table)** – Hosted in Azure SQL Database: `cpdatabase.dbo.customers`

---

## ✅ Business Rules & Data Validation

| Step | Validation | Action |
|------|------------|--------|
| 1️⃣ | No duplicate `order_id` in CSV | If duplicates found → Move to `discarded/` |
| 2️⃣ | `order_status` must match dynamic list of valid statuses | If invalid → Move to `discarded/` |
| ✅ | If both checks pass | Move to `staging/` for further processing |

**Valid statuses (fetched dynamically from DB):**
`ON HOLD, PAYMENT_REVIEW, PROCESSING, CLOSED, SUSPECTED_FRAUD, COMPLETE, PENDING, CANCELED, PENDING_PAYMENT`

---

## 🛠️ Azure & Databricks Resources

### 🔗 Azure Resources

- **Resource Group**: `capstone-sales-project`
- **Storage (ADLS Gen2)**: `cpsalesstorage`  
  ┗ Container: `sales/` with folders: `landing/`, `staging/`, `discarded/`, `order_items/`
- **Data Factory**: `capstone-sales-project-DF`
- **Trigger**: `adls_trigger_cp` (Blob event trigger)

### 🔄 Linked Services

| Service              | Linked Service Name         |
|----------------------|-----------------------------|
| ADLS Gen2            | `ls_ADLSGen2_cp`            |
| Azure Databricks     | `ls_AzureDatabricks_cp`     |
| Azure Key Vault      | `ls_AzureKeyVault_cp`       |
| Amazon S3            | `ls_AmazonS3_cp`            |
| Azure SQL Database   | `ls_AzureSqlDatabase_cp`    |

### 🔐 Key Vault Secrets

- Storage Key: `storagesecrete`
- SQL Credentials: `SQLcredentials`
- Databricks Token: `databrickssecrete`

---

## 🧠 Data Processing Pipeline

### 📈 Pipeline Name: `pl_datavalidation_cp`

- **Trigger**: Executes when new `.csv` file lands in `landing/`
- **Activities**:
  - Check for duplicates
  - Validate order status
  - Route data to `staging/` or `discarded/`

### 📒 Databricks Workspace: `capstone-sales-project-databricks-ws`

| Component                | Name                        |
|--------------------------|-----------------------------|
| Notebook Name            | `cp-sales-logic-notebook`   |
| Logic Pipeline           | `pl_databrick_cp`           |
| Cluster                  | `cp-databricks-cluster`     |
| Scope                    | `DatabricksScope`           |

---

## 🧾 Data Schema

### 1. **order_items**
| Column | Description |
|--------|-------------|
| `order_item_id` | Unique item ID |
| `order_item_order_id` | Foreign key to order |
| `order_item_product_id` | Product ID |
| `order_item_quantity` | Quantity ordered |
| `order_item_subtotal` | Line item subtotal |
| `order_item_product_price` | Unit price |

### 2. **customers**
| Column | Description |
|--------|-------------|
| `customer_id` | Unique customer ID |
| `customer_fname` | First name |
| `customer_lname` | Last name |
| `customer_email` | Email address |
| `customer_city` | City |
| `customer_state` | State |
| `customer_zipcode` | Zip Code |

### 3. **stagingorders**
| Column | Description |
|--------|-------------|
| `order_id` | Order ID |
| `order_date` | Date of order |
| `customer_id` | Foreign key |
| `order_status` | Status (validated) |

---

## 📊 Final Business Output

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
  customers.customer_id = stagingorders.customer_id AND
  stagingorders.order_id = order_items.order_item_order_id
GROUP BY 
  customers.customer_id, 
  customers.customer_fname, 
  customers.customer_lname, 
  customers.customer_city, 
  customers.customer_state, 
  customers.customer_zipcode
ORDER BY 
  num_orders_placed DESC;
```

---

## 📁 Folder Structure

```
azure-data-pipeline/
│
├── datafactory/
│   ├── pl_datavalidation_cp.json
│   └── adls_trigger_cp.json
│
├── notebooks/
│   └── cp-sales-logic-notebook.py
│
├── configs/
│   └── keyvault_secrets.json
│
├── resources/
│   └── architecture-diagram.png (optional)
│
└── README.md
```

---

## 📌 Conclusion

This project demonstrates an automated and scalable pipeline using Azure services and Databricks, focused on validating and transforming order data to provide customer-level sales insights.

> 🔐 All secrets and credentials are securely managed through Azure Key Vault and Databricks secret scopes.

---

