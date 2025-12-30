# -Vendor_Performance-_Data_Analyst_Project
A data analysis project focusing on vendor performance metrics using Python, SQL, and visualization tools.
# Vendor Performance Data Analyst Project

This project performs exploratory data analysis (EDA) and data processing on vendor-related purchase and sales data. It helps identify vendor performance trends, profit margins, and purchase efficiency using Python and SQLite.

---

## 📊 Objective

To analyze vendor performance using sales, purchases, and freight data, and generate summarized insights including:
- Total purchase vs sales comparisons
- Freight cost analysis
- Gross profit computation
- Brand-level insights
- Data-driven decision support

---

## 🛠️ Technologies Used

- **Python** (Pandas, NumPy, SQLite3, Matplotlib, Seaborn)
- **SQLite** for local database handling
- **Jupyter / VS Code** (compatible)
- **Data files**: `.csv` and `.db`

---

## 📁 Project Files

| File Name                  | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `Exploratory Data Analysis.py` | Performs overall data loading, cleaning, visualization, and summary stats     |
| `get_vendor_summary.py`       | Extracts summary insights like freight costs, gross profits, and brand-wise stats |
| `ingestion_db.py`             | Ingests cleaned data into SQLite database tables (`vendor_sales_summary`, etc.) |

---

## 📦 How to Run

1. **Clone or download** the repository:
   ```bash
   git clone https://github.com/Sumit724932/Vendor_Performance-_Data_Analyst_Project.git
