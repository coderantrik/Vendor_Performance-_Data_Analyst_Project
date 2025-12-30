import pandas as pd
import sqlite3
import time
import logging
from ingestion_db import ingest_db  # Make sure this file/function exists and is correct

# ------------------- Logging Setup -------------------
logging.basicConfig(
    filename=r"C:\Users\ASUS\Desktop\Vendor Performance Project\data\logs\get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ------------------- Create Vendor Summary -------------------
def create_vendor_summary(conn):
    vendor_sales_summary = pd.read_sql_query("""
    WITH 
    FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp 
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY 
            p.VendorNumber, p.VendorName, p.Brand, p.Description, 
            p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT 
            VendorNo, 
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss 
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs 
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """, conn)

    return vendor_sales_summary

# ------------------- Clean Data -------------------
def clean_data(df):
    df['volume'] = df['Volume'].astype('float')
    df.fillna(0, inplace=True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Derived Metrics
    df['Grossprofit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = (df['Grossprofit'] / df['TotalSalesDollars'].replace(0, 1)) * 100
    df['StockTurnover'] = df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'].replace(0, 1)
    df['SalestoPurchaseRatio'] = df['TotalSalesDollars'] / df['TotalPurchaseDollars'].replace(0, 1)

    return df

# ------------------- Remove Duplicate Columns -------------------
def remove_duplicate_columns(df):
    return df.loc[:, ~df.columns.duplicated()]

# ------------------- Main Execution -------------------
if __name__ == '__main__':
    try:
        conn = sqlite3.connect(r'C:\Users\ASUS\Desktop\invertory.db')

        logging.info('Creating Vendor Summary Table...')
        summary_df = create_vendor_summary(conn)
        logging.info('Summary Data Retrieved:\n%s', summary_df.head())

        logging.info('Cleaning Data...')
        clean_df = clean_data(summary_df)
        logging.info('Cleaned Data Preview:\n%s', clean_df.head())

        logging.info('Removing Duplicate Columns...')
        clean_df = remove_duplicate_columns(clean_df)

        logging.info('Ingesting Data to Database...')
        ingest_db(clean_df, 'vendor_sales_summary', conn)
        logging.info('Data Ingestion Completed Successfully.')

    except Exception as e:
        logging.error("Error occurred: %s", str(e))
        raise

