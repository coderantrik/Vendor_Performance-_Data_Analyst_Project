import pandas as pd
import sqlite3
import time
import logging
from ingestion_db import ingest_db

# Use full path to database if needed
conn = sqlite3.connect(r'C:\Users\ASUS\Desktop\invertory.db')

# ✅ FIXED: Corrected table query from "sqlite_master_master" to "sqlite_master"
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)

# ✅ FIXED: Loop through tables['name'] instead of the entire DataFrame
for table in tables['name']:
    print('-'*50, f'{table}', '-'*50)
    print('Count of records:', pd.read_sql(f"select count(*) as count from {table}", conn)['count'].values[0])
    # ✅ FIXED: display() is used in Jupyter, so use print() in VS Code
    print(pd.read_sql(f"select * from {table} limit 5", conn))

purchases=pd.read_sql_query("select * from purchases where VendorNumber=4466",conn)
print(purchases)

purchase_prices=pd.read_sql_query("""select * from purchase_prices where VendorNumber=4466""",conn)
print(purchase_prices)

vendor_invoice=pd.read_sql_query("select * from vendor_invoice where VendorNumber=4466",conn)
print(vendor_invoice)

sales=pd.read_sql_query("select * from sales where VendorNo=4466",conn)
print(sales)

purchases=purchases.groupby(['Brand','PurchasePrice'])[['Quantity','Dollars']].sum()
print(purchases)

sales=sales.groupby('Brand')[['SalesDollars','SalesPrice','SalesQuantity']].sum()
print(sales)


freight_summary=pd.read_sql_query("select VendorNumber ,SUM(Freight) as FreightCost From vendor_invoice Group BY VendorNumber ",conn)
print(freight_summary)

print(pd.read_sql_query('select p.VendorNumber , p.VendorName , p.Brand , p.PurchasePrice , pp.volume , pp.Price as ActualPrice , SUM(p.Quantity) as TotalPurchaseQuantity,SUM(p.Dollars) as TotalPurchaseDollars From purchases p JOIN purchase_prices pp ON p.Brand=pp.Brand Where p.PurchasePrice > 0 GROUP BY P.VendorNumber,p.VendorName,p.Brand ORDER BY TotalPurchaseDollars',conn))

print(pd.read_sql_query("SELECT VendorNo,Brand,SUM(SalesDollars) as TotalSalesDollars,SUM(SalesPrice) as TotalSalesPrice,SUM(SalesQuantity) as TotalSalesQuanity, SUM(ExciseTax) as TotalExciseTax FROM sales GROUP BY VendorNo,Brand ORDER BY TotalSalesDollars",conn))


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
    JOIN purchase_prices pp ON p.Brand = pp.Brand
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

print(vendor_sales_summary)


vendor_sales_summary['Volume']=vendor_sales_summary['Volume'].astype('float64')
vendor_sales_summary.fillna(0,inplace=True)
vendor_sales_summary['VendorName']=vendor_sales_summary['VendorName'].str.strip()
vendor_sales_summary['Grossprofit']=vendor_sales_summary['TotalSalesDollars']-vendor_sales_summary['TotalPurchaseDollars']

vendor_sales_summary['ProfitMragin']=(vendor_sales_summary['Grossprofit']/vendor_sales_summary['TotalSalesDollars'])*100
vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
vendor_sales_summary['SalestoPurchaseRatio']=vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']


cursor=conn.cursor()
'''cursor.execute(""" CREATE TABLE vendor_sales_summary(
               VendorNumber INT,
               VendorName VARCHAR(100),
               Brand INT,
               Description VARCHAR(100),
               PurchasePrice DECIMAL(10,2),
               ActualPrice DECIMAL(10,2),
               Volume,
               TotalPurchaseQuantity INT,
               TotalPurchaseDollars DECIMAL(15,2),
               TotalSalesQuanity INT,
               TotalSalesDollars DECIMAL(15,2),
               TotalSalesPrice DECIMAL(15,2),
               TotalExciseTax DECIMAL(15,2),
               FreightCost DECIMAL(15,2),
               GrossProfit DECIMAL(15,2),
               ProfitMargin DECIMAL(15,2),
               SalesTurnover DECIMAL(15,2),
               SalesRoPurchaseRatio DECIMAL(15,2),
               PRIMARY KEY(VendorNumber,Brand)
);
""")'''


print(vendor_sales_summary.to_sql('vendor_sales_summary',conn ,if_exists='replace',index=False))
print(pd.read_sql_query("select * from vendor_sales_summary",conn))


###''''start=time.time()final=pd.read_sql_query("SELECT pp.VendorNumber,pp.Brand,pp.Price as ActualPrice,pp.PurchasePrice,SUM(s.SalesQuantity) AS TotalSalesQuantity,SUM(s.SalesDollars) AS TotalSalesDollars,SUM(s.ExciseTax) AS TotalExciseTax,SUM(vi.Quantity) AS TotalPurchaseQuantity,SUM(vi.Dollars) AS TotalPurchaseDollars,SUM(vi.Freight) AS TotalFreightCost FROM purchase_prices pp JOIN sales s ON pp.VendorNumber=s.VendorNo AND pp.Brand=s.Brand JOIN vendor_invoice vi ON pp.VendorNumber=vi.VendorNumber GROUP BY pp.VendorNumber,pp.Brand,pp.Price,pp.PurchasePrice",conn)end=time.time()'



# Updated log file path
logging.basicConfig(
    filename="C:\\Users\\ASUS\\Desktop\\Vendor Performance Project\\data\\logs\\get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def create_vendor_summary(conn):
  vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
   SELECT 
       VendorNumber, 
       SUM(Freight) AS FreightCost
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
        p.VendorNumber, p.VendorName, p.Brand, p.Description,p.PurchasePrice, pp.Price, pp.Volume
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
    ORDER BY ps.TotalPurchaseDollars DESC""", conn)
  return vendor_sales_summary


def clean_data(df):
  df['volume']=df['Volume'].astype('float')
  df.fillna(0,inplace=True)
  df['VendorName']=df['VendorName'].str.strip()
  df['Description']=df['Description'].str.strip()
  vendor_sales_summary['Grossprofit']=vendor_sales_summary['TotalSalesDollars']-vendor_sales_summary['TotalPurchaseDollars']
  vendor_sales_summary['ProfitMragin']=(vendor_sales_summary['Grossprofit']/vendor_sales_summary['TotalSalesDollars'])*100
  vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
  vendor_sales_summary['SalestoPurchaseRatio']=vendor_sales_summary['TotalSalesDollars']/vendor_sales_summary['TotalPurchaseDollars']
  return df

def remove_duplicate_columns(df):
    # Only keep the first occurrence of each column name (case-insensitive)
    df = df.loc[:, ~df.columns.duplicated()]
    return df

if __name__ == '__main__':
    conn = sqlite3.connect(r'C:\Users\ASUS\Desktop\invertory.db')
    logging.info('Creating Vendor summary Table......')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data......')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Removing duplicate columns......')
    clean_df = remove_duplicate_columns(clean_df)

    logging.info('Ingesting data....')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed')
