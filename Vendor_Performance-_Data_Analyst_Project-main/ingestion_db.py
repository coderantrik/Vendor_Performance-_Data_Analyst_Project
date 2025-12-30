import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
from collections import Counter

# Logging setup
logging.basicConfig(
    filename=r"C:\Users\ASUS\Desktop\Vendor Performance Project\data\logs\ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# SQLite engine
engine = create_engine('sqlite:///invertory.db')


# ✅ Remove duplicate columns (case-insensitive)
def remove_duplicate_columns_case_insensitive(df):
    seen = set()
    new_cols = []
    for col in df.columns:
        lower_col = col.lower()
        if lower_col not in seen:
            seen.add(lower_col)
            new_cols.append(col)
    return df[new_cols]


# ✅ Optional: Log duplicate column names (for debugging)
def log_duplicate_columns(df):
    duplicates = [col for col, count in Counter([c.lower() for c in df.columns]).items() if count > 1]
    if duplicates:
        logging.warning(f'Duplicate column names found (case-insensitive): {duplicates}')


# ✅ Function to ingest a DataFrame into SQLite
def ingest_db(df, table_name, engine):
    log_duplicate_columns(df)
    df = remove_duplicate_columns_case_insensitive(df)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f'{table_name} ingested with {len(df)} rows and {len(df.columns)} columns.')


# ✅ Load and ingest all CSV files from the folder
def load_raw_data():
    start = time.time()
    folder = r"C:\Users\ASUS\Desktop\Vendor Performance Project\data"

    for file in os.listdir(folder):
        if file.endswith('.csv'):
            try:
                path = os.path.join(folder, file)
                df = pd.read_csv(path)
                logging.info(f'Loaded {file} with {len(df)} rows.')
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"❌ Failed to ingest {file}: {e}")

    total_time = (time.time() - start) / 60
    logging.info('✅ Ingestion Complete')
    logging.info(f'Total time taken: {total_time:.2f} minutes')


# ✅ Main execution
if __name__ == '__main__':
    load_raw_data()

