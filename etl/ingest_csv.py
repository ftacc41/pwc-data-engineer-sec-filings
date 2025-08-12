import pandas as pd
import pathlib
from datetime import datetime

BRONZE_DIR = pathlib.Path('data/bronze')
CSV_DIR = BRONZE_DIR / 'csv'
OUT_DIR = BRONZE_DIR / 'parquet'
OUT_DIR.mkdir(parents=True, exist_ok=True)

def ingest_csv_to_parquet(csv_path: pathlib.Path):
    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    out_file = OUT_DIR / (csv_path.stem + '.parquet')
    df.to_parquet(out_file, index=False)
    print('Saved', out_file)

if __name__ == '__main__':
    for csv_file in CSV_DIR.glob('*.csv'):
        ingest_csv_to_parquet(csv_file)
