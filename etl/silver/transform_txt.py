import pandas as pd
import pathlib
from typing import List
from pandas.errors import ParserError

# Define the input and output directories
BRONZE_TXT_DIR = pathlib.Path('data/bronze/txt')
SILVER_PARQUET_DIR = pathlib.Path('data/silver/financials')
SILVER_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strips whitespace and converts column names to snake_case."""
    df.columns = [c.strip().replace(' ', '_').replace('.', '').lower() for c in df.columns]
    return df

def transform_raw_txt_to_silver(file_path: pathlib.Path):
    """
    Reads a raw text file, cleans it, and saves it to the Silver layer
    as a Parquet file. Explicitly tries tab delimiter first.
    """
    delimiters = ['\t', ',', ';', '|']  # Common delimiters to try, starting with tab

    for sep in delimiters:
        try:
            # Try to read the file with the current delimiter
            df = pd.read_csv(file_path, sep=sep, on_bad_lines='warn', engine='python')
            print(f'File {file_path.name} successfully parsed with delimiter: "{sep}"')
            
            # Apply basic cleaning
            df = standardize_columns(df)

            # Define output path
            out_file = SILVER_PARQUET_DIR / (file_path.stem + '.parquet')

            # Save to Parquet format in the Silver layer
            df.to_parquet(out_file, index=False)
            print(f'Successfully transformed and saved: {out_file}')
            return  # Exit the function after a successful parse

        except ParserError as e:
            # The current delimiter failed, try the next one
            print(f'Failed to parse {file_path.name} with delimiter "{sep}": {e}')
        
        except Exception as e:
            # Handle other potential errors
            print(f'An unexpected error occurred while processing {file_path.name}: {e}')
            return

    # If the loop finishes without a successful parse
    print(f'Could not parse file {file_path.name} with any of the attempted delimiters.')


if __name__ == '__main__':
    # Remove old Parquet files for a clean run
    for old_file in SILVER_PARQUET_DIR.glob('*.parquet'):
        old_file.unlink()
    
    txt_files: List[pathlib.Path] = list(BRONZE_TXT_DIR.glob('*.txt'))
    
    if not txt_files:
        print(f"No .txt files found in {BRONZE_DIR}. Please make sure your files are in the correct directory.")
    else:
        for file in txt_files:
            transform_raw_txt_to_silver(file)