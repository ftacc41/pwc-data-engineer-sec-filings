import pandas as pd
import pathlib
from pandas.errors import ParserError
from typing import List

def validate_file_pair(original_file: pathlib.Path, transformed_file: pathlib.Path) -> bool:
    """
    Compares an original text file with its transformed parquet file to validate the ETL process.
    Returns True if validation is successful, False otherwise.
    """
    print(f"--- Validating {original_file.name} vs {transformed_file.name} ---")
    
    delimiters = [',', '\t', ';', '|']  # Same delimiters to try
    original_df = None
    
    # First, try to read the original file with multiple delimiters
    for sep in delimiters:
        try:
            original_df = pd.read_csv(original_file, sep=sep, on_bad_lines='warn', engine='python')
            print(f"SUCCESS: Original file parsed with delimiter: '{sep}'.")
            break  # Break the loop on the first successful parse
        except ParserError:
            continue # Try next delimiter
        except Exception as e:
            print(f"FAILED: An unexpected error occurred while parsing original file: {e}")
            return False
            
    if original_df is None:
        print(f"FAILED: Could not parse original file {original_file.name} with any of the attempted delimiters.")
        return False

    try:
        # Read the transformed parquet file
        transformed_df = pd.read_parquet(transformed_file)

        # 1. Check Row Counts
        if len(original_df) != len(transformed_df):
            print(f"FAILED: Row counts do not match. Original: {len(original_df)}, Transformed: {len(transformed_df)}")
            return False
        print(f"SUCCESS: Row counts match ({len(original_df)}).")

        # 2. Check Column Counts
        if len(original_df.columns) != len(transformed_df.columns):
            print(f"FAILED: Column counts do not match. Original: {len(original_df.columns)}, Transformed: {len(transformed_df.columns)}")
            return False
        print(f"SUCCESS: Column counts match ({len(original_df.columns)}).")

        # 3. Check for standardized columns
        expected_cols = [c.strip().replace(' ', '_').replace('.', '').lower() for c in original_df.columns]
        if list(transformed_df.columns) != expected_cols:
            print("FAILED: Column names are not standardized as expected.")
            print(f"Expected: {expected_cols}")
            print(f"Found:    {list(transformed_df.columns)}")
            return False
        print("SUCCESS: Column names are standardized.")

        # 4. Deep data comparison
        original_df.columns = expected_cols
        if not original_df.equals(transformed_df):
            print("FAILED: Data contents do not match.")
            return False
        print("SUCCESS: Data contents are identical.")
        
    except Exception as e:
        print(f"An unexpected error occurred during validation: {e}")
        return False
        
    print("\n")
    return True

if __name__ == '__main__':
    BRONZE_TXT_DIR = pathlib.Path('data/bronze/txt')
    SILVER_PARQUET_DIR = pathlib.Path('data/silver/financials')
    
    txt_files: List[pathlib.Path] = list(BRONZE_TXT_DIR.glob('*.txt'))
    
    if not txt_files:
        print("No .txt files found to validate.")
    else:
        for original_file in txt_files:
            transformed_file = SILVER_PARQUET_DIR / (original_file.stem + '.parquet')
            validate_file_pair(original_file, transformed_file)