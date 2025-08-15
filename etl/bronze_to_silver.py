import pandas as pd
from pathlib import Path
import PyPDF2
import shutil
import warnings

# Suppress specific warnings from PyPDF2 for cleaner output
warnings.filterwarnings("ignore", category=PyPDF2.errors.PdfReadWarning)

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extracts all text content from a given PDF file."""
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception as e:
        print(f"  - Warning: Could not read {pdf_path.name}. Error: {e}")
    return text

def main():
    """
    Main ETL script to process data from the Bronze layer to the Silver layer.
    """
    print("--- Starting Bronze to Silver ETL Process ---")

    # --- 1. Set Up Paths ---
    # Robustly define paths relative to the script's location
    SCRIPT_DIR = Path(__file__).resolve().parent
    ROOT_DIR = SCRIPT_DIR.parent
    BRONZE_DIR = ROOT_DIR / "data" / "bronze"
    SILVER_DIR = ROOT_DIR / "data" / "silver"
    STRUCTURED_BRONZE = BRONZE_DIR / "structured_filings"
    UNSTRUCTURED_BRONZE = BRONZE_DIR / "unstructured_filings_pdf"

    print(f"Source Bronze directory: {BRONZE_DIR}")
    print(f"Target Silver directory: {SILVER_DIR}")

    # --- 2. Initialize Silver Directory ---
    if SILVER_DIR.exists():
        print(f"Removing existing Silver directory: {SILVER_DIR}")
        shutil.rmtree(SILVER_DIR)
    SILVER_DIR.mkdir(parents=True)
    print("✓ Silver directory created.")

    # --- 3. Load Structured Data ---
    print("\nStep 1: Loading structured data from Bronze CSVs...")
    dfs = {}
    for csv_file in STRUCTURED_BRONZE.glob("*.csv"):
        table_name = csv_file.stem
        dfs[table_name] = pd.read_csv(csv_file)
    print(f"✓ Loaded {len(dfs)} tables: {list(dfs.keys())}")

    # --- 4. Extract and Process Unstructured Data ---
    print("\nStep 2: Extracting text from unstructured PDFs...")
    pdf_texts = []
    pdf_files = sorted(list(UNSTRUCTURED_BRONZE.glob("*.pdf")))
    total_pdfs = len(pdf_files)

    for i, pdf_file in enumerate(pdf_files):
        adsh = pdf_file.stem  # Filename is the accession number (adsh)
        text = extract_text_from_pdf(pdf_file)
        pdf_texts.append({'adsh': adsh, 'extracted_pdf_text': text})
        if (i + 1) % 10 == 0 or (i + 1) == total_pdfs:
            print(f"  - Processed {i + 1}/{total_pdfs} PDFs")
    
    pdf_df = pd.DataFrame(pdf_texts)
    print("✓ Extracted and compiled PDF text into a DataFrame.")

    # --- 5. Merge, Clean, and Transform Data ---
    print("\nStep 3: Merging, cleaning, and transforming data...")
    # Merge extracted PDF text into the 'sub' dataframe
    dfs['sub'] = pd.merge(dfs['sub'], pdf_df, on='adsh', how='left')
    print("  - Merged PDF text with 'sub' table.")

    # Data Type Conversion (example)
    # Convert ddate to a proper datetime format
    if 'ddate' in dfs['num'].columns:

        dfs['num']['ddate'] = pd.to_datetime(dfs['num']['ddate'].astype(int).astype(str), format='%Y%m%d')
        print("  - Converted 'ddate' column to datetime format.")
    
    print("✓ Data merging and cleaning complete.")

    # --- 6. Save to Silver Layer as Parquet ---
    print("\nStep 4: Saving cleaned dataframes to Silver layer as Parquet files...")
    for table_name, df in dfs.items():
        output_path = SILVER_DIR / f"{table_name}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"  - Saved {output_path}")
    print("✓ All tables saved to Silver layer.")

    print("\n--- ✅ Bronze to Silver ETL Process Complete ---")

if __name__ == "__main__":
    main()