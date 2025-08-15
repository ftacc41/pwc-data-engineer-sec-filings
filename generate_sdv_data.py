import pandas as pd
from pathlib import Path
from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer
from faker import Faker
from fpdf import FPDF
import shutil
import sys
import traceback

def generate_comprehensive_data(num_filings=50):
    """
    Generates comprehensive, synthetic structured and unstructured data for the project.
    - Uses SDV to create relational tabular data based on a sample.
    - Uses Faker to generate rich text summaries.
    - Uses FPDF2 to create corresponding PDF files for unstructured data processing.
    """
    print("--- Starting comprehensive data generation script ---")

    try:
        # --- 0. Set Up Robust Paths ---
        SCRIPT_DIR = Path(__file__).resolve().parent
        BRONZE_DIR = SCRIPT_DIR / "data" / "bronze"
        STRUCTURED_DIR = BRONZE_DIR / "structured_filings"
        UNSTRUCTURED_DIR = BRONZE_DIR / "unstructured_filings_pdf"
        
        print(f"Step 0: Target data directory set to: {BRONZE_DIR}")

        # --- 1. Define the REAL data to train the SDV model ---
        fake = Faker()
        sub_real = pd.DataFrame({
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "cik": [1000180, 1000180, 1000200],
            "name": ["Innovation Corp", "Innovation Corp", "Data Dynamics Inc"],
            "form": ["10-Q", "10-K", "10-Q"],
            "sic": [3711, 3711, 7372],
            "filing_summary": [fake.text(max_nb_chars=1000) for _ in range(3)]
        })
        tag_real = pd.DataFrame({
            "tag_id": [1, 2, 3], "tag": ["Revenues", "NetIncomeLoss", "Assets"],
            "version": ["us-gaap/2023"] * 3, "custom": [0, 0, 0],
            "label": ["Revenues", "Net Income (Loss)", "Assets"],
        })
        pre_real = pd.DataFrame({
            "pre_id": [101, 102, 103],
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "stmt": ["IS", "BS", "IS"], "tag_id": [1, 3, 2],
        })
        num_real = pd.DataFrame({
            "adsh": ["0000000001-19-000001", "0000000002-19-000002", "0000000003-19-000003"],
            "tag_id": [1, 3, 2], "version": ["us-gaap/2023"] * 3,
            "ddate": [20230331, 20231231, 20230930], "qtrs": [1, 4, 3],
            "value": [2.5e10, 7.5e10, 1.5e10],
        })
        real_data = {'sub': sub_real, 'tag': tag_real, 'pre': pre_real, 'num': num_real}
        print("Step 1: In-memory sample data defined.")

        # --- 2. Define the Metadata for SDV ---
        metadata = MultiTableMetadata()
        metadata.detect_from_dataframes(data=real_data)
        metadata.update_column(
            table_name='sub',
            column_name='filing_summary',
            sdtype='text'
        )

        # This is the crucial fix to ensure only valid dates are generated
        metadata.update_column(
            table_name='num',
            column_name='ddate',
            sdtype='datetime',
            datetime_format='%Y%m%d'
        )
        
        metadata.set_primary_key(table_name='tag', column_name='tag_id')
        print("Step 2: SDV metadata configured.")
        
        # --- 3. Train the SDV Synthesizer ---
        print("Step 3: Training SDV synthesizer... (This may take a moment)")
        synthesizer = HMASynthesizer(metadata)
        synthesizer.fit(real_data)
        print("✓ Synthesizer trained successfully.")

        # --- 4. Generate Synthetic Data ---
        print(f"Step 4: Generating synthetic data for {num_filings} new filings...")
        # This uses the corrected syntax for your version of SDV
        synthetic_data = synthesizer.sample(num_filings)
        print(f"✓ Generated {len(synthetic_data['sub'])} parent records and related child data.")

        # --- 5. Save the Data ---
        print("Step 5: Preparing to save data to the file system...")
        if BRONZE_DIR.exists():
            print(f"  - Found existing directory, removing: {BRONZE_DIR}")
            shutil.rmtree(BRONZE_DIR)
        
        print(f"  - Creating structured data directory: {STRUCTURED_DIR}")
        STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)
        
        print(f"  - Creating unstructured data directory: {UNSTRUCTURED_DIR}")
        UNSTRUCTURED_DIR.mkdir(parents=True, exist_ok=True)
        
        print("  - Saving structured data as CSVs...")
        for table_name, df in synthetic_data.items():
            df.to_csv(STRUCTURED_DIR / f"{table_name}.csv", index=False)
        print(f"  ✓ Structured data saved to {STRUCTURED_DIR}")
        
        print("  - Generating and saving corresponding PDF files...")
        total_pdfs = len(synthetic_data['sub'])
        for i, (_, row) in enumerate(synthetic_data['sub'].iterrows()):
            adsh, filing_text, name, form = row['adsh'], row['filing_summary'], row['name'], row['form']
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial", 'B', 16)
            pdf.cell(0, 10, f"Filing for: {name} ({form})", 0, 1, 'C')
            pdf.ln(10)
            pdf.set_font("Arial", size=12)
            pdf.write(5, filing_text)
            pdf.output(UNSTRUCTURED_DIR / f"{adsh}.pdf")
            if (i + 1) % 10 == 0 or (i + 1) == total_pdfs:
                print(f"    - Generated {i + 1}/{total_pdfs} PDFs")
        print(f"  ✓ PDF reports saved to {UNSTRUCTURED_DIR}")

        print("\n--- ✅ SUCCESS ---")
        print("Comprehensive data generation complete.")
        print(f"Data is located in: {BRONZE_DIR}")

    except Exception as e:
        print("\n--- ❌ ERROR ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        traceback.print_exc()

if __name__ == "__main__":
    generate_comprehensive_data(num_filings=50)