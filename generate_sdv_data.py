import pandas as pd
from pathlib import Path
from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer
from faker import Faker
from fpdf import FPDF
import random
import shutil
import sys
import traceback

def generate_rich_text(fake: Faker) -> str:
    """Generates a more realistic paragraph with searchable keywords."""
    keywords = ['risk', 'revenue', 'growth', 'challenges', 'assets', 'liabilities', 'market conditions', 'competition', 'strategy', 'operating results']
    templates = [
        "The primary market risk for our company relates to {} and overall {}.",
        "In this quarter, we saw significant growth in {}, exceeding all expectations.",
        "Our forward-looking strategy focuses on overcoming challenges related to {} and expanding our market share.",
        "A detailed discussion of our financial results, including {} and {}, is included in this filing.",
        "We face significant {} from established players in the industry.",
        "Management's discussion includes an analysis of our {} and their impact on our {}."
    ]
    
    num_sentences = random.randint(4, 8)
    paragraph_sentences = []
    for _ in range(num_sentences):
        template = random.choice(templates)
        k = template.count('{}')
        selected_keywords = random.sample(keywords, k)
        paragraph_sentences.append(template.format(*selected_keywords))

    return " ".join(paragraph_sentences)


def generate_comprehensive_data(num_filings=200):
    print("--- Starting comprehensive data generation script ---")
    try:
        SCRIPT_DIR = Path(__file__).resolve().parent
        BRONZE_DIR = SCRIPT_DIR / "data" / "bronze"
        STRUCTURED_DIR = BRONZE_DIR / "structured_filings"
        UNSTRUCTURED_DIR = BRONZE_DIR / "unstructured_filings_pdf"
        print(f"Step 0: Target data directory set to: {BRONZE_DIR}")

        print("Step 1: Defining a larger, more varied sample data set with rich text...")
        fake = Faker()
        sub_real_data = []
        for i in range(10):
            company_name = fake.company() + " " + fake.company_suffix()
            cik = fake.unique.random_number(digits=7, fix_len=True)
            for _ in range(random.randint(1, 3)):
                adsh = f"{fake.random_number(digits=10, fix_len=True)}-{fake.random_number(digits=2, fix_len=True)}-{fake.random_number(digits=6, fix_len=True)}"
                form_type = random.choice(['10-K', '10-Q', '8-K'])
                sub_real_data.append({
                    "adsh": adsh, "cik": cik, "name": company_name, "form": form_type,
                    "sic": fake.random_number(digits=4, fix_len=True),
                    "filing_summary": generate_rich_text(fake)
                })
        sub_real = pd.DataFrame(sub_real_data)
        tag_real = pd.DataFrame({
            "tag_id": [1, 2, 3, 4, 5, 6],
            "tag": ["Revenues", "NetIncomeLoss", "Assets", "Liabilities", "OperatingExpenses", "Cash"],
            "version": ["us-gaap/2023"] * 6, "custom": [0] * 6,
            "label": ["Revenues", "Net Income (Loss)", "Assets", "Liabilities", "Operating Expenses", "Cash and Cash Equivalents"],
        })
        pre_real_data = []
        for adsh in sub_real['adsh'].tolist():
            stmt = random.choice(['IS', 'BS', 'CF'])
            num_tags = random.randint(1, 4)
            for tag_id in random.sample(tag_real['tag_id'].tolist(), k=num_tags):
                pre_real_data.append({
                    "pre_id": fake.unique.random_number(digits=5, fix_len=True),
                    "adsh": adsh, "stmt": stmt, "tag_id": tag_id,
                })
        pre_real = pd.DataFrame(pre_real_data)
        num_real_data = []
        num_id_counter = 0
        for _, row in pre_real.iterrows():
            num_real_data.append({
                "num_id": num_id_counter, "adsh": row['adsh'], "tag_id": row['tag_id'], "version": "us-gaap/2023",
                "ddate": int(fake.date_between(start_date='-2y', end_date='today').strftime('%Y%m%d')),
                "qtrs": random.randint(1, 4), "value": random.randint(100000, 999999999),
            })
            num_id_counter += 1
        num_real = pd.DataFrame(num_real_data)
        real_data = {'sub': sub_real, 'tag': tag_real, 'pre': pre_real, 'num': num_real}
        print(f"✓ Defined sample data with {len(sub_real)} filings.")

        metadata = MultiTableMetadata()
        metadata.detect_from_dataframes(data=real_data)
        metadata.update_column(table_name='sub', column_name='filing_summary', sdtype='text')
        metadata.update_column(table_name='num', column_name='ddate', sdtype='datetime', datetime_format='%Y%m%d')
        metadata.set_primary_key(table_name='tag', column_name='tag_id')
        metadata.set_primary_key(table_name='num', column_name='num_id')
        print("Step 2: SDV metadata configured.")
        
        synthesizer = HMASynthesizer(metadata)
        print("Step 3: Training SDV synthesizer... (This may take a moment)")
        synthesizer.fit(real_data)
        print("✓ Synthesizer trained successfully.")

        print(f"Step 4: Generating synthetic data for {num_filings} new filings...")
        synthetic_data = synthesizer.sample(num_filings)
        print(f"✓ Generated {len(synthetic_data['sub'])} parent records and related child data.")

        print("Step 5: Preparing to save data to the file system...")
        if BRONZE_DIR.exists():
            shutil.rmtree(BRONZE_DIR)
        STRUCTURED_DIR.mkdir(parents=True, exist_ok=True)
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
        print(f"  ✓ Generated {total_pdfs} PDF reports and saved to {UNSTRUCTURED_DIR}")
        print("\n--- ✅ SUCCESS ---")
        print("Comprehensive data generation complete.")

    except Exception as e:
        print("\n--- ❌ ERROR ---", file=sys.stderr)
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        traceback.print_exc()

if __name__ == "__main__":
    generate_comprehensive_data(num_filings=200)