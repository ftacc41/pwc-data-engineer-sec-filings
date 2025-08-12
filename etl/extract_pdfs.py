from PyPDF2 import PdfReader
import pathlib

PDF_DIR = pathlib.Path('data/raw_pdfs')  # place your SEC PDF filings here
OUT_DIR = pathlib.Path('data/bronze/pdfs_text')
OUT_DIR.mkdir(parents=True, exist_ok=True)

def extract_text_from_pdf(pdf_path: pathlib.Path):
    text = []
    reader = PdfReader(str(pdf_path))
    for p in reader.pages:
        text.append(p.extract_text() or '')
    out_file = OUT_DIR / (pdf_path.stem + '.txt')
    out_file.write_text('\n'.join(text), encoding='utf-8')
    print('Wrote', out_file)

if __name__ == '__main__':
    for pdf in PDF_DIR.glob('*.pdf'):
        extract_text_from_pdf(pdf)
