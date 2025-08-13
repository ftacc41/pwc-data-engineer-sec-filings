from sqlmodel import Session
from data_access.db import engine
from data_access.models import FilingDim
import pathlib

def register_raw_filing(accession, form_type, filename):
    with Session(engine) as session:
        f = FilingDim(accession_number=accession, form_type=form_type, filing_path=str(filename))
        session.add(f)
        session.commit()
        return f.id

if __name__ == '__main__':
    for txt in pathlib.Path('data/bronze/pdfs_text').glob('*.txt'):
        # crude placeholder values; you’ll replace with real parsed data later
        register_raw_filing('unknown', '10-K', txt)
