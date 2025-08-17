# In api/services.py

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from fastapi import HTTPException, status
import threading

from .api_schemas import SubMission, SubMissionCreate, SubMissionUpdate

# Define the path to our raw data file
# This assumes the script is run from the project root, which Docker does.
BRONZE_SUB_CSV_PATH = Path("data/bronze/structured_filings/sub.csv")

# A simple file lock to prevent race conditions when writing to the CSV.
# In a real-world scenario, you'd use a proper database or a more robust locking mechanism.
csv_lock = threading.Lock()

def get_all_submissions(skip: int = 0, limit: int = 100) -> List[SubMission]:
    """
    Reads all submission records from the CSV file with pagination.
    """
    if not BRONZE_SUB_CSV_PATH.exists():
        return []
    df = pd.read_csv(BRONZE_SUB_CSV_PATH)
    paginated_df = df.iloc[skip : skip + limit]
    return [SubMission(**row) for row in paginated_df.to_dict(orient='records')]

def get_submission_by_adsh(adsh: str) -> Optional[SubMission]:
    """
    Finds a single submission record by its unique adsh.
    """
    if not BRONZE_SUB_CSV_PATH.exists():
        return None
    df = pd.read_csv(BRONZE_SUB_CSV_PATH)
    record = df[df['adsh'] == adsh]
    if record.empty:
        return None
    return SubMission(**record.iloc[0].to_dict())

def create_submissions(submissions: List[SubMissionCreate]) -> List[SubMission]:
    """
    Adds one or more new submission records to the CSV file.
    This handles both single and batch creation.
    """
    with csv_lock:
        df = pd.read_csv(BRONZE_SUB_CSV_PATH) if BRONZE_SUB_CSV_PATH.exists() else pd.DataFrame()
        
        new_records_df = pd.DataFrame([s.model_dump() for s in submissions])
        
        # Check for duplicates before appending
        existing_adsh = df['adsh'].isin(new_records_df['adsh'])
        if existing_adsh.any():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"One or more submissions with these adsh values already exist."
            )
            
        df = pd.concat([df, new_records_df], ignore_index=True)
        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
    
    return [SubMission(**s.model_dump()) for s in submissions]


def update_submission(adsh: str, submission_update: SubMissionUpdate) -> SubMission:
    """
    Updates an existing submission record in the CSV file.
    """
    with csv_lock:
        if not BRONZE_SUB_CSV_PATH.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission file not found.")
            
        df = pd.read_csv(BRONZE_SUB_CSV_PATH)
        record_index = df.index[df['adsh'] == adsh].tolist()

        if not record_index:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Submission with adsh '{adsh}' not found.")
        
        # Get the update data, excluding any fields that were not set
        update_data = submission_update.model_dump(exclude_unset=True)
        
        for key, value in update_data.items():
            df.loc[record_index[0], key] = value
            
        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
        
        updated_record = df.iloc[record_index[0]]
        return SubMission(**updated_record.to_dict())


def delete_submission(adsh: str) -> Dict[str, str]:
    """
    Deletes a submission record from the CSV file.
    """
    with csv_lock:
        if not BRONZE_SUB_CSV_PATH.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Submission file not found.")

        df = pd.read_csv(BRONZE_SUB_CSV_PATH)
        original_len = len(df)
        df = df[df['adsh'] != adsh]

        if len(df) == original_len:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Submission with adsh '{adsh}' not found.")

        df.to_csv(BRONZE_SUB_CSV_PATH, index=False)
        
    return {"message": f"Submission with adsh '{adsh}' deleted successfully."}