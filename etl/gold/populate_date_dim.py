import pandas as pd
from sqlmodel import Session
from data_access.db import engine
from data_access.models import DateDim
from datetime import date

def populate_date_dim():
    """
    Programmatically generates a date dimension table for a fixed range.
    """
    print("Populating DateDim...")

    try:
        # Define a date range to generate. This can be adjusted.
        print("  - Defining date range...")
        start_date = date(2005, 1, 1)
        end_date = date(2025, 12, 31)

        # Generate the date range using pandas
        print("  - Generating date range with pandas...")
        date_range_df = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D')})

        # Extract date attributes
        print("  - Extracting date attributes...")
        date_range_df['year'] = date_range_df['date'].dt.year
        date_range_df['month'] = date_range_df['date'].dt.month
        date_range_df['day'] = date_range_df['date'].dt.day
        date_range_df['quarter'] = date_range_df['date'].dt.quarter
        
        # We'll leave fiscal_year as None for now, as it requires more complex logic
        date_range_df['fiscal_year'] = None
        
        # Format the date column as a string
        print("  - Formatting date column...")
        date_range_df['date'] = date_range_df['date'].dt.strftime('%Y-%m-%d')
        
        print(f"Generated {len(date_range_df)} date records from {start_date} to {end_date}.")

        with Session(engine) as session:
            # Create a list of DateDim objects
            dates_to_add = []
            for _, row in date_range_df.iterrows():
                date_record = DateDim(
                    date=row['date'],
                    year=row['year'],
                    month=row['month'],
                    day=row['day'],
                    quarter=row['quarter'],
                    fiscal_year=row['fiscal_year']
                )
                dates_to_add.append(date_record)
            
            # Add all date records to the session
            print(f"  - Adding {len(dates_to_add)} records to the session...")
            session.add_all(dates_to_add)
            
            # Commit the session
            print("  - Committing changes...")
            session.commit()
            print(f"Successfully added {len(dates_to_add)} records to DateDim.")

        print("Populating DateDim complete.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    populate_date_dim()