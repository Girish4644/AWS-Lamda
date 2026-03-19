import os
import shutil
import pandas as pd

# Directories
raw_dir = 'raw'
location1 = 'location1'
location2 = 'location2'

# Create directories if they don't exist
os.makedirs(location1, exist_ok=True)
os.makedirs(location2, exist_ok=True)

# List of files to process
files = ['data_1.csv', 'data_2.csv']

for file in files:
    path = os.path.join(raw_dir, file)
    if os.path.exists(path):
        # Read the CSV file
        df = pd.read_csv(path)
        
        # Check if 'OrderID' column exists
        has_orderid = 'OrderID' in df.columns
        
        # Check if more than 2 records (rows)
        has_more_than_2_records = len(df) > 2
        
        # Decide destination
        if has_orderid and has_more_than_2_records:
            dest = os.path.join(location1, file)
            print(f"Moving {file} to location1")
        else:
            dest = os.path.join(location2, file)
            print(f"Moving {file} to location2")
        
        # Move the file
        shutil.move(path, dest)
    else:
        print(f"File {file} not found in {raw_dir}")

print("Processing complete.")