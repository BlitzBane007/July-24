import dask.dataframe as dd
from tqdm import tqdm
import pandas as pd
import tkinter as tk
from tkinter import filedialog
import time
from dask.diagnostics import ProgressBar

# Create a Tkinter root window
root = tk.Tk()
root.withdraw()
ProgressBar().register()
# Select the first input file (efs1.csv)
print("Select the first input file (efs1.csv)")
efs_file_path = filedialog.askopenfilename()

# Select the second input file (hfi1.csv)
print("Select the second input file (hfi1.csv)")
hfi_file_path = filedialog.askopenfilename()

# Select the output file path
print("Select the output file")
output_file_path = filedialog.asksaveasfilename(defaultextension=".csv")
start_time = time.time()

print("Initialising STAGE 1 - Data Loading...")
stage1 = time.time()

print("Loading file 1 - Please wait...")
load1 = time.time()
df_efs = dd.read_csv(efs_file_path, header=0, sep=';', low_memory=False, assume_missing=True, blocksize=1e9, skiprows=[1], dtype=str)
print("File 1 Loaded in", round((time.time() - load1), 2))
print("----")
print("Loading file 2 - Please wait...")

load2 = time.time()
df_hfi = dd.read_csv(hfi_file_path, header=0, sep=';', low_memory=False, assume_missing=True, blocksize=1e9, skiprows=[1], dtype=str)
print("File 2 loaded in", round((time.time() - load2), 2))

output_data = []
OFST020000 = 'OFST020000'
print("STAGE 1 COMPLETED in", round((time.time() - stage1), 2))
print("--------------")

print("Initialising STAGE 2 - Computing Data")
stage2 = time.time()

print("Computing Dataframe 1 - Please wait...")
df1t = time.time()
df_efs = df_efs.compute()
print("Dataframe 1 Loaded in", round((time.time() - df1t), 2))
print("----")
print("Computing Dataframe 2 - Please wait...")
df2t = time.time()
df_hfi = df_hfi.compute()
print("Dataframe 2 Loaded in", round((time.time() - df2t), 2))
print("STAGE 2 COMPLETED in", round((time.time() - stage2), 2))
print("--------------")
print("Initialising STAGE 3 - Get ISIN and Header count")
stage3 = time.time()

# Get the common ISIN and count
join1 = df_efs.merge(df_hfi, how='inner', on=OFST020000)
com_isin = join1[OFST020000].unique()
num_isin = len(com_isin)
print('Total ISIN:', num_isin)

# List all the headers and count
intersection_headers = df_efs.columns.intersection(df_hfi.columns)
intersection_headers_list = intersection_headers.tolist()
headers = intersection_headers_list
num_header = len(headers)
print('Total headers', num_header)
print("STAGE 3 COMPLETED in", round((time.time() - stage3), 2))
print("--------------")
print("Initialising STAGE 4 - Index data using common ISIN and Headers")
stage4 = time.time()

# Sort both dataframes based on primary_k (ISIN) column
df_efs_sorted = df_efs.sort_values(OFST020000).drop_duplicates(subset=OFST020000)
print("Indexing in progress - Subroutine 1/4 completed")
df_hfi_sorted = df_hfi.sort_values(OFST020000).drop_duplicates(subset=OFST020000)
print("Indexing in progress - Subroutine 2/4 completed")
# Preprocessing step to handle specific cases that may cause exceptions
efs_filtered = df_efs_sorted[df_efs_sorted[OFST020000].isin(com_isin)][headers].reset_index(drop=True)
print("Indexing in progress - Subroutine 3/4 completed")
hfi_filtered = df_hfi_sorted[df_hfi_sorted[OFST020000].isin(com_isin)][headers].reset_index(drop=True)
print("Indexing in progress - Subroutine 4/4 completed")

print('EFS Lines', len(efs_filtered))
print('HFI Lines', len(hfi_filtered))
print("STAGE 4 COMPLETED in", round((time.time() - stage4), 2))
print("--------------")
print("Initialising STAGE 5 - Iterate on each data value and compare for discrepancy ")
stage5 = time.time()
# Iterate over headers and compare values
for header in tqdm(headers, desc="Processing Headers", unit="Header"):
    efs_values = efs_filtered[header]
    hfi_values = hfi_filtered[header]
    not_matching = (efs_values.str.lower() != hfi_values.str.lower()) & efs_values.notna()
    mismatched_data = pd.DataFrame({
        'ISIN': efs_filtered[OFST020000][not_matching],
        'OFSTID': header,
        'EFS': efs_values[not_matching],
        'HFI': hfi_values[not_matching]
    })
    output_data.append(mismatched_data)
print("STAGE 5 COMPLETED in", round((time.time() - stage5), 2))
print("--------------")
print("Initialising STAGE 6 - Write data to CSV file")
stage6 = time.time()

output_data = pd.concat(output_data, ignore_index=True)

output_data.to_csv(output_file_path, index=False)

print('Write completed in', round((time.time() - stage6), 2))
print("--------------")
print('Time taken:', time.time() - start_time)
