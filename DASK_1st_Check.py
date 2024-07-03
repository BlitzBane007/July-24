import dask.dataframe as dd
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

# Get the union of unique headers
union_headers = pd.unique(df_efs.columns.union(df_hfi.columns))
# Convert the union_headers to a list if needed
headers = union_headers.tolist()

# Get ISIN missing in df_efs
missing_isin =pd.Series(list(set(df_hfi[OFST020000].unique()) - set(df_efs[OFST020000].unique())))
print('ISIN missing in efs:', len(missing_isin))
missing_isin_df=pd.DataFrame({'ISIN missing in efs:': missing_isin})
output_data.append(missing_isin_df)


# Get headers missing in df_efs
missing_headers =pd.Series(list(set(headers) - set(df_efs.columns)))
print('Headers missing in efs:', len(missing_headers))
missing_headers_df=pd.DataFrame({'Headers missing in efs:': missing_headers})
output_data.append(missing_headers_df)

print("STAGE 3 COMPLETED in", round((time.time() - stage3), 2))
print("--------------")
print("Initialising STAGE 4 - Write data to CSV file")
stage6 = time.time()

output_data = pd.concat(output_data, ignore_index=True)

output_data.to_csv(output_file_path, index=False)

print('Write completed in', round((time.time() - stage6), 2))
print("--------------")
print('Time taken:', time.time() - start_time)
