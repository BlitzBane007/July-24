import os.path
import pandas as pd
import xlwings as xw

user = input("Enter the Path of AERO_PTC: ")
latest = os.path.join(user, "Latest")
csv_out = os.path.join(user, "CSV_Out")
merged_book = os.path.join(user, "Merged_File1.xlsx")
salesforce = ""
amazon = ""
files = os.listdir(latest)
Sheet = "Merged_File"
table = "Merged"

for file in files:
    if "Contact Search Result" in file:
        amazon = os.path.join(latest, file)
    else:
        salesforce = os.path.join(latest, file)


# Read the CSV files
df1 = pd.read_csv(salesforce, usecols=['Case Number', 'Amazon Connect Contact Id'])
df2 = pd.read_csv(amazon, usecols=['Contact ID', 'Agent'])

# Merge the dataframes
merged_df = pd.merge(df1, df2, left_on='Amazon Connect Contact Id', right_on='Contact ID')

# Print the merged dataframe
print(merged_df)
# Open the workbook and select the sheet
book = xw.Book(merged_book)
sheet = book.sheets['Merged_File']  # replace 'SheetName' with the name of your sheet

# Select the table
table = sheet.api.ListObjects('Merged')  # replace 'TableName' with the name of your table
# Clear the contents of the table (but not the table itself)
table.DataBodyRange.ClearContents()

# Ensure the DataFrame has the same column order as the Excel table
merged_df = merged_df[['Case Number', 'Amazon Connect Contact Id', 'Agent']]

# Convert the DataFrame to a list of lists
rows = [list(row) for row in merged_df.itertuples(index=False)]

# Write the DataFrame to the table
for i, row in enumerate(rows, start=1):
    sheet.range(f'A{i + 1}').value = row

# Save and close the workbook
book.save()
book.close()