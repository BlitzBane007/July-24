import csv
from tqdm import tqdm# Open the CSV file
with open(r'C:\Users\Aditya.Apte\OneDrive - FE fundinfo\Desktop\Desktop Icons\Aditya Apte\FILES\BigFile\BigFile.csv','r') as file:
    # Create a CSV reader
    reader = csv.DictReader(file, delimiter=';')
    # Open the destination CSV file
    with open('SmallFile.csv', 'w', newline='') as dest_file:
        # Create a CSV writer
        writer = csv.DictWriter(dest_file, fieldnames=['ISIN', 'Language'])
        # Write the headers
        writer.writeheader()
        # Initialize a tqdm progress bar
        pbar = tqdm(reader)
        # Iterate over the rows in the source CSV file
        for row in pbar:
            # Check if the 'LanguageScript' column is blank
            if not row['LanguageScript']:
                if row['Language'] == 'zh':
                    # Write the 'ISIN' and 'Language' to the destination CSV file
                    writer.writerow({'ISIN': row['ISIN'], 'Language': row['Language']})


    # num_rows = sum(1 for row in reader)
    # print(num_rows)