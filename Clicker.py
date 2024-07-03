import csv

# Step 2: Increase the CSV field size limit
csv.field_size_limit(262144)  # Adjust this as necessary

# Step 3: Open the CSV file and create a reader object
with open(r'C:\Users\Aditya.Apte\OneDrive - FE fundinfo\Desktop\Desktop Icons\Aditya Apte\AERO\TRINITY\35a88116-96ef-461a-8f9b-8c04af286cd5\35a88116-96ef-461a-8f9b-8c04af286cd5_CT.csv', 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    
    # Step 5: Initialize a counter variable
    row_number = 0
    
    # Step 6: Iterate over the rows in the CSV file
    try:
        for row in csv_reader:
            row_number += 1  # Step 7: Increment the counter
            # Process each row
    except csv.Error:
        # Step 8: Print the current row number
        print(f"CSV error on row {row_number}")
