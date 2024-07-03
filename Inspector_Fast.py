import tkinter as tk
from tkinter import filedialog, messagebox
import csv
from tqdm import tqdm


# Function to browse for a CSV file
def browse_file():
    global csv_file_path
    filename = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if filename:
        csv_file_path.set(filename)


def search_keyword():
    try:
        with open(csv_file_path.get(), mode='r', encoding=encoding_var.get()) as file:
            counter = 0
            reader = csv.reader(file, delimiter=delimiter_var.get())
            header = next(reader)  # Read the header row
            print(header)  # Print the header row
            found_rows = [header]  # Initialize list with header for the new CSV
            header = next(reader)  # Read the header row
            print(header)  # Print the header row
            found_rows.append(header)  # Initialize list with header for the new CSV

            for row in tqdm(reader, desc='Searching Rows', unit='Rows'):
                if any(keyword_entry.get() in cell for cell in row):
                    print(row)
                    counter = counter+1
                    found_rows.append(row)  # Add the found row to the list

            # Write to a new CSV file
            with open(r'C:\Inspect_Fast\found_rows.csv', mode='w', newline='', encoding=encoding_var.get()) as new_file:
                writer = csv.writer(new_file, delimiter=',')
                writer.writerows(found_rows)  # Write all rows at once
            print('Search Completed!')
            print(f'Rows found = {counter}')
            exit(0)

    except Exception as e:
        messagebox.showerror("Error", str(e))


# Main application window
root = tk.Tk()
root.title("CSV Keyword Search")
root.geometry('200x150')

# Variables
csv_file_path = tk.StringVar()
keyword_entry = tk.StringVar()
delimiter_var = tk.StringVar(value=',')
encoding_var = tk.StringVar(value='utf-8-sig')

# Layout
tk.Button(root, text="Browse CSV File", command=browse_file).pack()
tk.Entry(root, textvariable=keyword_entry).pack()
tk.OptionMenu(root, delimiter_var, ',', ';', '|').pack()
tk.OptionMenu(root, encoding_var, 'utf-8-sig', 'cp1252').pack()
tk.Button(root, text="Search", command=search_keyword).pack()

# Run the application
root.mainloop()
