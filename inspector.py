import tkinter as tk
from tkinter import filedialog
from tkinter import ttk
import csv
import openpyxl

import pandas as pd

missing = []


def browse_file():
    file_path = filedialog.askopenfilename(filetypes=[('CSV Files', '*.csv'), ('Excel Files', '*.xlsx')])
    file_label.config(text=file_path)
    # Read the headers and populate the dropdown list
    # headers = get_file_headers(file_path)
    # header_combobox['values'] = headers


def browse_output_file(entry):
    filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[('CSV Files', '*.csv')])
    entry.delete(0, tk.END)
    entry.insert(tk.END, filename)


def get_file_headers(file_path):
    if file_path.endswith('.csv'):
        with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
            reader = csv.reader(csv_file)
            headers = next(reader)
            return headers
    elif file_path.endswith('.xlsx'):
        df = pd.read_excel(file_path)
        headers = df.columns.tolist()
        return headers
    return []


def search_keywords():
    file_path = file_label.cget("text")
    keywords = keyword_entry.get()
    delimiter = delimiter_entry.get()
    # selected_header = header_combobox.get()

    total_keywords = 0  # Initialize total_keywords to 0
    matched_keywords = set()  # Initialize matched_keywords to an empty set

    if file_path.endswith('.csv'):
        with open(file_path, 'r', encoding='utf-8-sig') as csv_file:
            reader = csv.reader(csv_file, delimiter=delimiter)
            headers = next(reader)
            rows = []
            total_keywords = len(keywords.split(','))
            progress = 0

            progress_bar['maximum'] = total_keywords

            for keyword in keywords.split(','):
                found_match = False
                for row in reader:
                    if any(keyword in field for field in row):
                        rows.append(row)
                        found_match = True
                if not found_match:
                    matched_keywords.add(keyword)
                progress += 1
                progress_bar['value'] = progress
                progress_label.config(text=f"Progress: {progress}/{total_keywords}")
                csv_file.seek(0)  # Reset the reader to the beginning of the file for each keyword search

    elif file_path.endswith('.xlsx'):
        wb = openpyxl.load_workbook(file_path)
        sheet = wb.active
        rows = []
        total_keywords = len(keywords.split(','))
        progress = 0

        progress_bar['maximum'] = total_keywords

        for keyword in keywords.split(','):
            found_match = False
            for row in sheet.iter_rows(values_only=True):
                if any(keyword in str(field) for field in row):
                    rows.append(row)
                    found_match = True
            if not found_match:
                matched_keywords.add(keyword)
            progress += 1
            progress_bar['value'] = progress
            progress_label.config(text=f"Progress: {progress}/{total_keywords}")

    # Process the rows list containing matched keywords if you need to do something with them

    # The rest of your code goes here...

    output_text.delete(1.0, tk.END)
    output_text.insert(tk.END, f"Progress: Searching for keywords in {file_path}\n")
    output_text.insert(tk.END, f"Number of keywords: {total_keywords}\n")
    output_text.insert(tk.END, f"\nNumber of matching rows: {len(rows)}\n")
    output_text.insert(tk.END, f"\nKeywords with no matches:\n")
    output_text.insert(tk.END, f"{', '.join(matched_keywords)}\n")
    output_text.insert(tk.END, "\nMatching Rows:\n")
    output_path = entry_output_file.get()
    with open(output_path, 'a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file, delimiter=',')
        writer.writerow(headers)

    for row in rows:
        with open(output_path, 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter=',')
            writer.writerow(row)
        # output_text.insert(tk.END, f"{row}\n")


# Create the main window
window = tk.Tk()
window.title("CSV/XLSX Search Tool")

# Create widgets
file_label = ttk.Label(window, text="No file selected")
browse_button = ttk.Button(window, text="Browse", command=browse_file)
keyword_label = ttk.Label(window, text="Keywords (comma-separated):")
keyword_entry = ttk.Entry(window)
delimiter_label = ttk.Label(window, text="Delimiter (if CSV):")
delimiter_entry = ttk.Entry(window)
# header_label = ttk.Label(window, text="Select Header:")
# header_combobox = ttk.Combobox(window)
search_button = ttk.Button(window, text="Search", command=search_keywords)
output_text = tk.Text(window, height=10, width=50)
output_text.config(insertbackground='black')

# Create progress bar
progress_frame = ttk.Frame(window)
progress_label = ttk.Label(progress_frame, text="Progress: 0/0")
progress_bar = ttk.Progressbar(progress_frame, mode='determinate')

# Arrange widgets in the grid
file_label.grid(row=0, column=0, columnspan=2, pady=10)
browse_button.grid(row=0, column=2, pady=10)
keyword_label.grid(row=1, column=0, sticky=tk.W, pady=10)
keyword_entry.grid(row=1, column=1, columnspan=2, pady=10)
delimiter_label.grid(row=2, column=0, sticky=tk.W, pady=10)
delimiter_entry.grid(row=2, column=1, columnspan=2, pady=10)
# Output file
label_output_file = tk.Label(window, text="Output File:")
label_output_file.grid(row=3, column=0, sticky="w")

entry_output_file = tk.Entry(window, width=50)
entry_output_file.grid(row=3, column=1, padx=5)

button_browse_output_file = tk.Button(window, text="Browse", command=lambda: browse_output_file(entry_output_file))
button_browse_output_file.grid(row=3, column=2, padx=5)

search_button.grid(row=4, column=0, columnspan=3, pady=10)
output_text.grid(row=6, column=0, columnspan=3, pady=10)

# Grid layout for progress bar
progress_frame.grid(row=7, column=0, columnspan=3, pady=10)
progress_label.pack(side=tk.LEFT, padx=5)
progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

# Start the GUI event loop
window.mainloop()
