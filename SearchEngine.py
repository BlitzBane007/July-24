import tkinter as tk
from tkinter import filedialog, ttk, scrolledtext
import csv
import pandas as pd
import dask.dataframe as dd
import os
from threading import Thread


def browse_file():
    try:
        file_path = filedialog.askopenfilename(filetypes=[('CSV Files', '*.csv'), ('Excel Files', '*.xlsx')])
        if file_path and os.path.isfile(file_path):
            file_label.config(text=file_path)
    except Exception as e:
        output_text.insert(tk.END, f"Error: {str(e)}\n")


def browse_output_file(entry):
    try:
        filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[('CSV Files', '*.csv')])
        if filename:
            entry.delete(0, tk.END)
            entry.insert(tk.END, filename)
    except Exception as e:
        output_text.insert(tk.END, f"Error: {str(e)}\n")


def process_partition(partition, keywords):
    matched_keywords = set()
    keyword_list = keywords.split(',')
    for keyword in keyword_list:
        found_match = partition.apply(lambda row: any(keyword.lower() in str(field).lower() for field in row), axis=1)
        if found_match.any():
            matched_keywords.add(keyword)
    return pd.Series([matched_keywords])


def search_keywords():
    def run():
        # Disable search button
        search_button.config(state=tk.DISABLED)
        file_path = file_label.cget("text")
        keywords = keyword_entry.get()
        delimiter = delimiter_entry.get() or ','

        if not file_path or not keywords:
            output_text.insert(tk.END, "Error: File path or keywords are missing.\n")
            return

        total_keywords = len(keywords.split(','))
        progress_bar['maximum'] = total_keywords
        progress_bar['value'] = 0
        progress_label.config(text="Progress: 0/0")

        output_path = entry_output_file.get()
        if not output_path:
            output_text.insert(tk.END, "Error: Output file path is missing.\n")
            return

        output_text.delete(1.0, tk.END)

        try:
            if file_path.endswith('.csv'):
                df = dd.read_csv(file_path, assume_missing=True, delimiter=delimiter, dtype=str)
            elif file_path.endswith('.xlsx'):
                try:
                    df = dd.from_pandas(pd.read_excel(file_path, dtype=str), npartitions=10)
                except Exception as e:
                    output_text.insert(tk.END, f"Error reading Excel file: {str(e)}\n")
                    return
            else:
                output_text.insert(tk.END, "Error: Invalid file type. Please select a CSV or XLSX file.\n")
                return

            matched_keywords = df.map_partitions(process_partition, keywords, meta=pd.Series(dtype=object))

            with open(output_path, 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file, delimiter=delimiter)
                headers = df.columns.tolist()
                writer.writerow(headers)
                for index, row in enumerate(df[df.apply(lambda r:
                                                        any(keyword.lower() in str(field).lower() for field in r for keyword in
                                                            keywords.split(',')), axis=1)].compute().iterrows()):
                    writer.writerow(row[1])
                    progress_bar['value'] = index + 1
                    progress_label.config(text=f"Progress: {index + 1}/{total_keywords}")

            output_text.insert(tk.END, f"Progress: Searching for keywords in {file_path}\n")
            output_text.insert(tk.END, f"Number of keywords: {total_keywords}\n")
            output_text.insert(tk.END, f"\nKeywords with matches:\n")
            output_text.insert(tk.END, f"{', '.join(matched_keywords)}\n")
        except Exception as e:
            output_text.insert(tk.END, f"Error: {str(e)}\n")

        # Enable search button
        search_button.config(state=tk.NORMAL)

    Thread(target=run).start()


# Create the main window
window = tk.Tk()
window.title("CSV/XLSX Search Tool")

# Create frames for better organization
file_frame = ttk.Frame(window)
keyword_frame = ttk.Frame(window)
delimiter_frame = ttk.Frame(window)
output_frame = ttk.Frame(window)
progress_frame = ttk.Frame(window)

# Create widgets for file path
file_label = ttk.Label(file_frame, text="File:")
browse_button = ttk.Button(file_frame, text="Browse", command=browse_file)
file_label.pack(side=tk.LEFT, padx=5)
browse_button.pack(side=tk.LEFT, padx=5)

# Create widgets for keywords
keyword_label = ttk.Label(keyword_frame, text="Keywords:")
keyword_entry = ttk.Entry(keyword_frame)
keyword_label.pack(side=tk.LEFT, padx=5)
keyword_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

# Create widgets for delimiter
delimiter_label = ttk.Label(delimiter_frame, text="Delimiter:")
delimiter_entry = ttk.Entry(delimiter_frame, width=5)
delimiter_label.pack(side=tk.LEFT, padx=5)
delimiter_entry.pack(side=tk.LEFT, padx=5)

# Create widgets for output file
label_output_file = ttk.Label(output_frame, text="Output File:")
entry_output_file = ttk.Entry(output_frame)
button_browse_output_file = ttk.Button(output_frame, text="Browse",
                                       command=lambda: browse_output_file(entry_output_file))
label_output_file.pack(side=tk.LEFT, padx=5)
entry_output_file.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
button_browse_output_file.pack(side=tk.LEFT, padx=5)

# Create widgets for search button
search_button = ttk.Button(window, text="Search", command=search_keywords)

# Create widgets for output text
output_text = scrolledtext.ScrolledText(window, height=10, width=50)

# Create widgets for progress bar
progress_label = ttk.Label(progress_frame, text="Progress:")
progress_bar = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, length=200, mode='determinate')
progress_label.pack(side=tk.LEFT, padx=5)
progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

# Arrange frames in the grid
file_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
keyword_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=5)
delimiter_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=5)
output_frame.grid(row=3, column=0, sticky="ew", padx=10, pady=5)
search_button.grid(row=4, column=0, pady=10)
output_text.grid(row=5, column=0, padx=10, pady=5)
progress_frame.grid(row=6, column=0, sticky="ew", padx=10, pady=5)

# Configure the column to expand
window.grid_columnconfigure(0, weight=1)

# Start the GUI event loop
window.mainloop()
