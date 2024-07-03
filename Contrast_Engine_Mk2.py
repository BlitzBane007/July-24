import csv
import sys
import time
import tkinter as tk
from tkinter import filedialog, messagebox

from tqdm import tqdm

OFST020000 = 'OFST020000'


def find_headers(file):
    headers_candidates = [next(file) for _ in range(2)]
    for i, candidate in enumerate(headers_candidates):
        if OFST020000 in candidate:  # Check if typical header value is in this row
            return i, candidate
    raise ValueError("Headers not found in the first two lines of the file.")


def select_efs_file():
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    efs_file_label.config(text=file_path)


def select_hfi_file():
    file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
    hfi_file_label.config(text=file_path)


def compare_data():
    efs_file_path = efs_file_label.cget("text")
    hfi_file_path = hfi_file_label.cget("text")
    if not efs_file_path or not hfi_file_path:
        messagebox.showerror("Error", "Please select both EFS and HFI files.")
        return

    output_file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV Files", "*.csv")])
    if not output_file_path:
        return

    try:
        with open(efs_file_path, "r") as efs_file, open(hfi_file_path, "r") as hfi_file, open(
                output_file_path, "w", newline="", encoding="utf-8") as output_file:
            efs_reader = csv.reader(efs_file, delimiter=";")
            hfi_reader = csv.reader(hfi_file, delimiter=",")
            output_writer = csv.writer(output_file, delimiter=",")

            efs_line, efs_headers = find_headers(efs_reader)
            hfi_line, hfi_headers = find_headers(hfi_reader)

            if efs_line == 1:  # If headers are in the second row, remove the first line from efs_data
                next(efs_reader)
            if hfi_line == 1:  # If headers are in the second row, remove the first line from hfi_data
                next(hfi_reader)

            output_writer.writerow(["isin", "header", "efs", "hfi"])

            efs_data = {row[efs_headers.index(OFST020000)]: row for row in efs_reader}
            hfi_data = {row[hfi_headers.index(OFST020000)]: row for row in hfi_reader}

            total_headers = len(efs_headers)

            # Create the progress bar
            progress_bar = tqdm(total=total_headers, desc="Processing Headers", unit="Header", file=sys.stdout)
            start_time = time.time()

            # Iterate over headers and update the progress bar
            for header_index, efs_header in enumerate(efs_headers):
                if efs_header != OFST020000:
                    if efs_header in hfi_headers:
                        for isin, efs_row in efs_data.items():
                            if isin in hfi_data:
                                hfi_row = hfi_data[isin]
                                efs_value = efs_row[efs_headers.index(efs_header)].lower()
                                hfi_value = hfi_row[hfi_headers.index(efs_header)].lower()
                                if efs_value != hfi_value:
                                    output_writer.writerow([isin, efs_header, efs_value, hfi_value])

                # Update the progress bar value
                progress_bar.update(1)

            progress_bar.close()
            end_time = time.time()
            messagebox.showinfo("Done", "Compare completed!")
            time_taken = end_time - start_time
            print('Time taken:', time_taken)
            exit()
    except FileNotFoundError:
        messagebox.showerror("Error", "File not found.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {str(e)}")


# Create the GUI window
window = tk.Tk()
window.title("CSV Comparison Tool")

# Create the "Select EFS File" button
efs_button = tk.Button(window, text="Select EFS File", command=select_efs_file)
efs_button.pack(pady=10)

# Create a label to display the selected EFS file path
efs_file_label = tk.Label(window, text="")
efs_file_label.pack()

# Create the "Select HFI File" button
hfi_button = tk.Button(window, text="Select HFI File", command=select_hfi_file)
hfi_button.pack(pady=10)

# Create a label to display the selected HFI file path
hfi_file_label = tk.Label(window, text="")
hfi_file_label.pack()

# Create the "Compare Data" button
compare_button = tk.Button(window, text="Compare Data", command=compare_data)
compare_button.pack(pady=10)

# Start the GUI event loop
window.mainloop()
