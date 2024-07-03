import threading
import tkinter as tk
from tkinter import filedialog, messagebox
import csv
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm


def browse_file():
    global csv_file_path
    filename = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if filename:
        csv_file_path.set(filename)


def process_sub_chunk(sub_chunk, keyword):
    for row in sub_chunk:
        if any(keyword in cell for cell in row):
            print(row)
            return row


def process_chunk(chunk, keyword):
    num_threads = 10
    sub_chunk_size = len(chunk) // num_threads
    sub_chunks = [chunk[i:i + sub_chunk_size] for i in range(0, len(chunk), sub_chunk_size)]

    found_rows = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_sub_chunk, sub_chunk, keyword) for sub_chunk in sub_chunks]
        for future in futures:
            found_rows.extend(future.result())

    return found_rows


def search_keyword():
    try:
        output_path = r'C:\Inspect_Fast\found_rows.csv'
        chunk_size = 100000
        keyword = keyword_entry.get()
        delimiter = delimiter_var.get()

        with open(csv_file_path.get(), mode='r', encoding=encoding_var.get()) as file, open(output_path, mode='w',
                                                                                            newline='',
                                                                                            encoding=encoding_var.get()) as new_file:
            reader = csv.reader(file, delimiter=delimiter)
            writer = csv.writer(new_file, delimiter=',')

            header = next(reader)
            writer.writerow(header)

            while True:
                chunk = [next(reader, None) for _ in range(chunk_size)]
                chunk = [row for row in chunk if row]  # Filter out None values from next()

                if not chunk:
                    break

                found_rows = process_chunk(chunk, keyword)

                for row in found_rows:
                    writer.writerow(row)

        print(f'Search Completed!')
    except Exception as e:
        messagebox.showerror("Error", str(e))


def run_search():
    search_thread = threading.Thread(target=search_keyword)
    search_thread.start()


root = tk.Tk()
root.title("CSV Keyword Search")
root.geometry('200x150')

csv_file_path = tk.StringVar()
keyword_entry = tk.StringVar()
delimiter_var = tk.StringVar(value=',')
encoding_var = tk.StringVar(value='utf-8-sig')

tk.Button(root, text="Browse CSV File", command=browse_file).pack()
tk.Entry(root, textvariable=keyword_entry).pack()
tk.OptionMenu(root, delimiter_var, ',', ';', '|').pack()
tk.OptionMenu(root, encoding_var, 'utf-8-sig', 'cp1252').pack()
tk.Button(root, text="Search", command=run_search).pack()

root.mainloop()
