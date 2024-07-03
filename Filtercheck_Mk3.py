import asyncio
import aiohttp
import csv
import time
import tkinter as tk
from tkinter import filedialog
from tkinter import ttk


async def call_api(api_key, citicodes, progress_var):
    total_isins = len(citicodes)
    data_to_write = []  # Variable to store the data
    start_time = time.time()  # Record the start time
    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, citicode in enumerate(citicodes, start=1):
            url = f'https://datafeeds.fefundinfo.com/api/data/filtercheck/{api_key}?citiCode={citicode}'
            task = asyncio.ensure_future(
                fetch_data(session, url, citicode, progress_var, index, total_isins, data_to_write))
            tasks.append(task)
        await asyncio.gather(*tasks)

    write_to_csv(data_to_write)  # Write the data to the file
    elapsed_time = time.time() - start_time  # Calculate the total elapsed time
    progress_var.set(f"API calls completed in {elapsed_time:.2f} seconds")
    window.update()


async def fetch_data(session, url, citicode, progress_var, index, total_isins, data_to_write):
    async with session.get(url) as response:
        data = await response.json()
        blocking_filters_tag = data['blockingFilters']
        data_to_write.append([citicode, blocking_filters_tag])  # Append data to the list
        progress_var.set(f"{index}/{total_isins} ISINs processed")
        window.update()


def write_to_csv(data):
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)


# noinspection PyGlobalUndefined
def submit():
    api_key = api_key_entry.get()
    citicodes = citicodes_entry.get().split(',')

    global csv_file_path
    csv_file_path = filedialog.asksaveasfilename(defaultextension='.csv')

    progress_var = tk.StringVar()
    progress_bar = ttk.Progressbar(window, mode='determinate', length=300)
    progress_bar.pack()
    progress_label = tk.Label(window, textvariable=progress_var)
    progress_label.pack()

    asyncio.run(call_api(api_key, citicodes, progress_var))

    status_label.configure(text='API calls completed and data stored.')
    progress_bar.destroy()
    progress_label.destroy()


# Create the GUI
window = tk.Tk()
window.title('API Call')
window.geometry('400x300')

api_key_label = tk.Label(window, text='API Key:')
api_key_label.pack()
api_key_entry = tk.Entry(window)
api_key_entry.pack()

citicodes_label = tk.Label(window, text='CitiCodes (comma-separated):')
citicodes_label.pack()
citicodes_entry = tk.Entry(window)
citicodes_entry.pack()

submit_button = tk.Button(window, text='Submit', command=submit)
submit_button.pack()

status_label = tk.Label(window, text='')
status_label.pack()

window.mainloop()
