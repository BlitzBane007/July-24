import csv
import json
import tkinter as tk
from tkinter import filedialog
import requests
import threading
from tqdm import tqdm
import re

def call_api(api_key, citicodes, text_area1, display_output):
    for citicode in tqdm(citicodes):
        url = f'https://datafeeds.fefundinfo.com/api/data/filtercheck/{api_key}?citiCode={citicode}'
        response = requests.get(url)
        data = json.loads(response.text)
        blocking_filters_tag = data['blockingFilters']
        write_to_csv(citicode, blocking_filters_tag)

        if display_output:
            text_area1.insert(tk.END, f"{citicode} - {blocking_filters_tag}\n")
            text_area1.update()

    if display_output:
        text_area1.insert(tk.END, "API calls completed and data stored.\n")
        text_area1.update()


def write_to_csv(citicode, blocking_filters_tag):
    with open(csv_file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([citicode, blocking_filters_tag])

def submit():
    api_key = api_key_entry.get()
    citicodes = citicodes_entry.get()

    global csv_file_path
    csv_file_path = filedialog.asksaveasfilename(defaultextension='.csv')

    text_area.delete('1.0', tk.END)  # Clear the text area

    # progress_bar = ttk.Progressbar(window, mode='determinate', length=300)
    # progress_bar.pack()

    display_output = output_checkbox_var.get()  # Get the value of the checkbox

    # Convert the input string to comma-separated values
    citicodes = citicodes.strip()
    citicodes = re.sub(r'\r?\n', ',', citicodes)
    values = citicodes.split(',')
    values = [val.strip() for val in values]
    comma_separated_values = ', '.join(values)

    # Update the entry field with the comma-separated values
    citicodes_entry.delete(0, tk.END)
    citicodes_entry.insert(tk.END, comma_separated_values)

    threading.Thread(target=call_api,
                     args=(api_key, values, text_area, display_output)).start()


# Create the GUI
window = tk.Tk()
window.title('API Call')
window.geometry('400x350')

api_key_label = tk.Label(window, text='API Key:')
api_key_label.pack()
api_key_entry = tk.Entry(window)
api_key_entry.pack()

citicodes_label = tk.Label(window, text='CitiCodes (comma-separated):')
citicodes_label.pack()
citicodes_entry = tk.Entry(window)
citicodes_entry.pack()

output_checkbox_var = tk.BooleanVar()
output_checkbox = tk.Checkbutton(window, text='Display Output', variable=output_checkbox_var)
output_checkbox.pack()

submit_button = tk.Button(window, text='Submit', command=submit)
submit_button.pack()

text_area = tk.Text(window, height=10, width=50)
text_area.pack()

window.mainloop()
