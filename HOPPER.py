import argparse
import os
import zipfile
from tkinter import messagebox
from urllib.parse import urlparse

import dask.dataframe as dd
import pandas as pd
import requests


def automate_task(efs_url, hfi_url, download_path):
    efs_file = "ZEFS.csv"
    hfi_file = "AhFI.csv"
    download_and_extract_zip(efs_url, download_path, efs_file)
    download_and_extract_zip(hfi_url, download_path, hfi_file)


def download_and_extract_zip(url, path, new_filename):
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)

    response = requests.get(url, stream=True)

    zip_filename = os.path.join(path, filename)
    with open(zip_filename, 'wb') as file:
        for data in response.iter_content(chunk_size=4096):
            file.write(data)

    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(path)

    extracted_files = zip_ref.namelist()
    for extracted_file in extracted_files:
        extracted_file_path = os.path.join(path, extracted_file)
        new_file_path = os.path.join(path, new_filename)
        os.rename(extracted_file_path, new_file_path)
        break

    os.remove(zip_filename)


def process_files(path, out_path, feed_name):
    # Get file path
    file1_path = ""
    file2_path = ""
    output_data = []
    if os.path.exists(path):
        files = os.listdir(path)
        if len(files) >= 2:
            for file in files:
                if file == "AhFI.csv":
                    file2_path = "C:/HOPPER/AhFI.csv"
                if file == "ZEFS.csv":
                    file1_path = "C:/HOPPER/ZEFS.csv"

            df_efs = dd.read_csv(file1_path, header=0, sep=';', low_memory=False, assume_missing=True,
                                 blocksize=1e9,
                                 skiprows=[1], dtype=str)

            df_hfi = dd.read_csv(file2_path, header=0, sep=';', low_memory=False, assume_missing=True,
                                 blocksize=1e9,
                                 skiprows=[1], dtype=str)

            df_efs = df_efs.compute()

            df_hfi = df_hfi.compute()

            # Get the union of unique headers
            union_headers = pd.unique(df_efs.columns.union(df_hfi.columns))
            # Convert the union_headers to a list if needed
            headers = union_headers.tolist()

            # Get headers missing in df_efs
            missing_headers = pd.Series(list(set(headers) - set(df_efs.columns)))
            head1 = 'Headers'
            missing_headers_df = pd.DataFrame({head1: missing_headers})
            output_data.append(missing_headers_df)

            # Get ISIN missing in df_efs
            missing_isins = pd.Series(list(set(df_hfi["OFST020000"].unique()) - set(df_efs["OFST020000"].unique())))
            head2 = 'ISIN'
            missing_isin_df = pd.DataFrame({head2: missing_isins})
            output_data.append(missing_isin_df)
        else:
            messagebox.showwarning("Warning", f"The directory '{path}' does not contain two files.")
    else:
        messagebox.showwarning("Warning", f"The directory path '{path}' does not exist.")

    output_data = pd.concat(output_data, axis=1)

    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        output_data.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        num_rows, num_cols = output_data.shape
        custom_table_name = feed_name
        worksheet.add_table(0, 0, num_rows - 1, num_cols - 1, {'columns': [{'header': col} for col in output_data.columns], 'name': custom_table_name})


def main_loop():
    parser = argparse.ArgumentParser()
    parser.add_argument('--efs_url', help='The EFS URL')
    parser.add_argument('--hfi_url', help='The HFI URL')
    parser.add_argument('--feed_name', help='The Feed Name')
    args = parser.parse_args()
    # efs_url = args.efs_url
    # hfi_url = args.hfi_url
    # feed_name = args.feed_name
    efs_url = "https://datafeeds.fefundinfo.com/api/v1/Feeds/cf25d872-c939-49d8-b004-1c9102c3d855/download?token=15f9ec54-4f04-4f0a-be3f-591b2a08fa89"
    hfi_url = "https://api.fundinfo.com/4.0/feed/bfc178be-478f-45b2-b42e-d56d075bf368/ZIP"
    feed_name = "Schroder"

    path = "C:/HOPPER/"
    out_path = f"C:/HOPPER/Compare/{feed_name}.xlsx"
    automate_task(efs_url, hfi_url, path)
    process_files(path, out_path, feed_name)


main_loop()
