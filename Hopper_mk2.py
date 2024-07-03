import argparse
import os
import zipfile
from urllib.parse import urlparse
import dask.dataframe as dd
import pandas as pd
import requests
from pathlib import Path
import json
from tqdm import tqdm
from dask.diagnostics import ProgressBar


def main_loop():
    parser = argparse.ArgumentParser()
    parser.add_argument('--efs_url', help='The EFS URL')
    parser.add_argument('--hfi_url', help='The HFI URL')
    parser.add_argument('--feed_name', help='The Feed Name')
    args = parser.parse_args()
    efs_url = args.efs_url
    hfi_url = args.hfi_url
    feed_name = args.feed_name
    ProgressBar().register()
    efs_path = "C:/HOPPER/ZEFS.csv"
    hfi_path = "C:/HOPPER/AhFI.csv"

    path = "C:/HOPPER/"
    out_path = f"C:/HOPPER/Compare/{feed_name}.xlsx"
    automate_task(efs_url, hfi_url, path)
    process_files(efs_url, path, out_path, feed_name)
    ready_run(efs_path, hfi_path)


def automate_task(efs_url, hfi_url, download_path):
    print("Downloading EFS file")
    download_and_extract_zip(efs_url, download_path, "ZEFS.csv")
    print("Downloading hFI file")
    download_and_extract_zip(hfi_url, download_path, "AhFI.csv")
    download_enigma_csv(download_path)


def download_enigma_csv(download_path):
    file_enig = 'Enigma.csv'
    edir_path = os.path.dirname(download_path)
    enigma_path = os.path.join(edir_path, file_enig)
    if not os.path.exists(enigma_path):
        url = "https://datafeeds.fefundinfo.com/api/v1/Feeds/22a151f4-7937-4b3f-b3bd-1f440d14e62e/download?token" \
              "=f26136d9-21a8-4d62-9694-e11c1a14a40b"
        print("Updating Enigma")
        download_and_extract_zip(url, download_path, file_enig)


def download_and_extract_zip(url, path, new_filename):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading {url}: {e}")
        return

    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    total_size = int(response.headers.get('content-length', 0))
    zip_filename = os.path.join(path, filename)

    with open(zip_filename, 'wb') as file:
        progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)
        for data in response.iter_content(chunk_size=4096):
            file.write(data)
            progress_bar.update(len(data))
        progress_bar.close()

    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(path)

    extracted_files = zip_ref.namelist()
    for extracted_file in extracted_files:
        extracted_file_path = os.path.join(path, extracted_file)
        new_file_path = os.path.join(path, new_filename)
        os.rename(extracted_file_path, new_file_path)
        break

    os.remove(zip_filename)


def process_files(url, path, out_path, feed_name):
    # Get file paths
    print("Initializing Stage 2")
    file1_path = Path(path) / "ZEFS.csv"
    file2_path = Path(path) / "AhFI.csv"
    blocks = []
    citi = []
    feedname = []
    head3 = "Blocking Filters"
    head4 = "Citicode"
    head5 = "Feed Name"

    if not file1_path.exists() or not file2_path.exists():
        print("Both 'ZEFS.csv' and 'AhFI.csv' files are required.")
        return

    print("Reading Data files")
    df_efs = dd.read_csv(file1_path, header=0, sep=';', low_memory=False, assume_missing=True, blocksize=1e9,
                         skiprows=[1], dtype=str)
    df_hfi = dd.read_csv(file2_path, header=0, sep=';', low_memory=False, assume_missing=True, blocksize=1e9,
                         skiprows=[1], dtype=str)
    enigma_path = "C:/HOPPER/Enigma.csv"
    df_enig = pd.read_csv(enigma_path, delimiter=';')

    # Merge DataFrames and then compute
    df_combined = dd.merge(df_efs, df_hfi, how='outer')
    df_combined = df_combined.compute()

    # Get the union of unique headers
    union_headers = df_combined.columns.tolist()

    # Get headers missing in df_efs
    missing_headers = list(set(union_headers) - set(df_efs.columns))
    missing_headers_df = pd.DataFrame({'Headers': missing_headers})

    # Get ISIN missing in df_efs
    missing_isins = list(set(df_hfi["OFST020000"].unique()) - set(df_efs["OFST020000"].unique()))
    missing_isin_df = pd.DataFrame({'ISIN': missing_isins})
    print("Filter check initiated")

    api = extract_api_from_url(url)
    for missing_isin in tqdm(missing_isins):
        citicode = find_associated_value(df_enig, missing_isin)
        if str(citicode) == "None":
            citi.append("No citicode on DB")
            blocks.append("NA")
            feedname.append(feed_name)
        else:
            citi.append(str(citicode))
            blocker = call_api(api, str(citicode))
            blocks.append(blocker)
            feedname.append(feed_name)
    citi_df = pd.DataFrame({head4: citi})
    blocker_df = pd.DataFrame({head3: blocks})
    feedname_df = pd.DataFrame({head5: feedname})

    output_data = pd.concat([feedname_df, missing_headers_df, missing_isin_df, citi_df, blocker_df], axis=1)

    print("Writing to Excel file")
    with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
        output_data.to_excel(writer, index=False, sheet_name='Sheet1')
        worksheet = writer.sheets['Sheet1']
        num_rows, num_cols = output_data.shape
        custom_table_name = feed_name
        worksheet.add_table(0, 0, num_rows - 1, num_cols - 1,
                            {'columns': [{'header': col} for col in output_data.columns], 'name': custom_table_name})


def extract_api_from_url(url):
    parsed_url = urlparse(url)
    path_segments = parsed_url.path.split('/')
    api = path_segments[-2]  # Extract the second-to-last segment

    return api


def find_associated_value(df, value_ofst020000):
    filtered_df = df[df['OFST020000'] == value_ofst020000]
    associated_value = None
    if not filtered_df.empty:
        associated_value = filtered_df['OFST900174'].iloc[0]  # Assuming there's only one associated value
    return associated_value


def call_api(api_key, citicode):
    url = f'https://datafeeds.fefundinfo.com/api/data/filtercheck/{api_key}?citiCode={citicode}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = json.loads(response.text)
        blocking_filters_tag = data['blockingFilters']
        return str(blocking_filters_tag)
    except requests.exceptions.RequestException as e:
        print(f"Error while calling API: {e}")
        return "NA"


def ready_run(efs_path, hfi_path):
    os.remove(efs_path)
    os.remove(hfi_path)


if __name__ == "__main__":
    main_loop()
