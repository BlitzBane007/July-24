import PyPDF2
from tqdm import tqdm

def is_pdf_compressed(file_path):
    reader = PyPDF2.PdfReader(file_path)
    for page in tqdm(reader.pages, desc='Processing Pages', unit='row'):
        try:
            # Getting raw content of each page
            raw_content = page.get_contents()
            if raw_content:
                for obj in raw_content:
                    # The content stream can be a TextStringObject, byte stream, etc.
                    # We'll check if the object is a dictionary with a '/Filter' entry
                    if isinstance(obj, PyPDF2.generic.StreamObject) and "/Filter" in obj:
                        return True
        except Exception as e:
            # Some pages might not be parsed correctly
            print(f"Error processing page: {e}")
    return False

file_path = r"C:\Users\Aditya.Apte\OneDrive - FE fundinfo\Desktop\Desktop Icons\Aditya Apte\FILES\ISIN check\BNY Mellon Global Funds Plc - Interim R&A 2023 Final.pdf"  # replace with your PDF file path
print("Finding /Filter in object to detect compression")
print("File Name: BNY Mellon Global Funds Plc - Interim R&A 2023 Final.pdf")
if is_pdf_compressed(file_path):
    print("The PDF file is compressed.")
else:
    print("RESULT : The PDF file is not compressed.")