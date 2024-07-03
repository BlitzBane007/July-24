import requests
from requests.auth import HTTPBasicAuth

# Define the URL and parameters for the GET request
url = "http://dsys/research/report.asmx/excellink"
params = {'name': 'FINEX_FR_FEED_LIST'}

# Replace 'your_username' and 'your_password' with your actual credentials
username = 'aditya.apte'
password = 'IndiaAVG@123456'

# Define custom headers as observed from the browser's successful request
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Host': 'dsys',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# Send the GET request with basic authentication and custom headers
response = requests.get(url, params=params, auth=HTTPBasicAuth(username, password), headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Process the content of the response
    data = response.content
    # You can now use the 'data' variable as needed
else:
    print(f"Failed to retrieve data: {response.status_code}")
