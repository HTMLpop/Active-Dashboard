import pandas as pd
import requests
import json
import urllib3
from requests.exceptions import RequestException, SSLError, Timeout, ConnectionError
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning
urllib3.disable_warnings(category=InsecureRequestWarning)

# Load your Excel file
INPUT_FILE = "combined_master_with_urls.xlsx"
OUTPUT_EXCEL = "broken_links.xlsx"
OUTPUT_JSON = "broken_links.json"
TIMEOUT = 10  # seconds

# Read Excel data
df = pd.read_excel(INPUT_FILE)

# Ensure 'Link' column exists
if 'Link' not in df.columns:
    raise ValueError("The Excel file must have a column named 'Link'.")

# Keep track of URL status
link_status = {}

# Check each URL
for i, url in enumerate(df['Link']):
    if pd.isna(url) or not isinstance(url, str):
        link_status[url] = "broken"
        continue

    print(f"Checking ({i + 1}/{len(df)}): {url}")
    try:
        response = requests.head(url, allow_redirects=True, timeout=TIMEOUT, verify=False)
        if response.status_code >= 400:
            # Retry with GET to avoid false positives
            response = requests.get(url, allow_redirects=True, timeout=TIMEOUT, verify=False)
        
        if response.status_code < 400:
            link_status[url] = "working"
        else:
            link_status[url] = "broken"
    except (RequestException, SSLError, Timeout, ConnectionError):
        link_status[url] = "broken"

# Save broken links to Excel
broken_links = [url for url, status in link_status.items() if status == "broken"]
df_broken = df[df['Link'].isin(broken_links)]
df_broken.to_excel(OUTPUT_EXCEL, index=False)

# Save status to JSON
with open(OUTPUT_JSON, 'w') as f:
    json.dump(link_status, f, indent=2)

print("✅ Done. Broken links saved to:", OUTPUT_EXCEL, "and", OUTPUT_JSON)
