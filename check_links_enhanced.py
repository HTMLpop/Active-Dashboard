import pandas as pd
import requests
from requests.exceptions import RequestException, SSLError, Timeout, ConnectionError

# Load Excel
df = pd.read_excel("combined_master_with_urls.xlsx")

# Columns to check
url_columns = [
    'Masking Forms_URL',
    'Fraud/Alerts_URLs',
    'Public Records Request Form_URLs'
]

# Settings
HEADERS = {'User-Agent': 'Mozilla/5.0'}
TIMEOUT = 10

def is_url_working(url):
    try:
        if not isinstance(url, str) or not url.startswith("http"):
            return "Broken"
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=False)
        if response.status_code < 400:
            return "OK"
        return "Broken"
    except (RequestException, SSLError, Timeout, ConnectionError):
        return "Broken"

# Disable SSL warnings (optional)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Check each column
for col in url_columns:
    status_col = col.replace("_URL", "_Status").replace("URLs", "Status")
    df[status_col] = df[col].apply(is_url_working)

# Save output
df.to_excel("link_status_report.xlsx", index=False)
print("✅ Link check complete — results saved to 'link_status_report.xlsx'")
