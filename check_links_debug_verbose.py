import pandas as pd
import requests
import urllib3
from openpyxl import Workbook

# Suppress only the single InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# File and columns
excel_file = "combined_master_with_urls.xlsx"
columns_to_check = ["Masking Forms_URL", "Fraud/Alerts_URLs", "Public Records Request Form_URLs"]

# Read Excel
try:
    df = pd.read_excel(excel_file)
except Exception as e:
    print(f"❌ Failed to read Excel file: {e}")
    raise

# Result list
results = []

# Checker function
def check_url(url):
    try:
        response = requests.get(url, timeout=8, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        return response.status_code
    except requests.exceptions.SSLError:
        return "SSL Error"
    except requests.exceptions.RequestException as e:
        return str(e)

# Loop through each column
for column in columns_to_check:
    if column not in df.columns:
        print(f"⚠️ Column '{column}' not found in the sheet.")
        continue

    for idx, url in enumerate(df[column].dropna()):
        url = str(url).strip()
        if not url.startswith("http"):
            continue

        print(f"🔍 Checking [{column}] row {idx+1}: {url}")
        status = check_url(url)
        print(f"    → Status: {status}")
        results.append({"Column": column, "Row": idx+1, "URL": url, "Status": status})

# Save to Excel
output_df = pd.DataFrame(results)
output_df.to_excel("broken_links_verbose.xlsx", index=False)
print("✅ Done. Results saved to 'broken_links_verbose.xlsx'")
