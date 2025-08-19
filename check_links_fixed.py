import pandas as pd
import requests
import os
import json
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

EXCEL_INPUT = "combined_master_with_urls.xlsx"
EXCEL_OUTPUT = "broken_links.xlsx"
JSON_OUTPUT = "broken_links.json"

COLUMNS_TO_SCAN = [
    'Masking Forms_URL',
    'Fraud/Alerts_URLs',
    'Public Records Request Form_URLs'
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}

def check_link(url):
    try:
        response = requests.get(url, headers=HEADERS, allow_redirects=True, timeout=10, verify=False)
        return response.status_code
    except requests.exceptions.SSLError:
        return "SSL Error"
    except Exception as e:
        return str(e)

def main():
    if not os.path.exists(EXCEL_INPUT):
        print(f"❌ Excel file '{EXCEL_INPUT}' not found.")
        return

    try:
        df = pd.read_excel(EXCEL_INPUT, engine='openpyxl')
    except Exception as e:
        print("❌ Error reading Excel file:", e)
        return

    missing_cols = [col for col in COLUMNS_TO_SCAN if col not in df.columns]
    if missing_cols:
        print("❌ Missing columns:", missing_cols)
        return

    all_links = pd.concat([df[col].dropna() for col in COLUMNS_TO_SCAN])
    unique_links = all_links.unique()

    results = []
    for i, url in enumerate(unique_links):
        status = check_link(url)
        print(f"{i+1}/{len(unique_links)} - {url} --> {status}")
        if (isinstance(status, int) and not str(status).startswith("2")) or isinstance(status, str):
            results.append({"url": url, "status": status})

    # Always write both files — even if results is empty
    pd.DataFrame(results).to_excel(EXCEL_OUTPUT, index=False)
    with open(JSON_OUTPUT, "w") as json_file:
        json.dump(results, json_file, indent=2)

    print(f"✅ Broken links saved to {EXCEL_OUTPUT} and {JSON_OUTPUT}")

if __name__ == "__main__":
    main()