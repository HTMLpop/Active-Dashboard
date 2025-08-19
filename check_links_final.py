import pandas as pd
import requests
import os

EXCEL_INPUT = "combined_master_with_urls.xlsx"
EXCEL_OUTPUT = "broken_links.xlsx"
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
        return "SSL Certificate Error"
    except Exception as e:
        return str(e)

def main():
    print("📄 Looking for file:", EXCEL_INPUT)
    if not os.path.exists(EXCEL_INPUT):
        print("❌ ERROR: Excel file not found.")
        return

    try:
        df = pd.read_excel(EXCEL_INPUT, engine='openpyxl')
        print("✅ File loaded successfully.")
    except Exception as e:
        print("❌ ERROR reading Excel file:", e)
        return

    missing_columns = [col for col in COLUMNS_TO_SCAN if col not in df.columns]
    if missing_columns:
        print("❌ ERROR: Missing expected columns:", missing_columns)
        return

    all_links = pd.concat([df[col].dropna() for col in COLUMNS_TO_SCAN])
    unique_links = all_links.unique()

    print(f"🔗 Checking {len(unique_links)} unique URLs using GET with headers and SSL bypass...")
    results = []
    for i, url in enumerate(unique_links):
        status = check_link(url)
        print(f"{i+1}/{len(unique_links)} - {url} --> {status}")
        if (isinstance(status, int) and not str(status).startswith("2")) or isinstance(status, str):
            results.append({"URL": url, "Status": status})

    result_df = pd.DataFrame(results)
    result_df.to_excel(EXCEL_OUTPUT, index=False)
    print(f"✅ Broken links saved to {EXCEL_OUTPUT}")

if __name__ == "__main__":
    main()