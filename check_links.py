import pandas as pd
import requests

EXCEL_INPUT = "combined_master_with_urls.xlsx"
EXCEL_OUTPUT = "broken_links.xlsx"
COLUMNS_TO_SCAN = [
    'Masking Forms_URL',
    'Fraud/Alerts_URLs',
    'Public Records Request Form_URLs'
]

def check_link(url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        return response.status_code
    except Exception as e:
        return str(e)

def main():
    df = pd.read_excel(EXCEL_INPUT, engine='openpyxl')
    all_links = pd.concat([df[col].dropna() for col in COLUMNS_TO_SCAN])
    unique_links = all_links.unique()

    results = []
    for url in unique_links:
        status = check_link(url)
        if not str(status).startswith("2"):
            results.append({"URL": url, "Status": status})

    result_df = pd.DataFrame(results)
    result_df.to_excel(EXCEL_OUTPUT, index=False)
    print(f"✅ Broken links saved to {EXCEL_OUTPUT}")

if __name__ == "__main__":
    main()