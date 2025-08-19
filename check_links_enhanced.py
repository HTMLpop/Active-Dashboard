import pandas as pd
import requests
import json
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

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
    "User-Agent": "Mozilla/5.0"
}

RETRYABLE_ERRORS = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

def check_link_with_retry(url, retries=2):
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10, verify=False)
            return (url, response.status_code)
        except RETRYABLE_ERRORS as e:
            if attempt < retries:
                sleep(2)
                continue
            return (url, str(type(e).__name__))
        except Exception as e:
            return (url, str(e))

def run_checker():
    print("✅ Starting link check")
    df = pd.read_excel(EXCEL_INPUT, engine='openpyxl')
    all_links = pd.concat([df[col].dropna() for col in COLUMNS_TO_SCAN])
    unique_links = all_links.unique()

    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_link_with_retry, url): url for url in unique_links}
        for i, future in enumerate(as_completed(futures), 1):
            url, status = future.result()
            print(f"{i}/{len(unique_links)} - {url} --> {status}")
            if (isinstance(status, int) and not str(status).startswith("2")) or isinstance(status, str):
                results.append({"url": url, "status": status})

    pd.DataFrame(results).to_excel(EXCEL_OUTPUT, index=False)
    with open(JSON_OUTPUT, "w") as f:
        json.dump(results, f, indent=2)

    print("✅ Retry-enhanced link check complete.")
