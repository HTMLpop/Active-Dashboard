import asyncio
import aiohttp
import pandas as pd
from openpyxl import Workbook
from datetime import datetime

INPUT_FILE = "combined_master_with_urls.xlsx"
OUTPUT_FILE = "broken_links.xlsx"
SHEET_NAME = "Sheet1"  # Change if your sheet has a different name
URL_COLUMNS = ["Masking Forms_URL", "Fraud/Alerts_URLs", "Public Records Request Form_URLs"]

results = []

async def fetch_status(session, url):
    try:
        async with session.get(url, timeout=10, ssl=False) as response:
            return url, response.status
    except Exception as e:
        return url, str(e)

async def check_all_links(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_status(session, url) for url in urls if pd.notna(url)]
        return await asyncio.gather(*tasks)

def main():
    df = pd.read_excel(INPUT_FILE)
    all_urls = []

    for column in URL_COLUMNS:
        if column in df.columns:
            all_urls.extend(df[column].dropna().tolist())

    # Run async check
    loop = asyncio.get_event_loop()
    try:
        results = loop.run_until_complete(check_all_links(all_urls))
    except RuntimeError:
        results = asyncio.get_event_loop().run_until_complete(check_all_links(all_urls))

    # Filter and label
    output = []
    for url, status in results:
        if isinstance(status, int) and 200 <= status < 400:
            continue  # Working link
        output.append({"URL": url, "Status": status})

    # Save to Excel
    out_df = pd.DataFrame(output)
    if not out_df.empty:
        out_df.to_excel(OUTPUT_FILE, index=False)
        print(f"Saved broken link report to {OUTPUT_FILE}")
    else:
        print("✅ All links are valid!")

if __name__ == "__main__":
    main()
