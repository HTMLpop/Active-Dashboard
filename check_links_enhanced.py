import pandas as pd
import requests
import time
import json
from openpyxl import load_workbook
from requests.exceptions import SSLError, ConnectionError

# Load Excel file
excel_file = 'combined_master_with_urls.xlsx'
df = pd.read_excel(excel_file)

# Clean up column name
df.columns = df.columns.str.strip()
url_col = next((col for col in df.columns if 'url' in col.lower()), None)

if not url_col:
    raise Exception("No URL column found in the spreadsheet.")

# Track broken links
broken_links = []
link_status_map = {}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

print(f"Checking {len(df)} URLs...\n")

for index, row in df.iterrows():
    url = row[url_col]
    if pd.isna(url) or not isinstance(url, str) or not url.startswith('http'):
        continue

    try:
        response = requests.get(url, headers=headers, timeout=10, verify=True)
        status = response.status_code

        # Allow some codes that still return content
        if status in [200, 301, 302]:
            link_status_map[url] = "working"
        else:
            link_status_map[url] = "broken"
            broken_links.append({
                'Row': index + 2,
                'URL': url,
                'Status': status,
                'Reason': f"Unexpected HTTP status: {status}"
            })

    except SSLError as ssl_err:
        link_status_map[url] = "working"  # Assume working due to cert issue
    except ConnectionError as conn_err:
        link_status_map[url] = "broken"
        broken_links.append({
            'Row': index + 2,
            'URL': url,
            'Status': 'Error',
            'Reason': 'Connection failed'
        })
    except Exception as e:
        link_status_map[url] = "broken"
        broken_links.append({
            'Row': index + 2,
            'URL': url,
            'Status': 'Error',
            'Reason': str(e)
        })

    # Throttle to avoid site bans
    time.sleep(0.25)

# Save broken links to Excel
if broken_links:
    df_broken = pd.DataFrame(broken_links)
    df_broken.to_excel("broken_links.xlsx", index=False)
    print(f"\n🔴 {len(broken_links)} broken links saved to broken_links.xlsx")
else:
    print("\n🟢 No broken links found.")

# Save full status map to JSON
with open("broken_links.json", "w") as json_file:
    json.dump(link_status_map, json_file, indent=2)

print("📝 Link status JSON saved to broken_links.json")
