import requests
import datetime
from concurrent.futures import ThreadPoolExecutor

base_url = "https://www.shfe.com.cn/data/dailydata/"
dates = []
today = datetime.date(2026, 1, 27)

# Generate last 20 days
for i in range(20):
    d = today - datetime.timedelta(days=i)
    # Skip weekends if needed, but lets check all
    dates.append(d.strftime("%Y%m%d"))

patterns = [
    "{}dailystock.dat",
    "{}dailyStock.dat",
    "{}dailystock.js",
    "{}dailyStock.js",
    "{}dailyStock.json",
    "{}dailystock.json",
    "kx/kx{}.dat",
    "kx/kx{}.js",
    "outcome/kx/kx{}.dat",
    "outcome/kx/kx{}.js",
    "2026/{}dailystock.dat"
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.shfe.com.cn/"
}

def check_url(url):
    try:
        r = requests.head(url, headers=headers, timeout=2)
        if r.status_code == 200:
            print(f"FOUND: {url}")
            return url
        # else:
        #     print(f"Failed ({r.status_code}): {url}")
    except Exception as e:
        pass
    return None

def main():
    urls_to_check = []
    for d in dates:
        for p in patterns:
            filename = p.format(d)
            urls_to_check.append(base_url + filename)

    print(f"Checking {len(urls_to_check)} URLs...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(check_url, urls_to_check)

if __name__ == "__main__":
    main()
