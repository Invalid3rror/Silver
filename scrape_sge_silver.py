import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

def fetch_sge_silver_benchmark(start_date, end_date):
    url = "https://www.sge.com.cn/sjzx/everyShyjzj"
    
    payload = {
        "start": start_date, # Format: YYYY-MM-DD
        "end": end_date      # Format: YYYY-MM-DD
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    print(f"Fetching data from {url} for period {start_date} to {end_date}...")
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        rows = soup.find_all('tr')
        
        data = []
        
        # Skip the first row if it's the header (based on inspection of response)
        # The response is a fragment, usually starts with headers if included, or just rows.
        # In our curl dump:
        # <tr class="color333 title">...</tr>
        # <tr class=" border_ea noTop_border">...</tr>
        
        for row in rows:
            if 'title' in row.get('class', []):
                continue
                
            cols = row.find_all('td')
            if len(cols) >= 4:
                date_str = cols[0].get_text(strip=True)
                contract = cols[1].get_text(strip=True)
                morning_price = cols[2].get_text(strip=True)
                afternoon_price = cols[3].get_text(strip=True)
                
                # Check if it's Silver (SHAG)
                if "SHAG" in contract:
                    entry = {
                        "date": date_str,
                        "contract": contract,
                        "morning_price": morning_price,
                        "afternoon_price": afternoon_price
                    }
                    data.append(entry)
                    print(f"Found: {entry}")
                    
        return data

    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

if __name__ == "__main__":
    # Example usage: Fetch last 30 days
    end = datetime.now()
    start = end - timedelta(days=30)
    
    s_date = start.strftime("%Y-%m-%d")
    e_date = end.strftime("%Y-%m-%d")
    
    fetch_sge_silver_benchmark(s_date, e_date)
