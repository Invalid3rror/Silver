import requests
import json
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_shfe_silver_inventory(days_back=10):
    """
    Fetches the latest available Silver (Ag) inventory from SHFE.
    SHFE reports usually in 'dailystock.dat' (JSON format).
    """
    
    # SHFE Silver Product ID is usually 'ag'
    PRODUCT_ID_KEY = 'PRODUCTID'
    PRODUCT_ID_VAL = 'ag'
    
    # URL Pattern: SHFE website uses .dat extension but returns JSON
    URL_PATTERN = "https://www.shfe.com.cn/data/dailydata/{}dailystock.dat"
    
    start_date = datetime.now()
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.shfe.com.cn/",
        "Accept": "application/json, text/javascript, */*; q=0.01"
    }

    for i in range(days_back):
        date_check = start_date - timedelta(days=i)
        date_str = date_check.strftime("%Y%m%d")
        url = URL_PATTERN.format(date_str)
        logging.info(f"Checking {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=5)
            # logging.info(f"Status: {response.status_code}")
            
            if response.status_code == 200:

                try:
                    data = response.json()
                    if 'o_cursor' in data:
                        # Iterate through all items
                        for item in data['o_cursor']:
                            # Normalize keys to uppercase
                            item_upper = {k.upper(): v for k, v in item.items()}
                            p_id = item_upper.get(PRODUCT_ID_KEY, '').strip().lower()
                            
                            # Debug print for first few items of a day
                            # logging.info(f"Item: {p_id} - {item_upper.get('VARNAME')} - {item_upper.get('WHABBRNAME')}")

                            if p_id == 'ag' or p_id == 'silver':
                                wh_name = item_upper.get('WHABBRNAME', '').strip()
                                # logging.info(f"Silver Row: {wh_name}")
                                
                                # Look for "Total" row
                                if "总计" in wh_name or "Total" in wh_name:
                                    inventory_kg = float(item_upper.get('WRTWGHTS', 0))
                                    return {
                                        'date': date_str,
                                        'inventory_kg': inventory_kg,
                                        'inventory_tonnes': inventory_kg / 1000,
                                        'inventory_oz': inventory_kg * 32.1507
                                    }


                except json.JSONDecodeError:
                    continue 
            elif response.status_code == 404:
                continue
                
        except Exception:
            continue

    return None

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    result = fetch_shfe_silver_inventory(days_back=5)
    print(json.dumps(result, indent=2))
