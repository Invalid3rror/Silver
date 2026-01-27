import requests
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_shfe_silver_inventory(days_back=10):
    """
    Fetches the latest available Silver (Ag) inventory from SHFE.
    SHFE reports usually in 'dailystock.dat' (JSON format).
    """
    
    # SHFE Silver Product ID is usually 'ag'
    PRODUCT_ID = 'ag'
    
    # URL Pattern
    # Note: SHFE website uses .dat extension for JSON data often
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
        
        logging.info(f"Checking SHFE data for {date_str} at {url}...")
        
        try:
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # SHFE data usually has a key 'o_cursor' containing the list of data
                    if 'o_cursor' in data:
                        inventory_list = data['o_cursor']
                        for item in inventory_list:
                            # Check for Silver
                            # VARNAME: '白银', PRODUCTID: 'ag_wh' (warehouse) or just 'ag'
                            # Keys might be 'VARNAME', 'REGWARHOUSE', 'WHROWS', 'WRTWGHTS' (Warrant Weights)
                            
                            # Note: Key names can be uppercase or lowercase depending on the endpoint version
                            # We normalize keys to uppercase for checking
                            item_upper = {k.upper(): v for k, v in item.items()}
                            
                            prod_id = item_upper.get('PRODUCTID', '').strip()
                            var_name = item_upper.get('VARNAME', '').strip()
                            
                            if 'ag' in prod_id.lower() or '白银' in var_name:
                                logging.info(f"Found Silver data for {date_str}")
                                
                                # Extract relevant fields
                                # WRTWGHTS: Warranted Weights (Registered?)
                                # WRTS: Warrants
                                # Usually we look for Total Inventory which might be listed
                                
                                # Let's dump the item to see what we have
                                print(json.dumps(item, indent=2, ensure_ascii=False))
                                
                                on_warrant = item_upper.get('WRTWGHTS', 0)
                                return {
                                    'date': date_str,
                                    'inventory_kg': on_warrant,
                                    'raw_data': item
                                }
                        
                        logging.warning(f"Data found for {date_str} but no Silver (ag) entry.")
                    else:
                        logging.warning(f"JSON structure unexpected: {data.keys()}")
                except json.JSONDecodeError:
                    logging.warning("Response was not valid JSON.")
            else:
                logging.debug(f"HTTP {response.status_code} - Data likely not published for this date.")
        
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")

    logging.error("Could not find any SHFE Silver inventory data in the last %d days.", days_back)
    return None

if __name__ == "__main__":
    result = fetch_shfe_silver_inventory()
    if result:
        print("\n--- SHFE Silver Inventory ---")
        print(f"Date: {result['date']}")
        print(f"Inventory: {result['inventory_kg']} kg")
        print(f"Tonnes: {float(result['inventory_kg']) / 1000} t")
    else:
        print("Failed to retrieve data.")
