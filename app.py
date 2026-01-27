import os
import time
from datetime import datetime

import pandas as pd
import requests
import streamlit as st

# --- CONFIG ---
CME_URL = "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls"
LOCAL_EXCEL = "silver_stocks_data.xls"
HISTORY_FILE = "inventory_history.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
}


def download_and_save():
    """Downloads report and updates local CSV history."""
    # Try up to 3 times if the server is slow
    for attempt in range(3):
        try:
            # Increased timeout to 30 seconds
            resp = requests.get(CME_URL, headers=HEADERS, timeout=30)
            resp.raise_for_status()

            with open(LOCAL_EXCEL, "wb") as f:
                f.write(resp.content)

            # Logic to record history for the chart
            totals, _ = load_data()
            if totals is not None:
                # CME Column 1 is usually Registered.
                # We convert to numeric just in case of formatting.
                reg_val = pd.to_numeric(totals.iloc[0, 1], errors="coerce")
                new_entry = pd.DataFrame(
                    [[datetime.now().strftime("%Y-%m-%d"), reg_val]],
                    columns=["Date", "Registered"],
                )

                if os.path.exists(HISTORY_FILE):
                    hist_df = pd.read_csv(HISTORY_FILE)
                    # Add new, remove duplicates for the same day, save back
                    hist_df = pd.concat([hist_df, new_entry]).drop_duplicates(
                        subset=["Date"], keep="last"
                    )
                    hist_df.to_csv(HISTORY_FILE, index=False)
                else:
                    new_entry.to_csv(HISTORY_FILE, index=False)
                return True, "Data updated successfully!"
            else:
                return False, "Could not parse totals from Excel file"
        except Exception as e:
            if attempt < 2:
                time.sleep(2)  # Wait 2 seconds before retrying
                continue
            return False, f"Download failed after 3 attempts: {e}"


def load_data():
    """Loads and returns (Totals Row, Full Dataframe)."""
    if not os.path.exists(LOCAL_EXCEL):
        return None, None
    try:
        # We skip the first 4 rows of logos/headers in CME files
        df = pd.read_excel(LOCAL_EXCEL, header=4).dropna(how="all")
        # Find the row that says 'TOTAL' in the first column
        totals = df[df.iloc[:, 0].astype(str).str.contains("TOTAL", case=False, na=False)]
        return totals, df
    except Exception as e:
        st.error(f"Excel Parse Error: {e}")
        return None, None


# --- UI ---
st.set_page_config(page_title="Silver Squeeze Tracker", page_icon="🥈")
st.title("🥈 Silver Inventory Squeeze Tracker")

# Sidebar for updates
with st.sidebar:
    st.header("Settings")
    if st.button("🔄 Sync Live CME Data"):
        success, msg = download_and_save()
        if success:
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)

    st.info("Note: CME updates these reports once daily in the afternoon (EST).")

totals, full_data = load_data()

if totals is not None:
    # 1. Historical Chart
    if os.path.exists(HISTORY_FILE):
        st.subheader("📉 Registered Inventory Over Time")
        hist_data = pd.read_csv(HISTORY_FILE)
        st.line_chart(hist_data.set_index("Date"))

    # 2. Key Metrics
    reg_val = totals.iloc[0, 1]
    elig_val = totals.iloc[0, 2]
    
    # Convert to numeric values
    reg_numeric = pd.to_numeric(reg_val, errors='coerce')
    elig_numeric = pd.to_numeric(elig_val, errors='coerce')

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "📦 Registered (Available)",
            f"{reg_numeric:,.0f} oz" if pd.notna(reg_numeric) else "N/A",
            help="Silver available for delivery. When this drops, short sellers panic.",
        )
    with col2:
        st.metric(
            "🔒 Eligible (Vaulted)",
            f"{elig_numeric:,.0f} oz" if pd.notna(elig_numeric) else "N/A",
            help="Private silver. Not for sale unless price rises drastically.",
        )

    # 3. Squeeze Status Indicator
    st.subheader("🚨 Short Squeeze Status")
    
    # Critical threshold for short squeeze (adjust as needed)
    CRITICAL_THRESHOLD = 10_000_000  # 10 million oz = critical shortage
    SQUEEZE_THRESHOLD = 50_000_000    # 50 million oz = squeeze conditions
    
    if pd.notna(reg_numeric):
        if reg_numeric < CRITICAL_THRESHOLD:
            status = "🔴 CRITICAL - Severe Short Squeeze Likely"
            color = "red"
            description = "Registered inventory is critically low. Short squeeze conditions are imminent or underway."
            price_impact = "📈 Silver Price: WILL SURGE - Shorts forced to cover at any price. Massive volatility expected."
        elif reg_numeric < SQUEEZE_THRESHOLD:
            status = "🟠 HIGH ALERT - Squeeze Conditions Building"
            color = "orange"
            description = "Registered inventory is dangerously low. Squeeze conditions are likely to develop."
            price_impact = "📈 Silver Price: UPWARD PRESSURE - Supply crisis imminent. Expect rapid price increases."
        else:
            status = "🟢 SAFE - Supply Stable"
            color = "green"
            description = "Registered inventory is healthy. Normal market conditions."
            price_impact = "📉 Silver Price: DOWNWARD PRESSURE - Abundant supply prevents short squeeze. Price may decline or stagnate."
        
        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown(f"### {status}")
            st.info(description)
            st.warning(price_impact)
        with col2:
            st.metric(
                "Threshold Status",
                f"{reg_numeric:,.0f} oz",
                f"vs {SQUEEZE_THRESHOLD:,.0f} oz"
            )
    else:
        st.error("⚠️ Unable to parse registered inventory value. Please check the data source.")
    
    # Quality Lookup Reference
    st.subheader("📚 Silver Quality Reference")
    with st.expander("What stops the short squeeze?"):
        st.markdown("""
        **Low Silver Quality Conditions** (Triggers Short Squeeze):
        - **Registered Inventory < 10M oz**: Critical shortage - shorts must cover at any price
        - **Registered Inventory < 50M oz**: Squeeze conditions - delivery failures likely
        - **Negative Spreads**: Futures trading below spot price (extreme scarcity)
        
        **What STOPS the Squeeze?**
        1. **New Mine Supply**: Fresh silver entering warehouses increases registered inventory
        2. **Price Spike**: Higher prices incentivize eligible (vaulted) silver to be registered
        3. **Reduced Demand**: Less delivery requests lowers pressure on inventory
        4. **Market Correction**: Futures prices normalize to realistic levels
        5. **Inventory Threshold > 50M oz**: Supply pressure eases significantly
        
        **Current Status Summary:**
        - 🟢 Safe: Registered > 50M oz
        - 🟡 Caution: Registered 10M-50M oz
        - 🔴 Critical: Registered < 10M oz
        """)

    # 3. Full Data Table
    st.subheader("🏢 Warehouse Breakdown")
    # Clean column names for display
    full_data.columns = [str(c).replace("\n", " ") for c in full_data.columns]
    
    # Apply styling only to numeric columns to avoid errors
    def highlight_numeric_max(s):
        """Highlight max value only for numeric columns."""
        if s.dtype in ['float64', 'int64', 'float32', 'int32']:
            is_max = s == s.max()
            return ['background-color: #e6f4ea' if v else '' for v in is_max]
        return [''] * len(s)
    
    st.dataframe(full_data.style.apply(highlight_numeric_max, axis=0))

else:
    st.warning(
        "⚠️ No data found locally. Please click 'Sync Live CME Data' in the sidebar."
    )
