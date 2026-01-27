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

    col1, col2 = st.columns(2)
    with col1:
        st.metric(
            "📦 Registered (Available)",
            f"{reg_val:,.0f} oz",
            help="Silver available for delivery. When this drops, short sellers panic.",
        )
    with col2:
        st.metric(
            "🔒 Eligible (Vaulted)",
            f"{elig_val:,.0f} oz",
            help="Private silver. Not for sale unless price rises drastically.",
        )

    # 3. Full Data Table
    st.subheader("🏢 Warehouse Breakdown")
    # Clean column names for display
    full_data.columns = [str(c).replace("\n", " ") for c in full_data.columns]
    st.dataframe(full_data.style.highlight_max(axis=0, color="#e6f4ea"))

else:
    st.warning(
        "⚠️ No data found locally. Please click 'Sync Live CME Data' in the sidebar."
    )
