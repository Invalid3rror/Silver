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
    """Loads and returns (Totals Row, Full Dataframe) with robust header detection."""
    if not os.path.exists(LOCAL_EXCEL):
        return None, None
    try:
        # Read raw without assuming headers; drop empty rows/cols
        raw = pd.read_excel(LOCAL_EXCEL, header=None)
        raw = raw.dropna(how="all")
        raw = raw.dropna(axis=1, how="all")

        # Try to locate the header row (usually contains 'DEPOSITORY' text)
        header_idx = None
        for idx, val in raw.iloc[:, 0].items():
            if isinstance(val, str) and "DEPOSITORY" in val.upper():
                header_idx = idx
                break

        # Fallback: use the first non-empty row as header
        if header_idx is None:
            header_idx = raw.index[0]

        header = raw.loc[header_idx].ffill()
        df = raw.iloc[header_idx + 1 :].copy()
        # Deduplicate header names to avoid Arrow errors
        deduped_cols = []
        seen = {}
        for col in header:
            col_str = str(col)
            if col_str in seen:
                seen[col_str] += 1
                deduped_cols.append(f"{col_str}_{seen[col_str]}")
            else:
                seen[col_str] = 0
                deduped_cols.append(col_str)
        df.columns = deduped_cols
        df = df.reset_index(drop=True)

        # Find TOTAL row in first column
        totals = df[df.iloc[:, 0].astype(str).str.contains("TOTAL", case=False, na=False)]

        if totals.empty:
            st.warning("No TOTAL row found in Excel file")
            return None, df

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

if totals is not None and not totals.empty:
    # 1. Historical Chart
    if os.path.exists(HISTORY_FILE):
        st.subheader("📉 Registered Inventory Over Time")
        hist_data = pd.read_csv(HISTORY_FILE)
        st.line_chart(hist_data.set_index("Date"))

    # 2. Key Metrics
    # Dynamically locate Registered and Eligible columns; fallback to first numeric values
    totals_row = totals.iloc[0]

    def pick_value(keywords, exclude_cols=None):
        exclude_cols = exclude_cols or set()
        # Try keyword match
        for col in totals_row.index:
            if col in exclude_cols:
                continue
            name = str(col).lower()
            if any(k in name for k in keywords):
                val = pd.to_numeric(totals_row[col], errors='coerce')
                if pd.notna(val):
                    return val, col
        # Fallback: first numeric column not excluded
        for col in totals_row.index:
            if col in exclude_cols:
                continue
            val = pd.to_numeric(totals_row[col], errors='coerce')
            if pd.notna(val):
                return val, col
        return None, None

    reg_numeric, reg_col = pick_value(["register", "reg"])
    elig_numeric, elig_col = pick_value(["eligible", "elig"], exclude_cols={reg_col} if reg_col else None)

    # Positional fallback: take first two numeric columns if keywords fail
    if pd.isna(reg_numeric) or pd.isna(elig_numeric):
        numeric_cols_in_order = []
        for col in totals_row.index:
            val = pd.to_numeric(totals_row[col], errors='coerce')
            if pd.notna(val):
                numeric_cols_in_order.append((col, val))
        if len(numeric_cols_in_order) >= 2:
            reg_col_fallback, reg_val_fallback = numeric_cols_in_order[0]
            elig_col_fallback, elig_val_fallback = numeric_cols_in_order[1]
            if pd.isna(reg_numeric):
                reg_numeric, reg_col = reg_val_fallback, reg_col_fallback
            if pd.isna(elig_numeric):
                elig_numeric, elig_col = elig_val_fallback, elig_col_fallback

    if pd.notna(reg_numeric) and pd.notna(elig_numeric):
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "📦 Registered (Available)",
                f"{reg_numeric:,.0f} oz",
                help="Silver available for delivery. When this drops, short sellers panic.",
            )
        with col2:
            st.metric(
                "🔒 Eligible (Vaulted)",
                f"{elig_numeric:,.0f} oz",
                help="Private silver. Not for sale unless price rises drastically.",
            )

        # 3. Squeeze Status Indicator
        st.subheader("🚨 Short Squeeze Status")
        
        # Critical threshold for short squeeze (adjust as needed)
        CRITICAL_THRESHOLD = 10_000_000  # 10 million oz = critical shortage
        SQUEEZE_THRESHOLD = 50_000_000    # 50 million oz = squeeze conditions
        
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
    else:
        st.error("⚠️ Unable to parse registered inventory value. Please check the data source.")

    # 3. Full Data Table
    st.subheader("🏢 Warehouse Breakdown")
    # Clean column names for display, ensure uniqueness, and reset index
    clean_cols = []
    seen = {}
    for col in full_data.columns:
        col_str = str(col).replace("\n", " ")
        if col_str in seen:
            seen[col_str] += 1
            clean_cols.append(f"{col_str}_{seen[col_str]}")
        else:
            seen[col_str] = 0
            clean_cols.append(col_str)
    full_data.columns = clean_cols
    full_data = full_data.reset_index(drop=True)
    
    # Apply styling only to numeric columns; fall back to raw table on any error
    try:
        numeric_cols = full_data.select_dtypes(include=['number']).columns
        styled = full_data.style
        if len(numeric_cols) > 0:
            styled = styled.highlight_max(axis=0, subset=numeric_cols, color="#e6f4ea")
        st.dataframe(styled)
    except Exception as e:
        st.warning(f"Table styling disabled due to: {e}")
        st.dataframe(full_data)

else:
    st.warning(
        "⚠️ No data found locally. Please click 'Sync Live CME Data' in the sidebar."
    )
