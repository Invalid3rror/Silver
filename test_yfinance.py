import yfinance as yf

try:
    print("Fetching Ticker History...")
    ticker = yf.Ticker("SI=F")
    hist = ticker.history(period="5d")
    print(hist.columns)
    print(hist.tail())
    
except Exception as e:
    print(f"Error: {e}")


