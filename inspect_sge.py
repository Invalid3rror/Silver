from bs4 import BeautifulSoup

with open("sge_dump.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

# Look for data tables
tables = soup.find_all("table")
print(f"Found {len(tables)} tables")

for i, table in enumerate(tables):
    print(f"\n--- Table {i} ---")
    rows = table.find_all("tr")
    print(f"Rows: {len(rows)}")
    # Print first few rows to see headers and data
    for row in rows[:5]:
        cols = row.find_all(['th', 'td'])
        text = [c.get_text(strip=True) for c in cols]
        print(text)
