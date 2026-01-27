from bs4 import BeautifulSoup

try:
    with open('yahoo.html', 'r', encoding='utf-8') as f:
        html = f.read()

    soup = BeautifulSoup(html, 'html.parser')
    
    # Try multiple ways to find the label
    label = soup.find(string="Open Interest")
    if label:
        print("Found label by string match")
        # Go up to the row
        parent = label.parent
        while parent and parent.name != 'tr':
            parent = parent.parent
        if parent:
            print("Row text:", parent.get_text(strip=True))
    else:
        print("Label not found by exact string.")

    # Dump all text to see if we are crazy
    all_text = soup.get_text(separator=' ', strip=True)
    if "Open Interest" in all_text:
        print("Text exists in document.")
        idx = all_text.find("Open Interest")
        print("Context:", all_text[idx:idx+100])
    
    # Check for specific classes often used by Yahoo
    # They often use <td class="Ta(end) Fw(600) Lh(14px)" data-test="OPEN_INTEREST-value">
    target = soup.find('td', {'data-test': 'OPEN_INTEREST-value'})
    if target:
        print("Found via data-test:", target.get_text())

except Exception as e:
    print(e)
