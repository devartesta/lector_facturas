"""
Script to download iPostal invoice PDFs using Playwright.
For each invoice: opens portal, clicks View Detail, extracts HTML, saves as PDF.
"""
import os
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

USERNAME = "artestastore"
PASSWORD = "Art3st@!!"
MAILBOX_URL = "https://portal.ipostal1.com/mailbox/3299330#/transactions"

# Output directory (temp, will upload to Drive)
OUTPUT_DIR = Path(r"C:\Users\AdriàSebastià\Downloads\ipostal_pdfs")
OUTPUT_DIR.mkdir(exist_ok=True)

# Invoices to download: (order_number, date_str_YYYYMMDD, date_filter_from, date_filter_to)
INVOICES = [
    # Nov 2025
    ("33486018", "20251115", "2025/11/01", "2025/11/30"),
    ("33644054", "20251122", "2025/11/01", "2025/11/30"),
    ("33781546", "20251129", "2025/11/01", "2025/11/30"),
    ("33795047", "20251129", "2025/11/01", "2025/11/30"),
    # Dec 2025
    ("34114236", "20251213", "2025/12/01", "2025/12/31"),
    ("34166613", "20251215", "2025/12/01", "2025/12/31"),
    # Mar 2026
    ("36541238", "20260325", "2026/03/23", "2026/03/30"),
    ("36584953", "20260328", "2026/03/23", "2026/03/30"),
    ("36600953", "20260328", "2026/03/23", "2026/03/30"),
]

CSS = """
<style>
  body { font-family: Arial, sans-serif; max-width: 700px; margin: 40px auto; padding: 20px; color: #333; }
  h2 { font-size: 22px; margin-bottom: 20px; }
  .label { color: #888; font-size: 12px; margin-bottom: 4px; }
  .value { font-size: 14px; margin-bottom: 16px; }
  .bold { font-weight: bold; }
  .two-col { display: flex; gap: 60px; margin-bottom: 20px; }
  table { width: 100%; border-collapse: collapse; margin: 20px 0; }
  th { text-align: left; border-bottom: 2px solid #ccc; padding: 8px 4px; font-size: 13px; }
  td { padding: 8px 4px; border-bottom: 1px solid #eee; font-size: 13px; }
  .total-row td { font-weight: bold; border-top: 2px solid #ccc; border-bottom: none; }
  .footer { margin-top: 20px; font-size: 13px; color: #555; }
  @media print { body { margin: 10px; } }
</style>
"""

def extract_invoice_html(page, invoice_number):
    """Extract the invoice modal content and return standalone HTML."""
    # Get the dialog content
    dialog_text = page.evaluate("""
        () => {
            const dialog = document.querySelector('.p-dialog');
            if (!dialog) return null;
            return {
                innerText: dialog.innerText,
                outerHTML: dialog.outerHTML
            };
        }
    """)
    if not dialog_text:
        return None

    # Build clean HTML from dialog inner HTML
    raw_html = dialog_text.get('outerHTML', '')

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
{CSS}
</head>
<body>
{raw_html}
</body>
</html>"""
    return html


def set_date_filter(page, date_from, date_to):
    """Set the date filter on the transactions page."""
    # Wait for date inputs
    page.wait_for_selector('input[placeholder="yyyy/mm/dd"]', timeout=10000)

    date_inputs = page.query_selector_all('input[placeholder="yyyy/mm/dd"]')
    if len(date_inputs) >= 2:
        # Clear and fill from date
        date_inputs[0].triple_click()
        date_inputs[0].fill(date_from)
        time.sleep(0.3)
        # Clear and fill to date
        date_inputs[1].triple_click()
        date_inputs[1].fill(date_to)
        time.sleep(0.3)
        # Press Enter to apply
        date_inputs[1].press("Enter")
        time.sleep(1.5)


def find_and_click_view_detail(page, invoice_number):
    """Find the View Detail button for a specific invoice and click it."""
    # Wait for table rows
    page.wait_for_selector('td', timeout=10000)
    time.sleep(0.5)

    # Find row with this invoice number
    result = page.evaluate(f"""
        () => {{
            const cells = Array.from(document.querySelectorAll('td'));
            const cell = cells.find(c => c.textContent.trim() === '{invoice_number}');
            if (!cell) return false;
            const row = cell.closest('tr');
            if (!row) return false;
            const btn = row.querySelector('button');
            if (!btn) return false;
            btn.click();
            return true;
        }}
    """)
    return result


def login(page):
    """Login to iPostal portal."""
    page.goto("https://portal.ipostal1.com/")

    # Wait for login form
    page.wait_for_selector('input[type="email"], input[name="email"], input[placeholder*="email" i], input[placeholder*="user" i]', timeout=15000)
    time.sleep(1)

    # Fill credentials
    # Try different selectors
    try:
        page.fill('input[type="email"]', USERNAME)
    except:
        try:
            page.fill('input[name="email"]', USERNAME)
        except:
            page.fill('input[type="text"]', USERNAME)

    page.fill('input[type="password"]', PASSWORD)
    page.keyboard.press("Enter")

    # Wait for navigation to dashboard/mailbox
    page.wait_for_url("**/mailbox/**", timeout=20000)
    time.sleep(2)


def download_invoice_pdf(page, invoice_number, date_str, date_from, date_to):
    """Download PDF for a single invoice."""
    output_path = OUTPUT_DIR / f"IPOSTAL_{date_str}_{invoice_number}.pdf"

    if output_path.exists():
        print(f"  Already exists: {output_path.name}")
        return str(output_path)

    print(f"  Setting date filter {date_from} -> {date_to}")

    # Navigate to transactions
    page.goto(MAILBOX_URL)
    page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(1)

    # Set date filter
    set_date_filter(page, date_from, date_to)

    print(f"  Looking for invoice {invoice_number}")
    found = find_and_click_view_detail(page, invoice_number)

    if not found:
        print(f"  WARNING: Invoice {invoice_number} not found in table!")
        return None

    # Wait for modal to open
    page.wait_for_selector('.p-dialog', timeout=10000)
    time.sleep(0.8)

    # Verify we have the right invoice
    modal_text = page.evaluate("() => document.querySelector('.p-dialog')?.innerText || ''")
    if invoice_number not in modal_text:
        print(f"  WARNING: Modal opened but invoice {invoice_number} not found in content!")
        print(f"  Modal text: {modal_text[:200]}")
        return None

    print(f"  Modal opened, extracting HTML...")

    # Extract HTML
    invoice_html = extract_invoice_html(page, invoice_number)
    if not invoice_html:
        print(f"  ERROR: Could not extract invoice HTML")
        return None

    # Create a new page with the invoice HTML and print to PDF
    print(f"  Rendering to PDF...")
    pdf_page = page.context.new_page()
    pdf_page.set_content(invoice_html, wait_until="networkidle")
    time.sleep(0.5)

    pdf_page.pdf(
        path=str(output_path),
        format="A4",
        margin={"top": "20mm", "bottom": "20mm", "left": "20mm", "right": "20mm"},
        print_background=True
    )
    pdf_page.close()

    print(f"  Saved: {output_path.name}")
    return str(output_path)


def main():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("Logging in to iPostal portal...")
        login(page)
        print("Logged in successfully")

        # Process each invoice
        current_date_range = None
        for invoice_number, date_str, date_from, date_to in INVOICES:
            print(f"\nProcessing invoice #{invoice_number} ({date_str})")
            try:
                path = download_invoice_pdf(page, invoice_number, date_str, date_from, date_to)
                results.append((invoice_number, date_str, path, None))
            except Exception as e:
                print(f"  ERROR: {e}")
                results.append((invoice_number, date_str, None, str(e)))

        browser.close()

    print("\n" + "="*60)
    print("RESULTS:")
    print("="*60)
    for invoice_number, date_str, path, error in results:
        if path:
            print(f"  OK: IPOSTAL_{date_str}_{invoice_number}.pdf")
        else:
            print(f"  FAIL: {invoice_number} - {error}")

    return results


if __name__ == "__main__":
    main()
