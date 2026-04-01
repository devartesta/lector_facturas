"""
Fix QuickBooks net_amount (INC, US tax is an expense -> net = gross)
and regenerate PYG INC workbook for 2026.
"""
import sys
import os
import psycopg2

sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent / 'src'))

from lector_facturas.settings import load_settings
from lector_facturas.pyg_sync import sync_pyg_inc_to_drive

def main():
    settings = load_settings()  # loads .env.local first
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    # Fix QuickBooks net_amount = gross_amount for INC (US sales tax is an expense)
    cur.execute("""
        UPDATE invoices.documents
        SET net_amount = gross_amount
        WHERE supplier_code = 'QUICKBOOKS'
          AND company_code = 'INC'
          AND net_amount != gross_amount
        RETURNING invoice_number, gross_amount, net_amount
    """)
    rows = cur.fetchall()
    if rows:
        print(f"Fixed {len(rows)} QUICKBOOKS records:")
        for r in rows:
            print(f"  {r[0]}: net_amount -> {r[2]}")
    else:
        print("QUICKBOOKS net_amount already correct (or no records found)")
    conn.commit()
    cur.close()
    conn.close()

    # Regenerate PYG INC 2026
    print("\nRegenerating PYG INC 2026...")
    sync_pyg_inc_to_drive(settings=settings, year=2026)
    print("Done!")

if __name__ == '__main__':
    main()
