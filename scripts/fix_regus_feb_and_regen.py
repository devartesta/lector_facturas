"""
Fix REGUS 3313-43718 billed_company_name and regenerate PYG INC 2026.
"""
import sys
import os
import psycopg2

sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent / 'src'))

from lector_facturas.settings import load_settings
from lector_facturas.pyg_sync import sync_pyg_inc_to_drive

def main():
    settings = load_settings()
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    cur.execute("""
        UPDATE invoices.documents
        SET billed_company_name = 'ARTESTA INC'
        WHERE invoice_number = '3313-43718'
          AND supplier_code = 'REGUS'
          AND company_code = 'INC'
        RETURNING invoice_number, billed_company_name
    """)
    rows = cur.fetchall()
    print(f"Fixed {len(rows)} rows: {rows}")
    conn.commit()
    cur.close()
    conn.close()

    print("Regenerating PYG INC 2026...")
    sync_pyg_inc_to_drive(settings=settings, year=2026)
    print("Done!")

if __name__ == '__main__':
    main()
