"""
Periodificaciones YOURACCOUNTSTAXES — LTD (one-shot script)

Sube el PDF de cada factura anual a la carpeta Drive de cada mes del periodo,
con el sufijo PERIODIFICADA_X_N, e inserta un registro en invoices.documents
por fracción mensual.

Facturas procesadas:
  INV-0679  £318.00 net  Jan–Dec 2026  (12 meses)
  INV-0681  £660.00 net  Feb 2026–Jan 2027  (12 meses, cruza año)
  INV-0682  £1244.04 net  Jan–Dec 2026  (12 meses, retroactivo)
"""
import sys
import os
import uuid
import psycopg2
from decimal import Decimal, ROUND_HALF_UP
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from lector_facturas.settings import load_settings
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.pyg_sync import sync_pyg_ltd_to_drive

# ── Constants ────────────────────────────────────────────────────────────────

SUPPLIER_CODE       = 'YOURACCOUNTSTAXES'
COMPANY_CODE        = 'LTD'
BILLED_COMPANY_NAME = 'ARTESTA STORES (UK) LTD'
ISSUER_COMPANY_NAME = 'YOUR ACCOUNTS AND TAXES'
CURRENCY_CODE       = 'GBP'
ENTITY_FOLDER_NAME  = 'Artesta Stores (UK) Ltd'  # Drive entity folder
ADMIN_PATH          = ['expenses', 'opex', 'administration']

# ── Invoice definitions ───────────────────────────────────────────────────────

INVOICES = [
    {
        'pdf_path':            Path(r'C:\Users\AdriàSebastià\Downloads\Invoice INV-0679.pdf'),
        'invoice_number':      'INV-0679',
        'gross_total':         Decimal('381.60'),
        'net_total':           Decimal('318.00'),
        'vat_total':           Decimal('63.60'),
        'vat_percent':         Decimal('20.00'),
        'billing_period_start': date(2026, 1, 1),
        'billing_period_end':   date(2026, 12, 31),
    },
    {
        'pdf_path':            Path(r'C:\Users\AdriàSebastià\Downloads\Invoice INV-0681.pdf'),
        'invoice_number':      'INV-0681',
        'gross_total':         Decimal('792.00'),
        'net_total':           Decimal('660.00'),
        'vat_total':           Decimal('132.00'),
        'vat_percent':         Decimal('20.00'),
        'billing_period_start': date(2026, 2, 1),
        'billing_period_end':   date(2027, 1, 31),
    },
    {
        'pdf_path':            Path(r'C:\Users\AdriàSebastià\Downloads\Invoice INV-0682.pdf'),
        'invoice_number':      'INV-0682',
        'gross_total':         Decimal('1492.85'),
        'net_total':           Decimal('1244.04'),
        'vat_total':           Decimal('248.81'),
        'vat_percent':         Decimal('20.00'),
        'billing_period_start': date(2026, 1, 1),
        'billing_period_end':   date(2026, 12, 31),
    },
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def billing_months(start: date, end: date) -> list[date]:
    """Return list of first-of-month dates from start to end inclusive."""
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append(date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


def distribute(total: Decimal, n: int) -> list[Decimal]:
    """Split total into n equal parts (2dp), absorbing rounding in last month."""
    base = (total / n).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    amounts = [base] * n
    diff = total - base * n
    amounts[-1] += diff
    return amounts


def get_admin_folder(client: GoogleDriveClient, root_id: str, period_yyyymm: str, cache: dict) -> str:
    if period_yyyymm in cache:
        return cache[period_yyyymm]
    year = period_yyyymm[:4]
    fid = root_id
    for part in [ENTITY_FOLDER_NAME, year, period_yyyymm] + ADMIN_PATH:
        fid = str(client.ensure_folder(name=part, parent_id=fid)['id'])
    print(f'  Admin folder {period_yyyymm}: {fid}')
    cache[period_yyyymm] = fid
    return fid


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    settings = load_settings()
    client   = GoogleDriveClient(settings.to_drive_config())
    conn     = psycopg2.connect(os.environ['DATABASE_URL'])
    cur      = conn.cursor()

    root_id = settings.drive_root_folder_id
    folder_cache: dict[str, str] = {}
    total_inserted = 0
    total_uploaded = 0

    for inv in INVOICES:
        pdf_path: Path = inv['pdf_path']
        inv_num: str   = inv['invoice_number']
        bp_start: date = inv['billing_period_start']
        bp_end: date   = inv['billing_period_end']

        print(f'\n-- {inv_num} ({bp_start} -> {bp_end}) --')

        pdf_bytes = pdf_path.read_bytes()
        months    = billing_months(bp_start, bp_end)
        n         = len(months)

        net_amounts   = distribute(inv['net_total'],   n)
        gross_amounts = distribute(inv['gross_total'], n)
        vat_amounts   = distribute(inv['vat_total'],   n)

        print(f'  {n} meses · net/mes base={net_amounts[0]} · gross/mes base={gross_amounts[0]}')

        for idx, month_date in enumerate(months):
            x          = idx + 1           # 1-based fraction
            period     = month_date.strftime('%Y%m')
            date_str   = month_date.strftime('%Y%m%d')
            fname      = f'{SUPPLIER_CODE}_{date_str}_{inv_num}_PERIODIFICADA_{x}_{n}.pdf'
            inv_num_db = f'{inv_num}_PERIODIFICADA_{x}_{n}'

            # Skip if already in DB
            cur.execute(
                "SELECT id FROM invoices.documents WHERE invoice_number = %s AND company_code = %s",
                (inv_num_db, COMPANY_CODE),
            )
            if cur.fetchone():
                print(f'  [{x}/{n}] {period} already in DB, skip')
                continue

            # Upload PDF to Drive
            admin_id = get_admin_folder(client, root_id, period, folder_cache)

            existing = client.list_files(parent_id=admin_id, name=fname)
            if existing:
                file_id  = str(existing[0]['id'])
                drive_url = str(existing[0].get('webViewLink', f'https://drive.google.com/file/d/{file_id}/view'))
                print(f'  [{x}/{n}] {period} file already in Drive: {fname}')
            else:
                result    = client.upload_file(name=fname, parent_id=admin_id, content=pdf_bytes)
                file_id   = str(result['id'])
                drive_url = str(result.get('webViewLink', f'https://drive.google.com/file/d/{file_id}/view'))
                print(f'  [{x}/{n}] {period} uploaded: {fname} -> {file_id}')
                total_uploaded += 1

            # Insert DB record
            cur.execute("""
                INSERT INTO invoices.documents (
                    id, invoice_number, invoice_date, period_yyyymm,
                    gross_amount, net_amount, vat_amount, vat_percent,
                    currency_code, supplier_code, company_code,
                    billed_company_name, issuer_company_name,
                    billing_period_start, billing_period_end,
                    status, parser_name, source_channel,
                    original_filename, drive_file_id, drive_url,
                    document_type
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    'classified', 'manual_periodificada', 'manual',
                    %s, %s, %s,
                    'invoice'
                )
            """, (
                str(uuid.uuid4()), inv_num_db, month_date, period,
                gross_amounts[idx], net_amounts[idx], vat_amounts[idx], inv['vat_percent'],
                CURRENCY_CODE, SUPPLIER_CODE, COMPANY_CODE,
                BILLED_COMPANY_NAME, ISSUER_COMPANY_NAME,
                bp_start, bp_end,
                fname, file_id, drive_url,
            ))
            total_inserted += 1

        conn.commit()
        print(f'  Committed {inv_num}')

    cur.close()
    conn.close()

    print(f'\nTotal: {total_uploaded} ficheros subidos, {total_inserted} registros insertados')

    # Regenerate PYG LTD 2026 and 2027 (INV-0681 has a Jan 2027 slice)
    print('\nRegenerando PYG LTD 2026...')
    sync_pyg_ltd_to_drive(settings=settings, year=2026)
    print('Regenerando PYG LTD 2027...')
    sync_pyg_ltd_to_drive(settings=settings, year=2027)
    print('PYG regenerados')


if __name__ == '__main__':
    main()
