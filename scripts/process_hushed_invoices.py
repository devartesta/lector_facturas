"""
Process all HUSHED phone invoices from Gmail.
- Creates HUSHED supplier in invoices.suppliers
- Downloads Invoice PDFs from each email
- Uploads to Google Drive under correct period admin folder
- Inserts records into invoices.documents
"""
import sys
import json
import re
import io
import uuid
import psycopg2
import os
from datetime import datetime
from urllib.request import Request, urlopen
sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent / 'src'))

from lector_facturas.settings import load_settings
from lector_facturas.review_notifications import refresh_access_token
from lector_facturas.gmail_sync import download_attachment_bytes
from lector_facturas.google_drive import GoogleDriveClient
from pypdf import PdfReader


MONTHS = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4,
    'May': 5, 'June': 6, 'July': 7, 'August': 8,
    'September': 9, 'October': 10, 'November': 11, 'December': 12,
}
INC_ARTESTA_FOLDER_ID = '10V8getNsSqrjEiz1sxMFLkM-IVmn6MRp'


def gmail_get(url, tok):
    req = Request(url, headers={'Authorization': f'Bearer {tok}'}, method='GET')
    with urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def find_attachments(part):
    results = []
    filename = part.get('filename', '')
    body = part.get('body', {})
    att_id = body.get('attachmentId', '')
    if filename and att_id:
        results.append({'filename': filename, 'att_id': att_id})
    for child in part.get('parts', []):
        results.extend(find_attachments(child))
    return results


def parse_hushed_pdf(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    text = ''
    for page in reader.pages:
        text += page.extract_text() or ''
    date_m = re.search(r'Date of issue\s+([A-Za-z]+ \d+,\s+\d{4})', text)
    amt_m = re.search(r'\$([0-9.]+)\s+USD due', text)
    if not amt_m:
        amt_m = re.search(r'Amount due\s+\$([0-9.]+)', text)
    if not amt_m:
        amt_m = re.search(r'Total\s+\$([0-9.]+)', text)
    date_str = date_m.group(1).strip() if date_m else ''
    amount = amt_m.group(1) if amt_m else ''
    inv_date = None
    if date_str:
        parts = date_str.replace(',', '').split()
        if len(parts) == 3:
            month_num = MONTHS.get(parts[0], 0)
            if month_num:
                inv_date = datetime(int(parts[2]), month_num, int(parts[1])).date()
    return date_str, amount, inv_date


def get_admin_folder(client, period_yyyymm, cache):
    if period_yyyymm in cache:
        return cache[period_yyyymm]
    year = period_yyyymm[:4]
    year_folder = client.ensure_folder(parent_id=INC_ARTESTA_FOLDER_ID, name=year)
    period_folder = client.ensure_folder(parent_id=year_folder['id'], name=period_yyyymm)
    exp_folder = client.ensure_folder(parent_id=period_folder['id'], name='expenses')
    opex_folder = client.ensure_folder(parent_id=exp_folder['id'], name='opex')
    admin_folder = client.ensure_folder(parent_id=opex_folder['id'], name='administration')
    fid = admin_folder['id']
    cache[period_yyyymm] = fid
    print(f'  Admin folder for {period_yyyymm}: {fid}')
    return fid


def main():
    settings = load_settings()
    cfg = settings.to_gmail_config()
    client = GoogleDriveClient(settings.to_drive_config())
    token = refresh_access_token(cfg)
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    # Step 1: Create HUSHED supplier if not exists
    cur.execute("SELECT supplier_code FROM invoices.suppliers WHERE supplier_code='HUSHED' AND company_code='INC'")
    if not cur.fetchone():
        print('Creating HUSHED supplier...')
        cur.execute("""INSERT INTO invoices.suppliers
            (id, supplier_code, supplier_name, company_code, destination_path, current_folder, is_active,
             billing_company_name, sender_emails)
            VALUES (%s, 'HUSHED', 'HUSHED', 'INC', 'expenses/opex/administration', 'hushed', true,
                    'Hushed c/o AffinityClick Inc.', '["invoice+statements@hushed.com"]')""",
            (str(uuid.uuid4()),))
        conn.commit()
        print('  HUSHED supplier created')
    else:
        print('HUSHED supplier already exists')

    # Step 2: Fetch all HUSHED message refs
    refs = []
    page_token = ''
    while True:
        params = 'q=hushed&maxResults=500'
        if page_token:
            params += f'&pageToken={page_token}'
        data = gmail_get(f'https://gmail.googleapis.com/gmail/v1/users/me/messages?{params}', token)
        refs.extend(data.get('messages', []))
        page_token = data.get('nextPageToken', '')
        if not page_token:
            break

    print(f'\nFetching {len(refs)} messages...')

    # Step 3: Get existing HUSHED records
    cur.execute("SELECT invoice_number FROM invoices.documents WHERE supplier_code='HUSHED' AND company_code='INC'")
    existing_hushed = {row[0] for row in cur.fetchall()}
    print(f'Existing HUSHED records in DB: {len(existing_hushed)}')

    drive_admin_cache = {}
    processed = 0
    skipped = 0
    errors = 0

    for i, ref in enumerate(refs):
        try:
            msg = gmail_get(
                f'https://gmail.googleapis.com/gmail/v1/users/me/messages/{ref["id"]}?format=full',
                token,
            )
            headers = {h['name'].lower(): h['value'] for h in msg['payload']['headers']}
            subject = headers.get('subject', '')

            m = re.search(r'#(\d{4}-\d{4})', subject)
            if not m:
                print(f'  [{i+1}] No receipt number in subject: {subject}')
                skipped += 1
                continue
            receipt_num = m.group(1)

            if receipt_num in existing_hushed:
                skipped += 1
                continue

            atts = find_attachments(msg['payload'])
            inv_att = next((a for a in atts if a['filename'].startswith('Invoice-')), None)
            if not inv_att:
                print(f'  [{i+1}] {receipt_num}: no Invoice PDF, skip')
                skipped += 1
                continue

            pdf_bytes = download_attachment_bytes(cfg, message_id=ref['id'], attachment_id=inv_att['att_id'])
            date_str, amount, inv_date = parse_hushed_pdf(pdf_bytes)

            if not inv_date or not amount:
                print(f'  [{i+1}] {receipt_num}: parse fail (date={date_str} amt={amount}), skip')
                skipped += 1
                continue

            period = inv_date.strftime('%Y%m')
            fname = f'HUSHED_{inv_date.strftime("%Y%m%d")}_{receipt_num}.pdf'

            admin_id = get_admin_folder(client, period, drive_admin_cache)

            existing_drive = client.list_files(parent_id=admin_id, name=fname)
            if existing_drive:
                f = existing_drive[0]
            else:
                f = client.upload_file(name=fname, parent_id=admin_id, content=pdf_bytes)

            drive_file_id = f['id']
            drive_url = f'https://drive.google.com/file/d/{drive_file_id}/view?usp=drivesdk'

            new_id = str(uuid.uuid4())
            issuer = 'HUSHED C/O AFFINITYCLICK INC.'
            extracted = json.dumps({
                'issuer_company_name': issuer,
                'billed_company_name': 'ARTESTA INC',
                'period_yyyymm': period,
                'currency_code': 'USD',
                'gross_amount': amount,
                'vat_percent': '0',
                'vat_amount': '0',
                'net_amount': amount,
                'original_subject': subject,
            })
            cur.execute(
                """INSERT INTO invoices.documents
                    (id, invoice_number, invoice_date, period_yyyymm,
                     gross_amount, net_amount, vat_amount, vat_percent,
                     currency_code, drive_file_id, drive_url, status,
                     supplier_code, supplier_name, company_code,
                     issuer_company_name, billed_company_name,
                     billing_period_start, billing_period_end,
                     parser_name, parser_confidence, original_filename,
                     extracted_raw, source_channel,
                     email_message_id, source_subject, sender_email)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            %s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)""",
                (
                    new_id, receipt_num, inv_date.isoformat(), period,
                    amount, amount, '0', '0',
                    'USD', drive_file_id, drive_url, 'classified',
                    'HUSHED', 'HUSHED', 'INC',
                    issuer, 'ARTESTA INC',
                    inv_date.isoformat(), None,
                    'hushed', '0.9800',
                    inv_att['filename'], extracted, 'gmail',
                    ref['id'], subject, 'invoice+statements@hushed.com',
                ),
            )
            conn.commit()
            existing_hushed.add(receipt_num)

            print(f'  [{i+1}] {receipt_num}: {inv_date} ${amount} -> {fname} OK')
            processed += 1

        except Exception as e:
            print(f'  [{i+1}] ERROR: {e}')
            conn.rollback()
            errors += 1

    cur.close()
    conn.close()
    print(f'\nDone! Processed={processed}, Skipped={skipped}, Errors={errors}')


if __name__ == '__main__':
    main()
