import sys, os, io, unicodedata, re
sys.path.insert(0, 'src')
from pypdf import PdfReader
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.settings import AppSettings
from lector_facturas.parsers.marketing_ads import parse_meta_ads_pdf
from pathlib import Path
import tempfile

settings = AppSettings(
    google_client_id=os.environ['GOOGLE_CLIENT_ID'],
    google_client_secret=os.environ['GOOGLE_CLIENT_SECRET'],
    google_refresh_token=os.environ['GOOGLE_REFRESH_TOKEN'],
    drive_shared_drive_id=os.environ.get('GOOGLE_DRIVE_SHARED_DRIVE_ID', ''),
    drive_root_folder_id=os.environ['GOOGLE_DRIVE_ROOT_FOLDER_ID'],
)
drive = GoogleDriveClient(settings.to_drive_config())

# Transaction_251912684.pdf -> drive_file_id 16vEw73tmvsHTcaXf95JthDwsCP0MsSxB (Meta content)
file_id = '16vEw73tmvsHTcaXf95JthDwsCP0MsSxB'
print(f'Downloading {file_id}...', flush=True)
content = drive.download_file_bytes(file_id=file_id)
print(f'Downloaded {len(content)} bytes', flush=True)

# Write to temp file and parse
with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as f:
    f.write(content)
    tmp_path = Path(f.name)

print(f'Temp file: {tmp_path}', flush=True)

try:
    # First show what text is extracted
    reader = PdfReader(str(tmp_path))
    text = '\n'.join((p.extract_text() or '') for p in reader.pages)
    print(f'Text length: {len(text)} chars', flush=True)
    
    def normalize(t):
        n = unicodedata.normalize('NFKD', t.replace('\xa0', ' ').replace('\r', ''))
        return ''.join(c for c in n if not unicodedata.combining(c))
    
    norm = normalize(text)
    idx = norm.find('Periodo')
    print(f'Periodo snippet: {repr(norm[idx:idx+60])}', flush=True)
    
    m = re.search(r'Periodo de facturacion:([A-Za-z]{3}-[0-9]{2})', norm, re.IGNORECASE)
    print(f'Pattern match: {m.group(1) if m else "NO MATCH"}', flush=True)
    
    # Now call actual parser
    print('Calling parse_meta_ads_pdf...', flush=True)
    rows = parse_meta_ads_pdf(tmp_path)
    print(f'Got {len(rows)} rows:', flush=True)
    for r in rows:
        print(f'  {r.division_invoice}: {r.net_amount} EUR', flush=True)
except Exception as e:
    import traceback
    print(f'ERROR: {e}', flush=True)
    traceback.print_exc()
finally:
    tmp_path.unlink(missing_ok=True)
