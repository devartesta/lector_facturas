import sys, os, io, unicodedata, re
sys.path.insert(0, 'src')
from pypdf import PdfReader
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.settings import AppSettings

settings = AppSettings(
    google_client_id=os.environ['GOOGLE_CLIENT_ID'],
    google_client_secret=os.environ['GOOGLE_CLIENT_SECRET'],
    google_refresh_token=os.environ['GOOGLE_REFRESH_TOKEN'],
    drive_shared_drive_id=os.environ.get('GOOGLE_DRIVE_SHARED_DRIVE_ID', ''),
    drive_root_folder_id=os.environ['GOOGLE_DRIVE_ROOT_FOLDER_ID'],
)
drive = GoogleDriveClient(settings.to_drive_config())

def normalize(text):
    n = unicodedata.normalize('NFKD', text.replace('\xa0', ' ').replace('\r', ''))
    return ''.join(c for c in n if not unicodedata.combining(c))

for file_id, name in [
    ('1eKzk82Ivk4r8b3sMyV5hc_D1_BeqZg1l', 'Transaction_251912684.pdf'),
    ('16vEw73tmvsHTcaXf95JthDwsCP0MsSxB', '5538547928.pdf'),
]:
    print(f'\n=== {name} ===', flush=True)
    try:
        content = drive.download_file_bytes(file_id=file_id)
        print(f'Downloaded {len(content)} bytes', flush=True)
        reader = PdfReader(io.BytesIO(content))
        text = '\n'.join((p.extract_text() or '') for p in reader.pages[:1])
        norm = normalize(text)
        print(norm[:600], flush=True)
        m1 = re.search(r'Periodo de facturacion:([A-Za-z]{3}-[0-9]{2})', norm, re.IGNORECASE)
        m2 = re.search(r'Numero de factura:\s*([0-9]{10})', norm, re.IGNORECASE)
        print(f'Meta period: {m1.group(1) if m1 else "NO MATCH"}', flush=True)
        print(f'Google inv#: {m2.group(1) if m2 else "NO MATCH"}', flush=True)
    except Exception as e:
        import traceback
        print(f'ERROR: {e}', flush=True)
        traceback.print_exc()
