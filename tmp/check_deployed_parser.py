import sys
sys.path.insert(0, 'src')
from lector_facturas.parsers.marketing_ads import parse_meta_ads_text
import inspect
src = inspect.getsource(parse_meta_ads_text)
print(src[:500])
