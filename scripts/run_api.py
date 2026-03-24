from __future__ import annotations

from pathlib import Path
import sys

import uvicorn


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


if __name__ == "__main__":
    uvicorn.run("lector_facturas.api.app:app", host="0.0.0.0", port=8000, reload=True)
