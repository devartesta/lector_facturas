from __future__ import annotations

import argparse
import html
import json
import os
import sys
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.review_workflow import (
    ReviewDecision,
    apply_review_decision,
    get_provider,
    list_companies,
    list_providers_for_company,
    normalize_company,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start a local review page for a pending invoice.")
    parser.add_argument("--root", required=True, help="Finance root folder")
    parser.add_argument("--file", required=True, help="Pending review file to correct")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open-browser", action="store_true")
    return parser.parse_args()


class ReviewHandler(BaseHTTPRequestHandler):
    root_path: Path
    source_file: Path

    def do_GET(self) -> None:
        if self.path.startswith("/open-file"):
            self._open_pending_file()
            return
        self._send_html(self._render_form())

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length).decode("utf-8")
        data = urllib.parse.parse_qs(body)
        try:
            company = normalize_company(_required(data, "company"))
            supplier_code = _required(data, "supplier_code")
            invoice_date = _required(data, "invoice_date")
            invoice_number = _required(data, "invoice_number")
            self._last_company = company
            self._last_supplier_code = supplier_code
            result = apply_review_decision(
                ReviewDecision(
                    root=self.root_path,
                    source_file=self.source_file,
                    company=company,
                    supplier_code=supplier_code,
                    invoice_date=invoice_date,
                    invoice_number=invoice_number,
                )
            )
            self._send_html(self._render_success(result.destination_file))
        except Exception as exc:  # noqa: BLE001
            self._send_html(self._render_form(error=str(exc)), status=400)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _render_form(self, error: str = "") -> str:
        companies = list_companies()
        default_company = normalize_company(self._guess_company())
        company_options = "".join(
            _option(company, company, selected=company == default_company) for company in companies
        )
        provider_map = {
            company: [
                {
                    "supplier_code": record.supplier_code,
                    "provider_name": record.provider_name,
                    "destination_path": record.destination_path,
                }
                for record in list_providers_for_company(company)
            ]
            for company in companies
        }
        first_provider_code = provider_map.get(default_company, [{}])[0].get("supplier_code", "")
        invoice_date = _guess_invoice_date(self.source_file)
        invoice_number = _guess_invoice_number(self.source_file)
        error_html = (
            f"<div style='margin-bottom:14px;padding:10px 12px;border-radius:10px;background:#fef2f2;border:1px solid #fecaca;color:#991b1b'>{html.escape(error)}</div>"
            if error
            else ""
        )
        return f"""\
<html>
  <head>
    <meta charset="utf-8">
    <title>Revisar factura</title>
    <style>
      body {{ font-family: Segoe UI, Arial, sans-serif; background:#f3f4f6; margin:0; padding:24px; color:#111827; }}
      .card {{ max-width:760px; margin:0 auto; background:#fff; border:1px solid #e5e7eb; border-radius:16px; overflow:hidden; }}
      .head {{ padding:18px 22px; background:#fafaf9; border-bottom:1px solid #e5e7eb; }}
      .body {{ padding:22px; }}
      label {{ display:block; font-size:13px; font-weight:600; margin-bottom:6px; }}
      input, select {{ width:100%; box-sizing:border-box; border:1px solid #d1d5db; border-radius:10px; padding:10px 12px; font-size:14px; }}
      .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
      .meta {{ margin-top:14px; padding:12px; background:#f8fafc; border:1px solid #e5e7eb; border-radius:10px; font-size:13px; }}
      .meta a {{ color:#2563eb; text-decoration:none; font-weight:600; }}
      button {{ margin-top:18px; background:#111827; color:#fff; border:0; border-radius:10px; padding:12px 16px; font-size:14px; font-weight:700; cursor:pointer; }}
    </style>
  </head>
  <body>
    <div class="card">
      <div class="head">
        <div style="font-size:11px; letter-spacing:.08em; text-transform:uppercase; color:#6b7280;">Lector Facturas · Revision manual</div>
        <div style="margin-top:8px; font-size:22px; font-weight:700;">Corregir factura desde enlace del mail</div>
      </div>
      <div class="body">
        {error_html}
        <div class="meta">
          <div>
            <strong>Archivo pendiente:</strong>
            <a href="/open-file">{html.escape(str(self.source_file))}</a>
          </div>
          <div style="margin-top:8px">
            <a href="/open-file">Abrir factura</a>
          </div>
        </div>
        <form method="post">
          <div class="grid" style="margin-top:18px;">
            <div>
              <label for="company">Empresa</label>
              <select id="company" name="company" onchange="refreshProviders()">{company_options}</select>
            </div>
            <div>
              <label for="supplier_code">Proveedor</label>
              <select id="supplier_code" name="supplier_code"></select>
            </div>
          </div>
          <div class="grid" style="margin-top:16px;">
            <div>
              <label for="invoice_date">Fecha de factura</label>
              <input id="invoice_date" name="invoice_date" value="{html.escape(invoice_date)}" placeholder="2026-03-21" />
            </div>
            <div>
              <label for="invoice_number">Numero de factura</label>
              <input id="invoice_number" name="invoice_number" value="{html.escape(invoice_number)}" placeholder="INV-2026-001" />
            </div>
          </div>
          <button type="submit">Guardar, renombrar y mover</button>
        </form>
      </div>
    </div>
    <script>
      const providerMap = {json.dumps(provider_map)};
      function refreshProviders() {{
        const company = document.getElementById('company').value;
        const select = document.getElementById('supplier_code');
        const options = providerMap[company] || [];
        select.innerHTML = '';
        for (const option of options) {{
          const el = document.createElement('option');
          el.value = option.supplier_code;
          el.textContent = `${{option.provider_name}} (${{option.supplier_code}})`;
          select.appendChild(el);
        }}
        if ('{first_provider_code}') {{
          const wanted = options.find((option) => option.supplier_code === '{first_provider_code}');
          if (wanted) {{
            select.value = wanted.supplier_code;
          }}
        }}
      }}
      refreshProviders();
    </script>
  </body>
</html>
"""

    def _render_success(self, destination_file: Path) -> str:
        provider = get_provider(self._submitted_company(), self._submitted_supplier_code())
        return f"""\
<html>
  <head><meta charset="utf-8"><title>Factura movida</title></head>
  <body style="font-family:Segoe UI, Arial, sans-serif;background:#f3f4f6;padding:24px">
    <div style="max-width:760px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:22px">
      <div style="font-size:22px;font-weight:700;color:#111827">Factura corregida</div>
      <div style="margin-top:12px;color:#374151">La factura se ha renombrado y movido correctamente.</div>
      <div style="margin-top:18px;padding:12px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:10px">
        <div><strong>Proveedor:</strong> {html.escape(provider.provider_name)}</div>
        <div><strong>Destino:</strong> {html.escape(str(destination_file))}</div>
      </div>
    </div>
  </body>
</html>
"""

    def _submitted_supplier_code(self) -> str:
        return getattr(self, "_last_supplier_code", "")

    def _submitted_company(self) -> str:
        return getattr(self, "_last_company", normalize_company(self._guess_company()))

    def _guess_company(self) -> str:
        path_parts = {part.lower(): part for part in self.source_file.parts}
        for company in list_companies():
            last_part = company.lower()
            if last_part in path_parts:
                return company
        return "Artesta Store, S.L"

    def _send_html(self, html_body: str, status: int = 200) -> None:
        payload = html_body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _open_pending_file(self) -> None:
        os.startfile(str(self.source_file))
        self.send_response(302)
        self.send_header("Location", "/")
        self.end_headers()


def _required(data: dict[str, list[str]], key: str) -> str:
    value = (data.get(key) or [""])[0].strip()
    if not value:
        raise ValueError(f"Falta el campo {key}.")
    return value


def _guess_invoice_date(source_file: Path) -> str:
    for part in source_file.parts:
        if len(part) == 6 and part.isdigit():
            return f"{part[:4]}-{part[4:]}-01"
    return "2026-03-01"


def _guess_invoice_number(source_file: Path) -> str:
    return source_file.stem.replace(" ", "_")


def _option(value: str, label: str, *, selected: bool) -> str:
    selected_attr = " selected" if selected else ""
    return f"<option value=\"{html.escape(value)}\"{selected_attr}>{html.escape(label)}</option>"


def main() -> int:
    args = parse_args()
    source_file = Path(args.file)
    if not source_file.exists():
        raise SystemExit(f"File not found: {source_file}")

    handler = type(
        "ConfiguredReviewHandler",
        (ReviewHandler,),
        {
            "root_path": Path(args.root),
            "source_file": source_file,
        },
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    url = f"http://{args.host}:{args.port}/"
    if args.open_browser:
        webbrowser.open(url)
    print(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
