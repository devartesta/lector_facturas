from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.pyg_sync import sync_payment_fee_detail_to_drive
from lector_facturas.settings import load_settings


def _default_end_period(year: int) -> str:
    now = datetime.now()
    if now.year == year:
        return f"{year}{now.month:02d}"
    return f"{year}12"


def _iter_periods(start_period: str, end_period: str) -> list[str]:
    start_year = int(start_period[:4])
    start_month = int(start_period[4:6])
    end_year = int(end_period[:4])
    end_month = int(end_period[4:6])

    periods: list[str] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year}{month:02d}")
        month += 1
        if month > 12:
            year += 1
            month = 1
    return periods


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill monthly payment fee detail workbooks to Drive.")
    parser.add_argument("--year", type=int, default=2026, help="Fiscal year to regenerate.")
    parser.add_argument("--start-period", default=None, help="Inclusive YYYYMM start period.")
    parser.add_argument("--end-period", default=None, help="Inclusive YYYYMM end period.")
    parser.add_argument(
        "--company",
        dest="companies",
        action="append",
        choices=("SL", "LTD", "INC"),
        help="Optional company filter. Repeat for multiple companies.",
    )
    args = parser.parse_args()

    settings = load_settings()
    companies = args.companies or ["SL", "LTD", "INC"]
    start_period = args.start_period or f"{args.year}01"
    end_period = args.end_period or _default_end_period(args.year)
    periods = _iter_periods(start_period, end_period)

    print(f"Backfilling payment fee detail workbooks for {companies} and periods {periods[0]}..{periods[-1]}")
    for company_code in companies:
        for period_yyyymm in periods:
            result = sync_payment_fee_detail_to_drive(
                settings=settings,
                company_code=company_code,
                period_yyyymm=period_yyyymm,
            )
            print(
                f"{company_code} {period_yyyymm} -> {result.drive_file_name} "
                f"({result.drive_file_id}) {result.drive_file_url}"
            )


if __name__ == "__main__":
    main()
