from __future__ import annotations

import calendar
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo


DISPLAY_TIMEZONE = ZoneInfo("Europe/Madrid")
SHOPIFY_DAILY_AVERAGE_LABEL = "Shopify daily average"
MINIMUM_ELAPSED_DAYS = Decimal("0.0416666666666667")


def daily_sales_divisor(yyyymm: str, *, now: datetime | None = None) -> Decimal:
    """Return the divisor used by the P&G Shopify daily-sales metric."""
    year = int(yyyymm[:4])
    month = int(yyyymm[4:])
    current_value = now or datetime.now(DISPLAY_TIMEZONE)
    current = (
        current_value
        if current_value.tzinfo is None
        else current_value.astimezone(DISPLAY_TIMEZONE).replace(tzinfo=None)
    )
    month_start = datetime(year, month, 1)
    target_month = (year, month)
    current_month = (current.year, current.month)
    if target_month > current_month:
        return Decimal("0")
    if target_month < current_month:
        return Decimal(calendar.monthrange(year, month)[1])
    elapsed_days = Decimal(str((current - month_start).total_seconds() / 86400))
    return max(elapsed_days, MINIMUM_ELAPSED_DAYS)


def excel_daily_sales_formula(*, column: str, shopify_row: int, header_row: int = 1) -> str:
    """Build the Excel formula matching :func:`daily_sales_divisor`."""
    header = f"{column}${header_row}"
    month_start = f'DATE(VALUE(LEFT({header},4)),VALUE(RIGHT({header},2)),1)'
    return (
        f'=IFERROR(IF({month_start}>TODAY(),"",'
        f'IF(EOMONTH({month_start},0)<TODAY(),'
        f'{column}{shopify_row}/DAY(EOMONTH({month_start},0)),'
        f'{column}{shopify_row}/MAX(NOW()-{month_start},1/24))),"")'
    )
