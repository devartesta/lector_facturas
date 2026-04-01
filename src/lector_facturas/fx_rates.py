from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from urllib.request import urlopen
from xml.etree import ElementTree


ECB_HIST_XML_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"
FX_RATE_QUANT = Decimal("0.00000001")
MONEY_QUANT = Decimal("0.01")


@dataclass(frozen=True)
class ConvertedAmount:
    amount_original: Decimal
    currency_original: str
    reporting_currency: str
    fx_rate: Decimal
    amount_reporting: Decimal


@dataclass(frozen=True)
class FxRateAuditRow:
    yyyymm: str
    rate_date: str
    currency_original: str
    reporting_currency: str
    reference_rate: Decimal
    fx_rate: Decimal
    source: str


@dataclass(frozen=True)
class FxMonthRate:
    yyyymm: str
    currency: str
    rate_date: date
    reference_rate: Decimal
    source: str


class EcbFxService:
    def convert(self, *, amount: Decimal, source_currency: str, reporting_currency: str, yyyymm: str) -> tuple[ConvertedAmount, FxRateAuditRow]:
        source = source_currency.strip().upper() or reporting_currency.strip().upper()
        target = reporting_currency.strip().upper()
        if not target:
            raise ValueError("reporting_currency is required")
        if not yyyymm or len(yyyymm) != 6:
            raise ValueError(f"Invalid yyyymm for FX conversion: {yyyymm!r}")
        if source == target:
            audit = FxRateAuditRow(
                yyyymm=yyyymm,
                rate_date=f"{yyyymm[:4]}-{yyyymm[4:]}-01",
                currency_original=source,
                reporting_currency=target,
                reference_rate=Decimal("1"),
                fx_rate=Decimal("1"),
                source="identity",
            )
            return (
                ConvertedAmount(
                    amount_original=amount,
                    currency_original=source,
                    reporting_currency=target,
                    fx_rate=Decimal("1"),
                    amount_reporting=_quantize_money(amount),
                ),
                audit,
            )

        source_month_rate = self.month_rate(yyyymm=yyyymm, currency=source)
        amount_in_eur = amount / source_month_rate.reference_rate
        if target == "EUR":
            fx_rate = Decimal("1") / source_month_rate.reference_rate
            amount_reporting = amount_in_eur
            rate_date = source_month_rate.rate_date
            reference_rate = source_month_rate.reference_rate
            source_label = source_month_rate.source
        else:
            target_month_rate = self.month_rate(yyyymm=yyyymm, currency=target)
            fx_rate = target_month_rate.reference_rate / source_month_rate.reference_rate
            amount_reporting = amount_in_eur * target_month_rate.reference_rate
            rate_date = min(source_month_rate.rate_date, target_month_rate.rate_date)
            reference_rate = source_month_rate.reference_rate
            source_label = f"{source_month_rate.source} -> {target_month_rate.source}"
        fx_rate = fx_rate.quantize(FX_RATE_QUANT, rounding=ROUND_HALF_UP)
        amount_reporting = _quantize_money(amount_reporting)
        return (
            ConvertedAmount(
                amount_original=amount,
                currency_original=source,
                reporting_currency=target,
                fx_rate=fx_rate,
                amount_reporting=amount_reporting,
            ),
            FxRateAuditRow(
                yyyymm=yyyymm,
                rate_date=rate_date.isoformat(),
                currency_original=source,
                reporting_currency=target,
                reference_rate=reference_rate,
                fx_rate=fx_rate,
                source=source_label,
            ),
        )

    def month_rate(self, *, yyyymm: str, currency: str) -> FxMonthRate:
        normalized = currency.strip().upper()
        if normalized == "EUR":
            return FxMonthRate(
                yyyymm=yyyymm,
                currency="EUR",
                rate_date=date(int(yyyymm[:4]), int(yyyymm[4:]), 1),
                reference_rate=Decimal("1"),
                source="identity",
            )
        year = int(yyyymm[:4])
        month = int(yyyymm[4:])
        matching_dates = [day for day in self._daily_rates().keys() if day.year == year and day.month == month and normalized in self._daily_rates()[day]]
        if not matching_dates:
            # Fallback for future months: use the latest available ECB rate
            all_dates = [day for day in self._daily_rates().keys() if normalized in self._daily_rates()[day]]
            if not all_dates:
                raise RuntimeError(f"No ECB FX rate found for {normalized} in any month.")
            rate_date = max(all_dates)
            return FxMonthRate(
                yyyymm=yyyymm,
                currency=normalized,
                rate_date=rate_date,
                reference_rate=self._daily_rates()[rate_date][normalized],
                source=f"{ECB_HIST_XML_URL} (latest available, no ECB data for {yyyymm})",
            )
        rate_date = max(matching_dates)
        return FxMonthRate(
            yyyymm=yyyymm,
            currency=normalized,
            rate_date=rate_date,
            reference_rate=self._daily_rates()[rate_date][normalized],
            source=ECB_HIST_XML_URL,
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def _daily_rates() -> dict[date, dict[str, Decimal]]:
        with urlopen(ECB_HIST_XML_URL, timeout=30) as response:
            payload = response.read()
        root = ElementTree.fromstring(payload)
        ns = {"gesmes": "http://www.gesmes.org/xml/2002-08-01", "def": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
        daily: dict[date, dict[str, Decimal]] = {}
        for cube_time in root.findall(".//def:Cube[@time]", ns):
            rate_date = date.fromisoformat(cube_time.attrib["time"])
            daily[rate_date] = {}
            for cube_rate in cube_time.findall("def:Cube[@currency]", ns):
                daily[rate_date][cube_rate.attrib["currency"].upper()] = Decimal(cube_rate.attrib["rate"])
        if not daily:
            raise RuntimeError("ECB FX feed returned no rates.")
        return daily


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
