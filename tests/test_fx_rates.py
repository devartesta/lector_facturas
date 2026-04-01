from __future__ import annotations

from decimal import Decimal

import lector_facturas.fx_rates as fx_rates


ECB_SAMPLE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-01-30">
      <Cube currency="USD" rate="1.2000"/>
      <Cube currency="GBP" rate="0.8000"/>
    </Cube>
    <Cube time="2026-02-27">
      <Cube currency="USD" rate="1.2500"/>
      <Cube currency="GBP" rate="0.8200"/>
    </Cube>
    <Cube time="2026-03-31">
      <Cube currency="USD" rate="1.3000"/>
      <Cube currency="GBP" rate="0.8400"/>
    </Cube>
  </Cube>
</gesmes:Envelope>
"""


class _FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self) -> bytes:
        return ECB_SAMPLE_XML


def test_ecb_fx_uses_last_available_day_of_month(monkeypatch) -> None:
    fx_rates.EcbFxService._daily_rates.cache_clear()
    monkeypatch.setattr(fx_rates, "urlopen", lambda *args, **kwargs: _FakeResponse())

    service = fx_rates.EcbFxService()
    converted, audit = service.convert(
        amount=Decimal("12.50"),
        source_currency="USD",
        reporting_currency="EUR",
        yyyymm="202602",
    )

    assert converted.amount_reporting == Decimal("10.00")
    assert converted.fx_rate == Decimal("0.80000000")
    assert audit.rate_date == "2026-02-27"


def test_ecb_fx_supports_cross_currency_conversion(monkeypatch) -> None:
    fx_rates.EcbFxService._daily_rates.cache_clear()
    monkeypatch.setattr(fx_rates, "urlopen", lambda *args, **kwargs: _FakeResponse())

    service = fx_rates.EcbFxService()
    converted, audit = service.convert(
        amount=Decimal("12.50"),
        source_currency="USD",
        reporting_currency="GBP",
        yyyymm="202602",
    )

    assert converted.amount_reporting == Decimal("8.20")
    assert converted.fx_rate == Decimal("0.65600000")
    assert audit.reporting_currency == "GBP"


def test_ecb_fx_identity_conversion_does_not_fetch_network() -> None:
    service = fx_rates.EcbFxService()
    converted, audit = service.convert(
        amount=Decimal("100.00"),
        source_currency="EUR",
        reporting_currency="EUR",
        yyyymm="202603",
    )

    assert converted.amount_reporting == Decimal("100.00")
    assert converted.fx_rate == Decimal("1")
    assert audit.source == "identity"
