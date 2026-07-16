from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.payroll import parse_payroll_summary_text


JAN_SAMPLE = dedent(
    """
    Resumen de Nmina
    PAGA TOTAL DEL 01/01/2026 AL 31/01/2026
    68 ARTESTA STORE S.L.
    -725,83                                        3.565,17       14.731,84                 TOTAL EMPRESA
      11.166,67       -2.432,21                                        4.291,00        8.008,63
    TOTAL TRABAJADORES EMPRESA                         3
    """
)

SEPTEMBER_SAMPLE = dedent(
    """
    Resumen de Nómina
    PAGA MENSUAL + FINIQUITO DEL 01/09/2025 AL 30/09/2025
    68 ARTESTA STORE S.L.
    -723,60                                        3.556,23       14.722,90                 TOTAL EMPRESA
      11.166,67       -2.433,31                                        4.279,83        8.009,76
    TOTAL TRABAJADORES EMPRESA                         3
    """
)

MARCH_REORDERED_SAMPLE = dedent(
    """
    Resumen de Nómina
    PAGA TOTAL DEL 01/03/2026 AL 31/03/2026
    68 ARTESTA STORE S.L.
    15.349,42           63,77                                        3.657,74                         -734,52 TOTAL EMPRESA
      11.691,68       -2.496,08                                        4.392,26        8.461,08
    TOTAL TRABAJADORES EMPRESA                         4
    """
)


class PayrollParserTests(unittest.TestCase):
    def test_parse_january_payroll_summary(self) -> None:
        parsed = parse_payroll_summary_text(JAN_SAMPLE, original_filename="ARTESTA STORE RESUMEN NOMINA.pdf")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.employee_count, 3)
        self.assertEqual(parsed.gross_pay_amount, Decimal("11166.67"))
        self.assertEqual(parsed.employee_deductions_amount, Decimal("725.83"))
        self.assertEqual(parsed.tax_withholdings_amount, Decimal("2432.21"))
        self.assertEqual(parsed.social_security_liquidation_amount, Decimal("4291.00"))
        self.assertEqual(parsed.net_pay_amount, Decimal("8008.63"))
        self.assertEqual(parsed.employer_social_security_amount, Decimal("3565.17"))
        self.assertEqual(parsed.total_company_cost_amount, Decimal("14731.84"))

    def test_parse_finiquito_period_heading(self) -> None:
        parsed = parse_payroll_summary_text(SEPTEMBER_SAMPLE, original_filename="ARTESTA STORE RESUMEN NOMINA.pdf")
        self.assertEqual(parsed.period_yyyymm, "202509")
        self.assertEqual(parsed.employee_count, 3)
        self.assertEqual(parsed.gross_pay_amount, Decimal("11166.67"))
        self.assertEqual(parsed.employee_deductions_amount, Decimal("723.60"))
        self.assertEqual(parsed.tax_withholdings_amount, Decimal("2433.31"))
        self.assertEqual(parsed.social_security_liquidation_amount, Decimal("4279.83"))
        self.assertEqual(parsed.net_pay_amount, Decimal("8009.76"))
        self.assertEqual(parsed.employer_social_security_amount, Decimal("3556.23"))
        self.assertEqual(parsed.total_company_cost_amount, Decimal("14722.90"))

    def test_parse_reordered_company_total_line(self) -> None:
        parsed = parse_payroll_summary_text(MARCH_REORDERED_SAMPLE, original_filename="ARTESTA STORE RESUMEN NOMINA.pdf")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.employee_count, 4)
        self.assertEqual(parsed.gross_pay_amount, Decimal("11691.68"))
        self.assertEqual(parsed.employee_deductions_amount, Decimal("734.52"))
        self.assertEqual(parsed.tax_withholdings_amount, Decimal("2496.08"))
        self.assertEqual(parsed.social_security_liquidation_amount, Decimal("4392.26"))
        self.assertEqual(parsed.net_pay_amount, Decimal("8461.08"))
        self.assertEqual(parsed.employer_social_security_amount, Decimal("3657.74"))
        self.assertEqual(parsed.total_company_cost_amount, Decimal("15349.42"))


if __name__ == "__main__":
    unittest.main()
