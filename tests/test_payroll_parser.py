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


if __name__ == "__main__":
    unittest.main()
