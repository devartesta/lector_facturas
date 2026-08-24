from lector_facturas.period_lock import frozen_periods


class _Result:
    def __init__(self, *, one=None, many=None):
        self._one = one
        self._many = many or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _ExistingStoreConnection:
    def __init__(self):
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((statement, params))
        if "to_regclass" in statement:
            return _Result(one={"table_name": "finance.sales_period_freezes"})
        if "SELECT period_yyyymm" in statement:
            return _Result(many=[{"period_yyyymm": "202606"}])
        raise AssertionError(f"Unexpected statement: {statement}")


def test_frozen_periods_does_not_run_ddl_when_store_exists():
    conn = _ExistingStoreConnection()

    assert frozen_periods(conn, company_code="SL", year=2026) == {"202606"}
    assert all("CREATE " not in statement.upper() for statement, _ in conn.statements)
