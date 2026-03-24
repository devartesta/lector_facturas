from __future__ import annotations

from pathlib import Path
from threading import Thread
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from worker_02_email_review import main as email_review_worker_main
from worker_04_payment_fees import main as payment_fees_worker_main
from worker_08_pyg_consolidated import main as pyg_consolidated_worker_main
from worker_07_pyg_inc import main as pyg_inc_worker_main
from worker_06_pyg_ltd import main as pyg_ltd_worker_main
from worker_05_pyg_sl import main as pyg_sl_worker_main


def _run_named(name: str, target) -> None:
    try:
        target()
    except Exception as exc:  # noqa: BLE001
        print(f"[combined-worker] {name} crashed | {exc}", flush=True)
        raise


def main() -> None:
    print(
        "[combined-worker] starting 02-email-review, 04-payment-fees, "
        "05-pyg-sl, 06-pyg-ltd, 07-pyg-inc, 08-pyg-consolidated",
        flush=True,
    )
    threads = [
        Thread(
            target=_run_named,
            args=("02-email-review", email_review_worker_main),
            daemon=False,
            name="02-email-review",
        ),
        Thread(
            target=_run_named,
            args=("04-payment-fees", payment_fees_worker_main),
            daemon=False,
            name="04-payment-fees",
        ),
        Thread(
            target=_run_named,
            args=("05-pyg-sl", pyg_sl_worker_main),
            daemon=False,
            name="05-pyg-sl",
        ),
        Thread(
            target=_run_named,
            args=("06-pyg-ltd", pyg_ltd_worker_main),
            daemon=False,
            name="06-pyg-ltd",
        ),
        Thread(
            target=_run_named,
            args=("07-pyg-inc", pyg_inc_worker_main),
            daemon=False,
            name="07-pyg-inc",
        ),
        Thread(
            target=_run_named,
            args=("08-pyg-consolidated", pyg_consolidated_worker_main),
            daemon=False,
            name="08-pyg-consolidated",
        ),
    ]
    for thread in threads:
        thread.start()
    while True:
        for thread in threads:
            if not thread.is_alive():
                raise RuntimeError(f"{thread.name} stopped unexpectedly")
        time.sleep(30)


if __name__ == "__main__":
    main()
