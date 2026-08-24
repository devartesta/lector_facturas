from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_regen_hourly():
    script_path = Path(__file__).parents[1] / "scripts" / "regen_hourly.py"
    spec = importlib.util.spec_from_file_location("regen_hourly", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_embedded_failures_reports_failed_substeps() -> None:
    module = _load_regen_hourly()
    body = json.dumps(
        {
            "results": [
                {"step": "gestoria/SL", "status": "ok"},
                {"step": "gestoria/LTD", "status": "error"},
                {"step": "gestoria/INC", "status": "blocked"},
            ]
        }
    )

    assert module._embedded_failures(body) == ["gestoria/LTD", "gestoria/INC"]


def test_embedded_failures_ignores_non_job_response() -> None:
    module = _load_regen_hourly()

    assert module._embedded_failures("not json") == []
    assert module._embedded_failures(json.dumps({"status": "ok"})) == []
