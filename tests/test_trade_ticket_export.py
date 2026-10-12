"""Run export/JWT acceptance in isolation so connector stubs cannot leak to other tests."""
from pathlib import Path
import subprocess
import sys


def test_trade_ticket_export_and_authorization_acceptance():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / 'tests/fixtures/trade_ticket_acceptance.py')],
        cwd=root, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
