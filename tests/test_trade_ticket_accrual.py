"""Exercise the real generator/routes with isolated external connector stubs."""
from pathlib import Path
import subprocess
import sys


def test_trade_ticket_accrual_acceptance():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, str(root / 'tests/fixtures/trade_ticket_accrual_acceptance.py')],
        cwd=root, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
