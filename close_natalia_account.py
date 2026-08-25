"""Submit Natalia Koptiuk's IBKR account closure request.

This script is intentionally dry-run by default.  Pass ``--execute`` only
after confirming the account is eligible in IBKR (status O or Q and zero
cleared balance), because the request changes external account state.
"""

import argparse
import json

from src.utils.connectors.ibkr_web_api import IBKRWebAPI


ACCOUNT_ID = "U4512987"
ACCOUNT_TITLE = "Nataliia Koptiuk"
CLOSE_REASON = "The user is no longer interested in an investment account in AGM."


def main() -> None:
    parser = argparse.ArgumentParser(description=f"Close {ACCOUNT_TITLE}'s IBKR account")
    parser.add_argument(
        "--master-account",
        default=None,
        help="IBKR master account credential set (defaults to the connector default)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit the closure request; without this flag only print the payload",
    )
    args = parser.parse_args()

    request_data = {
        "account_id": ACCOUNT_ID,
        "close_reason": CLOSE_REASON,
        "master_account": 'I6413690',
    }
    print(json.dumps({"target": ACCOUNT_TITLE, **request_data}, indent=2))

    if not args.execute:
        print("Dry run only. Re-run with --execute to submit this request to IBKR.")
        return

    response = IBKRWebAPI().close_account(account_id=ACCOUNT_ID, close_reason=CLOSE_REASON, master_account='I6413690')
    print(json.dumps(response, indent=2, default=str))


if __name__ == "__main__":
    main()
