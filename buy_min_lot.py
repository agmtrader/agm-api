from src.utils.connectors.ibkr_web_api import IBKRWebAPI
import json
import requests
import time
import csv
from datetime import datetime

""" BUY MINIMUM LOT """

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ORDER_DELAY = 0.5          # seconds between order attempts (rate-limit guard)
MAX_QTY_ATTEMPTS = 200     # safety cap so we don't loop forever
LIMIT_PRICE = 50.0         # low limit price so the order never fills
OUTPUT_CSV = "minimum_lots.csv"


def login(ibkr: IBKRWebAPI, credential: str, ip: str) -> str:
    """Log in via SSO, initialise brokerage session, return account ID."""
    print("\n--- Creating SSO session ---")
    sso = ibkr.create_sso_session(credential=credential, ip=ip)
    print(json.dumps(sso, indent=2))

    print("\n--- Initializing brokerage session ---")
    brokerage = ibkr.initialize_brokerage_session()
    print(json.dumps(brokerage, indent=2))

    print("\n--- Fetching brokerage accounts ---")
    accounts = ibkr.get_brokerage_accounts()
    print(json.dumps(accounts, indent=2))

    account_id = accounts.get("accounts", [None])[0]
    if not account_id:
        raise Exception("No brokerage account found after login")
    print(f"\nUsing account: {account_id}")
    return account_id


def get_bond_conids(ibkr: IBKRWebAPI) -> list[dict]:
    """
    Pull the list of bonds to test.
    Strategy: grab all watchlists, then collect instruments from each one.
    Returns a list of dicts: [{"conid": ..., "name": ...}, ...]
    """
    print("\n--- Fetching watchlists ---")
    watchlists = ibkr.get_all_watchlists()
    print(json.dumps(watchlists, indent=2))

    bonds = []
    if not watchlists or not isinstance(watchlists, dict):
        return bonds

    for wl in watchlists.get("data", watchlists.get("user_lists", [])):
        wl_id = wl.get("id")
        if not wl_id:
            continue
        print(f"\n  Fetching watchlist: {wl.get('name', wl_id)}")
        info = ibkr.get_watchlist_information(watchlist_id=str(wl_id))
        if not info:
            continue

        instruments = info if isinstance(info, list) else info.get("instruments", info.get("rows", []))
        for inst in instruments:
            conid = inst.get("conid") or inst.get("C")
            name = inst.get("name") or inst.get("ticker") or inst.get("N") or str(conid)
            if conid:
                bonds.append({"conid": int(conid), "name": name})

    print(f"\nCollected {len(bonds)} instruments from watchlists")
    return bonds


def place_and_check(ibkr: IBKRWebAPI, account_id: str, conid: int, qty: int) -> dict:
    """
    Place a limit buy order and handle any confirmation prompts.
    Returns the final API response dict.
    """
    order = {
        "conid": conid,
        "orderType": "LMT",
        "price": LIMIT_PRICE,
        "side": "BUY",
        "quantity": qty,
        "tif": "DAY",
    }
    result = ibkr.place_order(account_id=account_id, orders=[order])

    # IBKR may return confirmation prompts that need a reply
    if isinstance(result, list):
        for item in result:
            reply_id = item.get("id")
            if reply_id and "message" in item:
                messages = item.get("message", [])
                print(f"    Confirmation prompt: {messages}")
                result = ibkr.reply_to_order(reply_id=str(reply_id), confirmed=True)
                # Could be another round of prompts
                if isinstance(result, list):
                    for nested in result:
                        nested_reply_id = nested.get("id")
                        if nested_reply_id and "message" in nested:
                            print(f"    Nested confirmation: {nested.get('message')}")
                            result = ibkr.reply_to_order(reply_id=str(nested_reply_id), confirmed=True)
    return result


def extract_order_id(response) -> str | None:
    """Try to pull an order_id from a place_order / reply response."""
    if isinstance(response, list):
        for item in response:
            oid = item.get("order_id") or item.get("orderId")
            if oid:
                return str(oid)
    elif isinstance(response, dict):
        oid = response.get("order_id") or response.get("orderId")
        if oid:
            return str(oid)
    return None


def is_order_accepted(response) -> bool:
    """Determine whether the order was accepted (not rejected)."""
    if isinstance(response, list):
        for item in response:
            order_id = item.get("order_id") or item.get("orderId")
            order_status = str(item.get("order_status", "")).lower()
            if order_id and order_status not in ("rejected", "cancelled", "error"):
                return True
    elif isinstance(response, dict):
        order_id = response.get("order_id") or response.get("orderId")
        order_status = str(response.get("order_status", "")).lower()
        if order_id and order_status not in ("rejected", "cancelled", "error"):
            return True
    return False


def is_size_error(response) -> bool:
    """Check whether the rejection is specifically about order size / minimum lot."""
    size_keywords = [
        "minimum lot", "order size", "too small", "below minimum",
        "min size", "lot size", "increments of", "minimum quantity",
        "minimum order", "size requirement",
    ]
    text = json.dumps(response).lower()
    return any(kw in text for kw in size_keywords)


def find_minimum_lot(ibkr: IBKRWebAPI, account_id: str, conid: int, name: str) -> int | None:
    """
    Brute-force discover the minimum lot for a single bond by incrementing
    quantity until the order is accepted, then immediately cancel it.
    Returns the minimum quantity or None if it could not be determined.
    """
    print(f"\n{'='*60}")
    print(f"Testing bond: {name} (conid {conid})")
    print(f"{'='*60}")

    qty = 1
    while qty <= MAX_QTY_ATTEMPTS:
        print(f"  Trying qty = {qty} ...", end=" ")
        try:
            response = place_and_check(ibkr, account_id, conid, qty)
            print(f"Response: {json.dumps(response, indent=None)}")

            if is_order_accepted(response):
                print(f"  >> ORDER ACCEPTED at qty = {qty}")

                # Cancel the order immediately
                order_id = extract_order_id(response)
                if order_id:
                    print(f"  Cancelling order {order_id} ...")
                    cancel_resp = ibkr.cancel_order(account_id=account_id, order_id=order_id)
                    print(f"  Cancel response: {json.dumps(cancel_resp, indent=None)}")

                return qty

            # Order was not accepted — check if it's a size error
            if is_size_error(response):
                print(f"  Size-related rejection at qty = {qty}, incrementing...")
                qty += 1
                time.sleep(ORDER_DELAY)
                continue

            # Some other rejection (invalid symbol, market closed, etc.)
            print(f"  Non-size rejection — skipping this bond.")
            return None

        except Exception as e:
            error_text = str(e).lower()
            if any(kw in error_text for kw in ["minimum lot", "order size", "too small", "below minimum", "lot size", "increments of"]):
                print(f"  Size error in exception at qty = {qty}, incrementing...")
                qty += 1
                time.sleep(ORDER_DELAY)
                continue
            else:
                print(f"  Unexpected error: {e}")
                return None

    print(f"  Reached max attempts ({MAX_QTY_ATTEMPTS}) — could not determine minimum lot.")
    return None


def save_results(results: list[dict]):
    """Write results to a CSV file."""
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["conid", "name", "minimum_lot", "timestamp"])
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults saved to {OUTPUT_CSV}")


def main():
    ibkr = IBKRWebAPI()

    credential = 'aguilarcarboni24pt'
    ip = requests.get('https://api.ipify.org').content.decode('utf8')

    # Step 1 — Login
    account_id = login(ibkr, credential, ip)

    # Step 2 — Collect bonds from watchlists
    bonds = get_bond_conids(ibkr)
    if not bonds:
        print("No bonds found in watchlists. Exiting.")
        return

    # Step 3 — Find minimum lot for each bond
    results = []
    for bond in bonds:
        min_lot = find_minimum_lot(ibkr, account_id, bond["conid"], bond["name"])
        entry = {
            "conid": bond["conid"],
            "name": bond["name"],
            "minimum_lot": min_lot if min_lot is not None else "UNKNOWN",
            "timestamp": datetime.now().isoformat(),
        }
        results.append(entry)
        print(f"  => {bond['name']}: minimum lot = {entry['minimum_lot']}")

    # Step 4 — Save to CSV
    save_results(results)

    # Print summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    for r in results:
        print(f"  {r['name']:30s} conid={r['conid']}  min_lot={r['minimum_lot']}")

    print("\nDone.")


if __name__ == "__main__":
    main()
