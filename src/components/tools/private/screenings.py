from datetime import date, datetime
import re
import unicodedata
from src.components.clients.accounts import read_accounts
from src.components.clients.contacts import create_contact_screening_from_contact_id
from src.components.tools.public.reporting import get_ibkr_details
from src.components.tools.public.reporting import compare_all_sanctions_today_vs_yesterday
from src.utils.connectors.supabase import db
from src.utils.logger import logger

APPLY_SCREENINGS = True
# CHECK IF PEOPLE NEED SCREENING PENDING
def normalize_name(name: str) -> str:
    if not name:
        return ""
    ascii_name = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    )
    normalized = re.sub(r"[^a-z0-9 ]+", " ", ascii_name.lower())
    return " ".join(normalized.split())


def parse_screen_created(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").date()
    except ValueError:
        return None

def person_name_candidates(person: dict) -> set[str]:
    first = (person.get("firstName") or "").strip()
    middle = (person.get("middleName") or "").strip()
    middle_initial = (person.get("middleInitial") or "").strip()
    last = (person.get("lastName") or "").strip()

    raw_candidates = set()
    if first and last:
        raw_candidates.add(f"{first} {last}")
    if first and middle and last:
        raw_candidates.add(f"{first} {middle} {last}")
    if first and middle_initial and last:
        raw_candidates.add(f"{first} {middle_initial} {last}")

    return {normalize_name(c) for c in raw_candidates if c.strip()}


def _compact_screenings_result(
    payload: dict,
) -> dict:
    result = dict(payload)
    screening_errors = result.get("screening_errors") or []
    contacts_targeted_rows = result.get("contacts_targeted_rows") or []

    result["screening_errors_count"] = len(screening_errors)
    result["contacts_targeted_rows_count"] = len(contacts_targeted_rows)
    result["screening_errors_truncated"] = len(screening_errors) > 0
    result["contacts_targeted_rows_truncated"] = len(contacts_targeted_rows) > 0
    result["screening_errors"] = []
    result["contacts_targeted_rows"] = []
    return result


def run_screenings(apply_screenings: bool = APPLY_SCREENINGS) -> dict:
    sanctions_comparison = compare_all_sanctions_today_vs_yesterday()

    list_labels = [("ofac", "OFAC"), ("uk", "UK"), ("un", "UN")]
    changed_lists = []
    unchanged_lists = []
    unavailable_lists = []

    if isinstance(sanctions_comparison, dict):
        for key, label in list_labels:
            comparison = sanctions_comparison.get(key)
            if not isinstance(comparison, dict):
                unavailable_lists.append(f"{label}(invalid)")
                continue
            if comparison.get("error"):
                unavailable_lists.append(f"{label}(error)")
            elif not comparison.get("both_available"):
                missing_parts = []
                if comparison.get("missing_today"):
                    missing_parts.append("today")
                if comparison.get("missing_yesterday"):
                    missing_parts.append("yesterday")
                suffix = "/".join(missing_parts) if missing_parts else "unknown"
                unavailable_lists.append(f"{label}({suffix})")
            elif comparison.get("is_same"):
                unchanged_lists.append(label)
            else:
                changed_lists.append(label)

    logger.info(
        "Sanctions delta vs yesterday | "
        f"changed: {', '.join(changed_lists) if changed_lists else 'none'} | "
        f"unchanged: {', '.join(unchanged_lists) if unchanged_lists else 'none'} | "
        f"unavailable: {', '.join(unavailable_lists) if unavailable_lists else 'none'}"
    )
    if isinstance(sanctions_comparison, dict):
        for key, label in list_labels:
            comparison = sanctions_comparison.get(key)
            if not isinstance(comparison, dict):
                continue
            if comparison.get("both_available") and not comparison.get("is_same"):
                summary = comparison.get("change_summary") or {}
                logger.info(
                    f"{label} changes | added: {summary.get('added_count', 0)} | "
                    f"removed: {summary.get('removed_count', 0)}"
                )
                added_samples = summary.get("added_samples") or []
                removed_samples = summary.get("removed_samples") or []
                if added_samples:
                    logger.info(f"{label} added samples: {added_samples}")
                if removed_samples:
                    logger.info(f"{label} removed samples: {removed_samples}")

    if isinstance(sanctions_comparison, dict) and sanctions_comparison.get("error"):
        logger.warning(
            f"Sanctions comparison failed. Continuing screenings. Error: {sanctions_comparison.get('error')}"
        )
    elif len(unavailable_lists) > 0:
        raise Exception(f"Sanctions files unavailable: {', '.join(unavailable_lists)}")
    elif sanctions_comparison.get("all_available") and sanctions_comparison.get("all_same"):
        return _compact_screenings_result({
            "apply_screenings": apply_screenings,
            "screenings_skipped": True,
            "skip_reason": "OFAC, UK, and UN sanctions lists unchanged vs yesterday",
            "sanctions_comparison": sanctions_comparison,
            "contacts_targeted": 0,
            "contacts_with_no_screenings": 0,
            "accounts_with_some_contacts_no_screenings": 0,
            "contacts_targeted_rows": [],
            "accounts_targeted": 0,
            "accounts_with_no_screenings_at_all": 0,
            "accounts_with_some_screenings": 0,
            "total_screenings_planned": 0,
            "screenings_executed": 0,
            "screening_errors": [],
        })

    accounts = read_accounts({})
    details = get_ibkr_details()
    account_contact_rows = db.read("account_contact", query={}) or []
    contact_rows = db.read("contact", query={}) or []
    contact_screen_rows = db.read("contact_screening", query={}) or []

    account_contacts_by_account = {}
    for row in account_contact_rows:
        account_id = row.get("account_id")
        if not account_id:
            continue
        account_contacts_by_account.setdefault(account_id, []).append(row)

    contacts_by_id = {row.get("id"): row for row in contact_rows if row.get("id")}
    contact_screens_by_contact_id = {}
    for screen in contact_screen_rows:
        contact_id = screen.get("contact_id")
        if not contact_id:
            continue
        contact_screens_by_contact_id.setdefault(contact_id, []).append(screen)

    details_by_ibkr_number = {}
    for detail in details:
        ibkr_number = detail.get("account", {}).get("accountId")
        if ibkr_number:
            details_by_ibkr_number[ibkr_number] = detail

    accounts_with_some_contacts_no_screenings = 0
    accounts_with_no_screenings_at_all = 0
    accounts_with_some_screenings = 0
    contacts_with_no_screenings = 0
    accounts_targeted = 0
    contacts_targeted = 0
    total_screenings_planned = 0
    screenings_executed = 0
    screening_errors = []
    contacts_targeted_rows = []
    planned_contact_ids = []

    today = date.today()
    created_value_today = today.strftime("%Y%m%d000000")

    for account in accounts:
        ibkr_number = account.get("ibkr_account_number")
        account_id = account.get("id")
        detail = details_by_ibkr_number.get(ibkr_number, {})
        detail_account = detail.get("account", {})
        persons = detail.get("associatedPersons") or []
        account_contact_links = account_contacts_by_account.get(account_id, [])
        persons_by_entity_id = {
            str(person.get("entityId")).strip(): person
            for person in persons
            if person.get("entityId") is not None
        }

        date_opened = detail_account.get("dateOpened")

        has_any_screenings = False
        has_any_missing_screenings = False
        account_has_targeted_contacts = False

        for link in account_contact_links:
            person_contact_id = link.get("contact_id")
            if not person_contact_id:
                continue

            contact = contacts_by_id.get(person_contact_id) or {}
            display_name = (contact.get("name") or "").strip()
            if not display_name:
                continue

            matched_person = None
            entity_id = link.get("entity_id")
            if entity_id is not None:
                matched_person = persons_by_entity_id.get(str(entity_id).strip())

            if matched_person is None:
                normalized_contact_name = normalize_name(display_name)
                for person in persons:
                    if normalized_contact_name in person_name_candidates(person):
                        matched_person = person
                        break

            roles = (matched_person or {}).get("associations") or []
            if not roles and contact.get("type"):
                roles = [contact.get("type")]
            person_screens = contact_screens_by_contact_id.get(person_contact_id, [])

            roles_str = ", ".join(roles)

            if person_screens:
                has_any_screenings = True
            else:
                has_any_missing_screenings = True
                contacts_with_no_screenings += 1

            latest_screen_date = None
            for screen in person_screens:
                created_date = parse_screen_created(screen.get("created"))
                if created_date and (latest_screen_date is None or created_date > latest_screen_date):
                    latest_screen_date = created_date

            screenings_planned_for_contact = 1
            screening_reason = "Daily full screening after sanctions list change"

            account_has_targeted_contacts = True
            contacts_targeted += 1
            planned_contact_ids.append(person_contact_id)
            total_screenings_planned += screenings_planned_for_contact
            contacts_targeted_rows.append(
                {
                    "account_id": account_id,
                    "ibkr_number": ibkr_number,
                    "date_opened": date_opened,
                    "contact_id": person_contact_id,
                    "contact_name": display_name,
                    "roles": roles_str,
                    "latest_screening_date": latest_screen_date.isoformat()
                    if latest_screen_date
                    else None,
                    "screening_date": today.isoformat(),
                    "screening_reason": screening_reason,
                    "screenings_planned": screenings_planned_for_contact,
                }
            )

        if account_has_targeted_contacts:
            accounts_targeted += 1

        if has_any_missing_screenings and has_any_screenings:
            accounts_with_some_contacts_no_screenings += 1
        elif not has_any_screenings:
            accounts_with_no_screenings_at_all += 1
        else:
            accounts_with_some_screenings += 1

    today_screened_contact_ids = {
        screen.get("contact_id")
        for screen in contact_screen_rows
        if screen.get("contact_id")
        and parse_screen_created(screen.get("created")) == today
    }
    planned_unique_contact_ids = {contact_id for contact_id in planned_contact_ids if contact_id}

    if planned_unique_contact_ids and today_screened_contact_ids.issuperset(planned_unique_contact_ids):
        return _compact_screenings_result({
            "apply_screenings": apply_screenings,
            "screenings_skipped": True,
            "skip_reason": "All targeted contacts already have screenings for today",
            "sanctions_comparison": sanctions_comparison,
            "contacts_targeted": contacts_targeted,
            "contacts_with_no_screenings": contacts_with_no_screenings,
            "accounts_with_some_contacts_no_screenings": accounts_with_some_contacts_no_screenings,
            "contacts_targeted_rows": contacts_targeted_rows,
            "accounts_targeted": accounts_targeted,
            "accounts_with_no_screenings_at_all": accounts_with_no_screenings_at_all,
            "accounts_with_some_screenings": accounts_with_some_screenings,
            "total_screenings_planned": total_screenings_planned,
            "screenings_executed": 0,
            "screening_errors": [],
        })

    if apply_screenings:
        for contact_id in planned_contact_ids:
            result = create_contact_screening_from_contact_id(
                contact_id=contact_id,
                created=created_value_today,
            )
            if isinstance(result, dict) and result.get("error"):
                screening_errors.append(f"{contact_id}: {result.get('error')}")
            else:
                screenings_executed += 1

    result = {
        "apply_screenings": apply_screenings,
        "screenings_skipped": False,
        "sanctions_comparison": sanctions_comparison,
        "contacts_targeted": contacts_targeted,
        "contacts_with_no_screenings": contacts_with_no_screenings,
        "accounts_with_some_contacts_no_screenings": accounts_with_some_contacts_no_screenings,
        "contacts_targeted_rows": contacts_targeted_rows,
        "accounts_targeted": accounts_targeted,
        "accounts_with_no_screenings_at_all": accounts_with_no_screenings_at_all,
        "accounts_with_some_screenings": accounts_with_some_screenings,
        "total_screenings_planned": total_screenings_planned,
        "screenings_executed": screenings_executed,
        "screening_errors": screening_errors,
    }
    return _compact_screenings_result(result)
