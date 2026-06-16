from datetime import date, datetime
import re
import unicodedata
from src.components.clients.accounts import read_accounts
from src.components.clients.contacts import (
    build_contact_screening_payload,
    create_contact_screenings_batch,
)
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


def _sanctions_overview(sanctions_comparison: dict | None) -> dict:
    if not isinstance(sanctions_comparison, dict):
        return {"available": False, "error": "invalid_sanctions_comparison"}

    overview = {
        "available": bool(sanctions_comparison.get("all_available")),
        "all_same": bool(sanctions_comparison.get("all_same")),
        "has_error": bool(sanctions_comparison.get("error")),
        "lists": {},
    }

    for key in ("ofac", "uk", "un"):
        comparison = sanctions_comparison.get(key) or {}
        change_summary = comparison.get("change_summary") or {}
        overview["lists"][key] = {
            "both_available": bool(comparison.get("both_available")),
            "is_same": bool(comparison.get("is_same")),
            "missing_today": bool(comparison.get("missing_today")),
            "missing_yesterday": bool(comparison.get("missing_yesterday")),
            "has_error": bool(comparison.get("error")),
            "added_count": int(change_summary.get("added_count", 0) or 0),
            "removed_count": int(change_summary.get("removed_count", 0) or 0),
        }

    return overview


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
        sanctions_overview = _sanctions_overview(sanctions_comparison)
        return _compact_screenings_result({
            "apply_screenings": apply_screenings,
            "screenings_skipped": True,
            "skip_reason": "OFAC, UK, and UN sanctions lists unchanged vs yesterday",
            "sanctions_comparison": sanctions_overview,
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
    planned_contact_rows = []
    sanctions_overview = _sanctions_overview(sanctions_comparison)

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

            account_has_targeted_contacts = True
            contacts_targeted += 1
            planned_contact_rows.append({
                "contact_id": person_contact_id,
                "contact": contact,
                "account_row": account,
                "ibkr_detail": detail if isinstance(detail, dict) else None,
                "account_contact_link": link,
            })

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
    planned_contacts_by_id = {}
    for row in planned_contact_rows:
        contact_id = row.get("contact_id")
        if contact_id and contact_id not in planned_contacts_by_id:
            planned_contacts_by_id[contact_id] = row
    planned_unique_contact_ids = set(planned_contacts_by_id.keys())
    total_screenings_planned = len(planned_unique_contact_ids)

    if planned_unique_contact_ids and today_screened_contact_ids.issuperset(planned_unique_contact_ids):
        return _compact_screenings_result({
            "apply_screenings": apply_screenings,
            "screenings_skipped": True,
            "skip_reason": "All targeted contacts already have screenings for today",
            "sanctions_comparison": sanctions_overview,
            "contacts_targeted": contacts_targeted,
            "contacts_with_no_screenings": contacts_with_no_screenings,
            "accounts_with_some_contacts_no_screenings": accounts_with_some_contacts_no_screenings,
            "contacts_targeted_rows": [],
            "accounts_targeted": accounts_targeted,
            "accounts_with_no_screenings_at_all": accounts_with_no_screenings_at_all,
            "accounts_with_some_screenings": accounts_with_some_screenings,
            "total_screenings_planned": total_screenings_planned,
            "screenings_executed": 0,
            "screening_errors": [],
        })

    if apply_screenings:
        screening_payloads = []
        for contact_id, row in planned_contacts_by_id.items():
            try:
                screening_payloads.append(build_contact_screening_payload(
                    contact=row.get("contact") or {},
                    account_row=row.get("account_row") or {},
                    ibkr_detail=row.get("ibkr_detail"),
                    account_contact_link=row.get("account_contact_link") or {},
                    created=created_value_today,
                ))
            except Exception as e:
                screening_errors.append(f"{contact_id}: {str(e)}")

        if screening_payloads:
            batch_result = create_contact_screenings_batch(screening_payloads)
            if isinstance(batch_result, dict) and batch_result.get("error"):
                screening_errors.append(batch_result.get("error"))
            else:
                screenings_executed = int((batch_result or {}).get("inserted", len(screening_payloads)))

    result = {
        "apply_screenings": apply_screenings,
        "screenings_skipped": False,
        "sanctions_comparison": sanctions_overview,
        "contacts_targeted": contacts_targeted,
        "contacts_with_no_screenings": contacts_with_no_screenings,
        "accounts_with_some_contacts_no_screenings": accounts_with_some_contacts_no_screenings,
        "contacts_targeted_rows": [],
        "accounts_targeted": accounts_targeted,
        "accounts_with_no_screenings_at_all": accounts_with_no_screenings_at_all,
        "accounts_with_some_screenings": accounts_with_some_screenings,
        "total_screenings_planned": total_screenings_planned,
        "screenings_executed": screenings_executed,
        "screening_errors": screening_errors,
    }
    return _compact_screenings_result(result)
