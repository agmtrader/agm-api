from datetime import date, datetime, timedelta
import re
import unicodedata

from src.components.entities.accounts import read_accounts, screen_person
from src.components.tools.reporting import get_ibkr_details
from src.utils.connectors.supabase import db

EXCLUDED_ASSOCIATIONS = {"trusted contact"}
SCREENING_CYCLE_GRACE_DAYS = 3
APPLY_SCREENINGS = True

screens = db.read("account_screening", query={})
accounts = read_accounts({})
details = get_ibkr_details()

# CHECK IF PEOPLE NEED SCREENING PENDING
def normalize_name(name: str) -> str:
    if not name:
        return ""
    ascii_name = (
        unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    )
    normalized = re.sub(r"[^a-z0-9 ]+", " ", ascii_name.lower())
    return " ".join(normalized.split())


def parse_open_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

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


def anniversary_for_year(open_date: date, year: int) -> date:
    try:
        return open_date.replace(year=year)
    except ValueError:
        return open_date.replace(year=year, day=28)


def cycle_due_start(start_date: date, today: date) -> date:
    current_anniversary = anniversary_for_year(start_date, today.year)
    cycle_year = today.year if today >= current_anniversary else today.year - 1
    if cycle_year < start_date.year:
        cycle_year = start_date.year
    return anniversary_for_year(start_date, cycle_year)


def cycle_year_for_date(start_date: date, target_date: date) -> int:
    year_candidate = target_date.year
    candidate_due_start = anniversary_for_year(start_date, year_candidate)
    cycle_year = year_candidate if target_date >= candidate_due_start else year_candidate - 1
    return max(cycle_year, start_date.year - 1)


def screenings_needed_count(
    start_date: date,
    latest_screen_date: date | None,
    today: date,
) -> int:
    due_start = cycle_due_start(start_date, today)
    if today < due_start:
        return 0

    current_cycle_year = cycle_year_for_date(start_date, today)
    if latest_screen_date is None:
        latest_cycle_year = start_date.year - 1
    else:
        latest_cycle_year = cycle_year_for_date(start_date, latest_screen_date)

    return max(0, current_cycle_year - latest_cycle_year)


def is_excluded_person(roles: list[str]) -> bool:
    normalized_roles = {normalize_name(role) for role in roles if role}
    return bool(normalized_roles.intersection(EXCLUDED_ASSOCIATIONS))


def screening_status(
    start_date: date,
    person_screens: list[dict],
    today: date,
) -> tuple[bool, str, date | None]:
    due_start = cycle_due_start(start_date, today)

    if not person_screens:
        if today < due_start:
            return False, f"Not due yet (cycle starts {due_start.isoformat()})", None
        return True, "Never screened", None

    latest_screen_date = None
    for screen in person_screens:
        created_date = parse_screen_created(screen.get("created"))
        if created_date and (latest_screen_date is None or created_date > latest_screen_date):
            latest_screen_date = created_date

    if latest_screen_date is None:
        return True, "Screening exists but dates are invalid", None

    if latest_screen_date + timedelta(days=SCREENING_CYCLE_GRACE_DAYS) < due_start:
        return (
            True,
            f"Latest screening ({latest_screen_date.isoformat()}) is before due cycle start ({due_start.isoformat()})",
            latest_screen_date,
        )

    return False, "Up to date", latest_screen_date


def missing_cycle_years(
    start_date: date,
    latest_screen_date: date | None,
    today: date,
) -> list[int]:
    needed = screenings_needed_count(
        start_date=start_date,
        latest_screen_date=latest_screen_date,
        today=today,
    )
    if needed <= 0:
        return []

    current_cycle_year = cycle_year_for_date(start_date, today)
    first_missing_cycle_year = current_cycle_year - needed + 1
    return list(range(first_missing_cycle_year, current_cycle_year + 1))


screens_by_account = {}
for screen in screens:
    account_id = screen.get("account_id")
    screens_by_account.setdefault(account_id, []).append(screen)

details_by_ibkr_number = {}
for detail in details:
    ibkr_number = detail.get("account", {}).get("accountId")
    if ibkr_number:
        details_by_ibkr_number[ibkr_number] = detail

accounts_with_some_individuals_no_screenings = 0
accounts_with_no_screenings_at_all = 0
accounts_with_some_screenings = 0
individuals_with_no_screenings = 0
accounts_due_now = 0
total_screenings_to_be_done = 0
screenings_executed = 0
screening_errors = []
accounts_due_now_rows = []

today = date.today()

for account in accounts:
    ibkr_number = account.get("ibkr_account_number")
    account_id = account.get("id")
    detail = details_by_ibkr_number.get(ibkr_number, {})
    detail_account = detail.get("account", {})
    persons = detail.get("associatedPersons") or []

    date_opened = detail_account.get("dateOpened")
    date_started = detail_account.get("dateBegun")
    start_date = parse_open_date(date_started)
    if not start_date:
        continue

    account_screens = screens_by_account.get(account_id, [])
    has_any_screenings = False
    has_any_missing_screenings = False
    account_has_due_now = False

    for person in persons:
        roles = person.get("associations") or []
        if is_excluded_person(roles):
            continue

        first_name = person.get("firstName", None)
        middle_name = person.get("middleName", None)
        last_name = person.get("lastName", None)
        
        if first_name is None and middle_name is None and last_name is None:
            continue
        
        if first_name is not None:
            display_name = first_name + " "
        
        if middle_name is not None:
            display_name = display_name + middle_name + " "

        if last_name is not None:
            display_name = display_name + last_name

        candidates = person_name_candidates(person)
        person_screens = [
            screen
            for screen in account_screens
            if normalize_name(screen.get("holder_name", "")) in candidates
        ]

        roles_str = ", ".join(roles)

        print(f"\nAccount: {ibkr_number}")
        print(f"Individual: {display_name}")
        print(f"Roles: {roles_str}")
        print(f"Date Opened: {date_opened}")
        print(f"Date Started: {date_started}\n")

        if person_screens:
            has_any_screenings = True
            for person_screen in person_screens:
                print(f"Screening: {person_screen}")
        else:
            has_any_missing_screenings = True
            individuals_with_no_screenings += 1
            print("Screening: None")

        print("---------------------------------------------------")

        due_now, due_reason, latest_screen_date = screening_status(
            start_date=start_date,
            person_screens=person_screens,
            today=today,
        )

        due_start = cycle_due_start(start_date, today=today)
        screenings_needed = screenings_needed_count(
            start_date=start_date,
            latest_screen_date=latest_screen_date,
            today=today,
        )
        cycle_years_to_create = missing_cycle_years(
            start_date=start_date,
            latest_screen_date=latest_screen_date,
            today=today,
        )
        if not due_now:
            screenings_needed = 0
            cycle_years_to_create = []
        
        if cycle_years_to_create:
            print(f"Cycle years to screen: {cycle_years_to_create}")

        residence_country = (
            person.get("residenceCountry")
            or person.get("countryOfResidence")
            or detail_account.get("residenceCountry")
            or detail_account.get("countryOfResidence")
        )
        risk_score = None
        if latest_screen_date:
            sorted_person_screens = sorted(
                person_screens,
                key=lambda s: parse_screen_created(s.get("created")) or date.min,
                reverse=True,
            )
            if sorted_person_screens:
                risk_score = sorted_person_screens[0].get("risk_score")

        if APPLY_SCREENINGS and due_now and cycle_years_to_create:
            if display_name == "Unknown":
                screening_errors.append(
                    f"{ibkr_number}/{account_id}: missing holder_name for screening"
                )
            else:
                for cycle_year in cycle_years_to_create:
                    created_date = anniversary_for_year(start_date, cycle_year)
                    created_value = created_date.strftime("%Y%m%d000000")
                    
                    result = screen_person(
                        account_id=account_id,
                        holder_name=display_name,
                        residence_country=residence_country,
                        risk_score=risk_score,
                        created=created_value,
                    )
                    
                    if isinstance(result, dict) and result.get("error"):
                        screening_errors.append(
                            f"{ibkr_number}/{display_name}/{cycle_year}: {result.get('error')}"
                        )
                    else:
                        screenings_executed += 1
                    

        if due_now:
            account_has_due_now = True
            total_screenings_to_be_done += screenings_needed
            accounts_due_now_rows.append(
                {
                    "account_id": account_id,
                    "ibkr_number": ibkr_number,
                    "date_opened": date_opened,
                    "individual_name": display_name,
                    "roles": roles_str,
                    "latest_screening_date": latest_screen_date.isoformat()
                    if latest_screen_date
                    else None,
                    "due_cycle_start": due_start.isoformat(),
                    "due_reason": due_reason,
                    "screenings_needed_now": screenings_needed,
                    "cycle_years_to_screen": ",".join(
                        str(cycle_year) for cycle_year in cycle_years_to_create
                    ),
                }
            )

        print("\n")

    if account_has_due_now:
        accounts_due_now += 1

    if has_any_missing_screenings and has_any_screenings:
        accounts_with_some_individuals_no_screenings += 1
    elif not has_any_screenings:
        accounts_with_no_screenings_at_all += 1
    else:
        accounts_with_some_screenings += 1

print(f"# of accounts due with missing screenings: {accounts_due_now}")
print(f"# of individuals with no screenings: {individuals_with_no_screenings}")

print(f"# of screenings executed: {screenings_executed}")
print(f"# of screening errors: {len(screening_errors)}")

for error in screening_errors:
    print(f"Screening error: {error}")
