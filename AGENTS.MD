## Working Rules

- Be direct and honest. Do not sugarcoat results.
- If context is missing, ask before guessing.
- Prefer showing the exact payload/error over summarizing too early.

## Python Environment

- Use the project virtualenv, not system Python.
- Setup:
  - `python3 -m venv venv` (only if `venv/` does not exist)
  - `source venv/bin/activate`
  - `pip install -r requirements.txt`
- Run scripts with:
  - `./venv/bin/python sandbox.py`
  - or after activate: `python sandbox.py`

## Env Vars

- Always load `.env` before running local scripts that need credentials.
- Quick way:
  - `set -a; source .env; set +a`
- If secrets come from Google Secret Manager, `.env` alone may not be enough.

## Sandbox Script Workflow

- For `sandbox.py`, print payload samples and key counts first.
- Run `sandbox.py` before changing other scripts unless requested otherwise.
- Keep debug output focused:
  - First 5-10 rows for payload shape
  - Final summary counts for decision making
- When matching holder data:
  - Do not mix IBKR account numbers (`U...`) with internal account UUIDs.
  - Map IDs explicitly before matching `account_document` or `account_screening`.

## Debugging Checklist

- If module errors appear (`ModuleNotFoundError`), confirm `venv` + `pip install`.
- If DNS/secret errors appear, note that network/secret access is blocked and rerun with required permissions.
- Validate syntax before final handoff:
  - `python -m py_compile sandbox.py`

## Output Expectation

- End with plain counts and a short conclusion.
- If numbers are wrong, state why in one line and fix the mapping logic.
