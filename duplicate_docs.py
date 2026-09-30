"""Report all documents sharing an existing SHA-1 checksum (read-only).

Run from agm-api: ./venv/bin/python duplicate_docs.py
Loads .env; uses SUPABASE_DB_URL or the API's Secret Manager credentials.
Exit codes: 0 = complete scan, 1 = error, 2 = missing checksums.
Duplicate findings are reported for manual review; no database flags are changed.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.pool import NullPool


SUMMARY_SQL = """
SELECT COUNT(*) AS total_documents,
       COUNT(*) - COUNT(NULLIF(TRIM(sha1_checksum), '')) AS missing_checksums
FROM public.document
"""

DUPLICATES_SQL = """
SELECT id, file_name, sha1_checksum
FROM (
    SELECT id, file_name, sha1_checksum,
           COUNT(*) OVER (PARTITION BY sha1_checksum) AS checksum_count
    FROM public.document
    WHERE NULLIF(TRIM(sha1_checksum), '') IS NOT NULL
) AS checked
WHERE checksum_count > 1
ORDER BY sha1_checksum, file_name, id
"""


def database_url():
    """Resolve existing credentials without initializing or creating DB tables."""
    if os.environ.get("SUPABASE_DB_URL"):
        return os.environ["SUPABASE_DB_URL"]

    from google.api_core.exceptions import NotFound
    from src.utils.managers.secret_manager import get_secret

    try:
        url = get_secret("SUPABASE_DB_URL")
    except NotFound:
        url = None
    if url:
        return url
    return URL.create(
        "postgresql",
        username=f"postgres.{get_secret('SUPABASE_USER')}",
        password=get_secret("SUPABASE_PASSWORD"),
        host="aws-0-us-west-1.pooler.supabase.com",
        port=6543,
        database="postgres",
    )


def read_report(connection):
    summary = dict(connection.execute(text(SUMMARY_SQL)).mappings().one())
    duplicates = [dict(row) for row in connection.execute(text(DUPLICATES_SQL)).mappings()]
    return summary, duplicates


def table_cell(value):
    # Preserve filenames while escaping characters that could break the table.
    return "".join(
        character if character.isprintable() and character not in "\\|"
        else (r"\x7c" if character == "|" else character.encode("unicode_escape").decode("ascii"))
        for character in str(value)
    )


def print_report(summary, duplicates):
    print(f"Documents scanned: {summary['total_documents']}")
    print(f"Documents without a checksum: {summary['missing_checksums']}")
    if duplicates:
        groups = len({row["sha1_checksum"] for row in duplicates})
        print(f"Flagged {len(duplicates)} documents in {groups} duplicate SHA-1 groups.\n")
        headers = ("id", "file_name", "sha1_checksum")
        rows = [[table_cell(row[key]) for key in headers] for row in duplicates]
        widths = [max(len(key), *(len(row[i]) for row in rows)) for i, key in enumerate(headers)]
        def line(row):
            return "| " + " | ".join(value.ljust(width) for value, width in zip(row, widths)) + " |"
        print(line(headers))
        print(line(["-" * width for width in widths]))
        for row in rows:
            print(line(row))
    else:
        print("No duplicate documents found among nonblank stored SHA-1 checksums.")
    if summary["missing_checksums"]:
        print("Verification incomplete: documents without checksums could not be compared.")


def main():
    load_dotenv(Path(__file__).resolve().parent / ".env", override=False)
    engine = None
    try:
        engine = create_engine(
            database_url(),
            poolclass=NullPool,
            isolation_level="REPEATABLE READ",
            connect_args={"sslmode": "require", "connect_timeout": 10, "gssencmode": "disable"},
        )
        with engine.connect() as connection, connection.begin():
            connection.execute(text("SET TRANSACTION READ ONLY"))
            summary, duplicates = read_report(connection)
        print_report(summary, duplicates)
        return 2 if summary["missing_checksums"] else 0
    except Exception as exc:
        print(f"duplicate_docs failed ({type(exc).__name__}): {exc}", file=sys.stderr)
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
