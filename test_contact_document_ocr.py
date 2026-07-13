import csv
from sqlalchemy import Table

from src.components.clients.document_processing import (
    TEXT_EXTRACTION_PROCESS_TYPE,
    process_document_text_extraction,
)
from src.utils.connectors.supabase import db

CATEGORY = "Proof of Identity"
TARGET_COUNT = 50
PREVIEW_LENGTH = 300
DETAIL_CSV_PATH = "contact_document_ocr_extractions.csv"
SUMMARY_CSV_PATH = "contact_document_ocr_summary.csv"


def preview_text(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "<empty>"
    return normalized[:PREVIEW_LENGTH]


def read_target_contact_documents() -> list[dict]:
    table = Table("contact_document", db.metadata, autoload_with=db.engine)

    @db.with_session(commit=False)
    def _read(session):
        query = (
            session.query(table)
            .filter(table.c.category == CATEGORY)
            .order_by(table.c.created.desc(), table.c.id.desc())
            .limit(TARGET_COUNT)
        )
        rows = query.all()
        return [row._asdict() for row in rows]

    return _read()


def read_processing_record(document_id: str) -> dict | None:
    processing_rows = db.read(
        table="document_processing",
        query={
            "document_id": document_id,
            "process_type": TEXT_EXTRACTION_PROCESS_TYPE,
        },
    ) or []
    return processing_rows[0] if processing_rows else None


def write_detail_csv(rows: list[dict]) -> None:
    fieldnames = [
        "contact_document_id",
        "document_id",
        "document_language",
        "type",
        "category",
        "status",
        "provider",
        "output_chars",
        "output_preview",
        "output_text",
        "error",
    ]
    with open(DETAIL_CSV_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary_csv(summary_rows: list[dict]) -> None:
    fieldnames = ["category", "requested", "selected", "completed", "failed"]
    with open(SUMMARY_CSV_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)


def main() -> None:
    contact_documents = read_target_contact_documents()

    print("\n=== batch selection ===")
    print(f"category: {CATEGORY}")
    print(f"requested: {TARGET_COUNT}")
    print(f"selected: {len(contact_documents)}")

    if not contact_documents:
        raise ValueError(f"No contact_documents found for category: {CATEGORY}")

    completed = 0
    failed = 0
    extraction_rows = []

    for index, contact_document in enumerate(contact_documents, start=1):
        contact_document_id = str(contact_document.get("id") or "").strip()
        document_id = str(contact_document.get("document_id") or "").strip()
        source_language = contact_document.get("document_language")

        print(f"\n=== document {index}/{len(contact_documents)} ===")
        print(f"contact_document_id: {contact_document_id}")
        print(f"document_id: {document_id}")
        print(f"document_language: {source_language}")
        print(f"type: {contact_document.get('type')}")
        print(f"category: {contact_document.get('category')}")

        if not document_id:
            failed += 1
            print("status: failed")
            print("error: contact_document has no document_id")
            extraction_rows.append({
                "contact_document_id": contact_document_id,
                "document_id": document_id,
                "document_language": source_language,
                "type": contact_document.get("type"),
                "category": contact_document.get("category"),
                "status": "failed",
                "provider": None,
                "output_chars": 0,
                "output_preview": "<empty>",
                "output_text": "",
                "error": "contact_document has no document_id",
            })
            continue

        result = process_document_text_extraction(
            document_id=document_id,
            source_language=source_language,
        )

        status = str((result or {}).get("status") or "unknown").strip()
        processing_record = read_processing_record(document_id)
        provider = (processing_record or {}).get("provider")
        output_text = (processing_record or {}).get("output_text")
        error = (processing_record or {}).get("error") or (result or {}).get("error")

        print(f"status: {status}")
        print(f"provider: {provider}")
        print(f"output_chars: {len((output_text or '').strip())}")
        print(f"output_preview: {preview_text(output_text)}")

        if error:
            print(f"error: {error}")

        extraction_rows.append({
            "contact_document_id": contact_document_id,
            "document_id": document_id,
            "document_language": source_language,
            "type": contact_document.get("type"),
            "category": contact_document.get("category"),
            "status": status,
            "provider": provider,
            "output_chars": len((output_text or "").strip()),
            "output_preview": preview_text(output_text),
            "output_text": (output_text or "").strip(),
            "error": error,
        })

        if status == "completed":
            completed += 1
        else:
            failed += 1

    write_detail_csv(extraction_rows)
    write_summary_csv([{
        "category": CATEGORY,
        "requested": TARGET_COUNT,
        "selected": len(contact_documents),
        "completed": completed,
        "failed": failed,
    }])

    print("\n=== batch summary ===")
    print(f"completed: {completed}")
    print(f"failed: {failed}")
    print(f"detail_csv: {DETAIL_CSV_PATH}")
    print(f"summary_csv: {SUMMARY_CSV_PATH}")


if __name__ == "__main__":
    main()
