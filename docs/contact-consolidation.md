# Owner-approved contact consolidation

`contact_consolidation.py` is an offline planning and SQL-generation module. It
uses explicit same/different-contact decisions plus a fresh connected-Supabase
snapshot. It never discovers new merges, imports application credentials or
connects to the database. `render_sql(plan)` defaults to a rolled-back rehearsal;
`render_sql(plan, commit=True)` generates the separately executed commit script.

## Scope and survivor selection

Build connected groups from confirmed pairs, including the earlier Guillermo and
Manuel confirmations. Reject contradictions with any confirmed different-person
pair. Every snapshot contact must belong to those approved groups; missing or
additional contacts stop planning.

Retain the member with the most populated business fields (name, valid
person/company type, email, phone, meeting place). Break ties by existing reference
count, fuller name, then stable ID. Fill missing fields from approved members.
Preserve the selected populated email/phone and retain alternatives in private
evidence. Expand a name only from an approved member containing all its current
tokens. Normalize an invalid/missing type only when the group supplies a single
known person/company type. Reject mixed person/company groups and company account
links that still carry a person entity ID.

## Reference preservation

Catalog discovery must identify exactly the supported nine foreign-key tables:
`account_contact`, `advisor`, `contact_document`, `contact_screening`,
`document_review_email`, `document_review_responsible`, `investment_proposal`,
`risk_profile` and `user`. Inspect constraints, indexes and triggers. Also scan
other UUID columns and JSON fields for the approved contact IDs; investigate any
unmodeled references before proceeding.

Retain **every child row and its ID**, including multiple account/document links
that become similar after consolidation. Do not deduplicate relationships,
screenings, review history or proposals. Repoint only `contact_id`. Account entity
and external IDs, document metadata, screening results and all other fields stay
unchanged. Raw documents and storage files are not modified.

## Backup, transaction and verification

1. Export a private backup of all original contact rows, the complete account-link
   rows, and each affected child row's ID, original contact ID and fingerprint of
   every other field. The backup contains only what is required to restore changed
   fields; it does not export user password hashes or screening result payloads.
2. Save the deterministic survivor mapping, field alternatives, expected row
   counts and before/after manifests. SHA-256 fingerprints the complete plan;
   generating SQL from a changed plan is refused.
3. Rehearse the generated SQL against the real database with `ROLLBACK` and verify
   original contacts remain. Do not describe this as a read-only query: it executes
   transactional mutations which are rolled back.
4. Apply the same plan through connected Supabase only within the owner's explicit
   authorization. This run was authorized to keep the most complete records and
   preserve all references; no further survivor choice was inferred from a name
   similarity score.
5. Use serializable isolation, a transaction advisory lock, parent/child row locks,
   bounded lock/statement timeouts, exact contact-snapshot and dependency-manifest
   preconditions, and rejection of unexpected references or mutation triggers.
6. Update canonical contacts and all nine reference tables. Check exact repoint
   counts and expected child manifests before removing redundant contacts.
7. Delete only approved redundant contact rows. Recheck all child manifests, table
   counts, surviving contact fields, raw-document count and zero old references.
   Any failed assertion aborts the entire transaction, including deletions.
8. After commit, independently read and verify canonical fields, deleted IDs,
   reference manifests and population counts. Retain the result privately.

Manifests include row ID, mapped contact ID and MD5 of the unchanged JSONB fields.
MD5 here detects unintended data changes; the plan itself uses SHA-256. Recovery
would separately restore original contact rows first, restore each child row's
original contact ID by its retained row ID, restore survivor fields and verify
the original manifests in a reviewed transaction. No recovery is performed by
generating or running the detector.

## October 21, 2026 result

Confirmed decisions produced 27 groups / 55 original contacts. Actual rolled-back
rehearsal and committed execution both passed. Independent post-commit verification
confirmed 28 redundant rows removed and 880 contacts remaining. Every child row
was retained: account links 1,192; document links 3,120; screenings 139,993; advisors
80; review emails 369; review assignments 238; proposals 21; risk profiles 37; users
17. Raw documents remain 3,657. Only 2,081 child contact references were repointed.

Private original rows, plan, SQL, tool results and independent verification are in
ignored `tmp/contact-consolidation-20261021/`. Do not publish client data or these
artifacts to GitHub. The source module, tests and process updates remain local
until separately committed/released. Historical legacy scripts named in process
14 remain unavailable; this module supersedes their executable merge path.

Validation: `./venv/bin/python -m pytest tests/test_contact_consolidation.py -q`
plus actual database rehearsal and independent post-commit manifest verification.
