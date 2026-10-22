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

Catalog discovery must identify exactly the supported eleven foreign-key tables:
`account_contact`, `advisor`, `contact_document`, `contact_screening`,
`document_review_email`, `document_review_responsible`, `investment_proposal`,
`risk_profile`, `user`, `contact_identity` and `contact_identity_document`.
Inspect constraints, indexes and triggers. Also scan
other UUID columns and JSON fields for the approved contact IDs; investigate any
unmodeled references before proceeding.

Retain **every child row and its ID**, including multiple account/document links
that become similar after consolidation. Do not deduplicate relationships,
screenings, review history or proposals. Repoint only `contact_id`. Account entity
and external IDs, document metadata, screening results and all other fields stay
unchanged. Raw documents and storage files are not modified.

### Private identity evidence (October 22 update)

The planner now requires a schema-version-2 backup. Include explicit full-row
`backup['identity']['contact_identity']` and
`backup['identity']['contact_identity_document']` arrays, even when empty, along
with their dependency rows and global totals. Missing identity evidence is never
treated as empty. The birthday table has no separate `id`: use its `contact_id`
as the dependency row's `id`. Its manifest changes that key as well as the contact
reference when a row moves. Document row IDs stay unchanged.

`identity_snapshot_sql(approved_contact_ids)` generates a read-only adjunct query.
Run it through connected Supabase in the **same repeatable-read/read-only
transaction** as the other backup reads, with the same explicit approved IDs.
It returns:

- `identity`: assign to `backup['identity']`.
- `identity_references`: merge into `backup['references']['references']`.
- `identity_totals`: merge into `backup['references']['reference_totals']`.

The query exports private birthdays/IDs and provenance for review. Keep that
snapshot and the resulting plan/SQL in a private ignored folder; do not publish
them to GitHub. Source-account, verification, creation/update fields and every
document identifier remain byte-for-byte unchanged in SQL; only `contact_id`
moves. Leading zeroes and document type/country distinctions are retained.

Conflicting birthdays or competing government IDs within the same type/country
on different contacts stop planning. The one-birthday-row-per-contact primary key
also means two birthday records cannot simply be repointed to one survivor.
**Even matching birthday records stop for separately reviewed provenance
resolution**; this script never deletes a birthday row or silently selects one
source account. Multiple document records already held by one contact are
preserved. Different document types/countries can move together without being
collapsed. Company groups cannot inherit personal identity.

An old nine-table backup or schema-version-1 plan must be refreshed, not patched
by hand. The new SQL locks out the application matcher with its existing identity
advisory key and locks participating contact/reference tables for the bounded
transaction. These locks prevent concurrent dependency inserts or edits while
the reviewed snapshot/manifests are checked and applied. Active matching, drift,
new foreign keys or mutation triggers abort the entire merge.

## Backup, transaction and verification

1. Export a private backup of all original contact rows, the complete account-link
   rows, and each affected child row's ID, original contact ID and fingerprint of
   every other field, plus full private identity rows needed to review conflict
   policy. It does not export user password hashes or screening result payloads.
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
6. Update canonical contacts and all eleven reference tables. Check exact repoint
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

Historical validation used `tests/test_contact_consolidation.py`, plus actual
database rehearsal and independent post-commit manifest verification. That test
file is currently deleted in an unrelated checkout cleanup; this update does not
restore it or any deleted migrations.

## October 22, 2026 tooling validation

Six prior planner checks were rerun from a temporary copy against the new
explicit-empty identity contract. Sixteen additional checks executed the actual
snapshot query and generated rollback/commit SQL in a newly created disposable
localhost PostgreSQL database using synthetic contacts and the identity migration
from Git. They verified identity/provenance/reference preservation, leading zeroes,
conflict and duplicate-birthday refusal, omitted evidence, legacy/tampered plans,
live row drift, late identity inserts, extra foreign keys, trigger rejection and
the shared matcher lock. The disposable database is removed after verification.
No production merge, production rehearsal or new identity correction was run.
