# Read-only duplicate-contact review

`contact_duplicates.py` is an offline detector for API #111. It does not connect
to the database, initialize the application, call IBKR, choose survivors or merge
anything. It compares every contact pair and writes a new private JSON report.
There is no apply mode. The report is a candidate list, not a cleanup plan.

## Inputs and run

Export the following query through the connected Supabase tool against the
verified AGM project. Save the `snapshot` JSON object, not the connector envelope,
to a private file under the ignored `tmp/` directory. The transaction ensures the
three tables come from the same database snapshot. No credentials or application
JSON are exported.

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT json_build_object(
  'captured_at', current_timestamp,
  'contacts', (SELECT coalesce(json_agg(t), '[]'::json) FROM
    (SELECT id, type, name, email, phone FROM public.contact ORDER BY id) t),
  'accounts', (SELECT coalesce(json_agg(t), '[]'::json) FROM
    (SELECT id, ibkr_account_number, master_account FROM public.account ORDER BY id) t),
  'account_contacts', (SELECT coalesce(json_agg(t), '[]'::json) FROM
    (SELECT id, account_id, contact_id, entity_id, external_id, role
     FROM public.account_contact ORDER BY id) t)
) AS snapshot;
COMMIT;
```

Supply an existing IBKR account-details list in the format consumed by
`get_ibkr_details`: `account.accountId`, `account.masterAccountId`,
`associatedPersons` and optional `_agm_refresh` provenance. Obtaining a newer
backup is a separate read; this script never refreshes it or invokes an ETL.

```sh
python3 -I contact_duplicates.py \
  --snapshot tmp/contact-dedup-20261020/database.json \
  --ibkr-details dev/ibkr_details_runs/integrated_20261014T205914Z/account_details.json \
  --output tmp/contact-dedup-20261020/review.json
```

Output must be a new filename. The script creates it with mode `0600`, refuses
overwrite, stores both input SHA-256 fingerprints and prints only aggregate
counts. The JSON contains personal data and internal IDs; keep it in an ignored,
private directory. Inputs are not modified. Exit 0 means report generation
succeeded, not that the population is duplicate-free or coverage is complete.
Exit 1 means an input/output failure; the report must not be treated as complete.

## Evidence and controls

- The real `contact` schema has no birthday or government-ID columns. Identity
  signals come from IBKR people, resolved through `account_contact` and `account`.
- Company contacts remain linked to their accounts but are excluded from IBKR
  associated-person resolution. A company link with a person entity ID is a
  data-quality finding, not permission to inherit the representative's identity.
- Join IBKR details by BOTH the stored broker account and master. Resolve a person
  by linked entity/external ID; when both are populated, both must match the same
  person. Account-level email and shared account membership are not person IDs.
- As of October 21, candidate discovery also compares resolved `entityId` values
  across accounts, following the owner's person-identity rule. Shared entity IDs
  are included explicitly in each pair. External codes remain account-scoped.
  Company/person type conflicts remain blocked: a company contact linked to a
  representative's person ID is a relationship-review case, not a proven duplicate.
- A unique name fallback is exposed as a lead but never supplies birthday or ID
  corroboration. This prevents matching a name and then using its inferred ID to
  prove that same match. Trusted contacts/other associated roles remain visible
  in source evidence, without treating their account role as ownership proof.
- Compare government IDs within their document type and country, retaining
  leading zeroes. Normalize accents, spacing and name-token order. A candidate
  needs matching person/ID evidence or a credible multi-token name match.
  Abbreviated names require the same first name and the shorter token set to be
  contained in the longer one. Other fuzzy names require the same first name,
  at least two shared tokens and similarity of at least 0.90. Shared surnames
  with different first names do not qualify. Phone, email, birthday and account
  membership alone never generate a pair; they are supporting evidence only.
  Different numeric company registration identifiers prevent fuzzy company-name
  matching. Shared IBKR/government identity under unrelated names is retained in
  `identity_link_anomalies`, separately from duplicate-review `candidates`.
- Strong candidates require person/ID agreement, name support, matching known
  contact types, no conflicts and per-person source timestamps within 48 hours.
  Even strong candidates require manual review. Old, stale or undated backups
  never qualify as current corroboration merely because their stored status says
  `fresh`. No supplied snapshot proves current broker acceptance.
- Conflicting birthdays, government-ID values, account-scoped person IDs, contact
  types or unresolved link identifiers are explicit. Multiple document numbers
  can reflect renewal; the detector requires review rather than guessing.
- Missing master IDs, missing/duplicate details, unresolved people and unlinked
  contacts are counted as coverage gaps. No identity is invented for them.
- Pairs are independent leads; do not turn connected pairs into a merge cluster.
  Account/document/screening/user/advisor dependency preservation would require a
  separate reviewed merge implementation and authorization.

## Verified October 20, 2026

The first real offline run used a fresh read-only database export (908 contacts,
836 accounts, 1,192 links) and the existing October 14 backup (636 account-detail
entries). Nineteen backup entries lack a complete account/master key. No broker
calls were made. See the private generated report for candidate and coverage
counts; they are point-in-time, not confirmed duplicates or current IBKR coverage.

The focused regression suite uses sanitized observed shapes and covers joint
holders with similar names, both-ID disagreement, master mismatch, country-scoped
document IDs, circular name fallback, old backups, missing/duplicate inputs,
private output, unchanged inputs and overwrite refusal:

```sh
./venv/bin/python -m pytest tests/test_contact_duplicates.py -q
```

## Legacy manual mismatch

Compliance process 14 names `dev/contact_dedup_dry_run.py`,
`dev/contact_dedup_apply.py` and `contact_dedup_manual_review.py`; none exists in
this checkout. Those historical steps are not an available executable workflow.
The new detector restores only read-only review, not the old apply/manual merge
workflow. Do not execute or recreate a merge from the old prose alone.
