# Application contact identity rollout

API #88 / Hub #68. Form-script refactor #73 and persisted background jobs are outside this change.

## Behavior
Hub saves the application and queues one authenticated `POST /contacts/reconcile_application` with only `account_id`. The API reads the saved pending application and atomically matches/creates all person/company contacts, stores private identity and creates or reuses their account links. The browser advances without awaiting this work until final submission. Documents waits for the current reconciliation and offers retry. Screening and file operations run outside the identity transaction.

`contact_identity` stores the birth date and `contact_identity_document` permits multiple typed, country-scoped identifiers. IDs remain text and retain leading zeroes. Source account and applicant-declared status are explicit; these records are not verification. Raw identity is absent from generic contact reads, matcher responses and form logs. Both tables enable RLS and revoke public/anon/authenticated privileges. The Supabase advisor returns the informational [RLS Enabled No Policy](https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy) notice for this intentional deny-by-default configuration; no permissive browser policy is added.

Known identity conflicts and ambiguous matches return HTTP 409 with `contact_identity_conflict`, `contact_identity_ambiguous` or `contact_identity_insufficient`; malformed input is 422, unknown account is 404, and database failures use a generic 503 without SQL parameter values. Names/emails alone do not prove a match. Older contacts require an existing holder-ID link or independently holder-ID-resolved saved application evidence. Unresolved older records need review; this change does not mass-enrich, merge or delete them. Company contacts never receive person birthdays/IDs. Entire contact/identity/link writes roll back on a holder failure.

## Deployment order
1. Apply the checked migration `supabase/migrations/20261021175026_contact_identity_matching.sql`. Applied through the connected Supabase plugin to AGM; migration history version matches the local file. Independent catalog reads verify indexes, RLS, denied browser privileges and empty tables immediately after application. No customer identity was backfilled.
2. Deploy the API candidate with both new table models and the new route through the normal schema-preflight gate. Old serving code keeps running, but the strict preflight for an older image will reject the new tables; use this updated candidate before the next gated refresh.
3. Verified the configured Hub service principal and appended only `contacts/reconcile_application`, preserving its existing scope list. Independent readback confirms that permission and existing `accounts/contact`. Verify the deployed credential resolves to this principal. The matcher requires `contacts/reconcile_application` (or its existing coarse contacts/all grant) plus existing `accounts/contact`. Do not grant permissions to similarly named scheduled-workflow users.
4. Deploy Hub only after the API route is available. The updated Hub requires the matcher and must not fall back to the old name/email-only writer.
5. Verify a hosted application with authorized data. No hosted form acceptance is claimed by local tests.

The two new tables were created empty; implementation and validation made no broker call or production application/contact mutation. Commit/push and CI status are tracked in API #88; a push alone does not establish deployment or hosted acceptance. No durable background worker is required.

## Validation
The API regression runs the real authentication, scopes, Flask route and SQL transaction against disposable PostgreSQL. Set `AGM_IDENTITY_TEST_DATABASE_URL` to a localhost `agm_identity_fixture` or CI `agm_upload_fixture`; each test uses and removes its own uniquely named schema. The test refuses other hosts/databases. CI includes the suite with its existing PostgreSQL service.

```sh
AGM_IDENTITY_TEST_DATABASE_URL=postgresql+psycopg2://postgres@127.0.0.1:55439/agm_identity_fixture ./venv/bin/python -m pytest tests/test_contact_identity.py tests/test_contact_read_filters.py tests/test_authz_contract.py tests/test_database_startup.py -q
```

Cases cover unique identity reuse with changed email, shared email/different people, country/type/leading-zero distinctions, birthdays/ID conflicts, multiple documents, legacy missing/ambiguous identity, company/person isolation, multi-holder rollback, concurrent requests, denied roles, private response fields and schema-model compatibility.

The local mobile browser check exercised actual Hub/Server Actions → real Flask → real temporary PostgreSQL at 390px. A delayed matcher stayed pending while Personal Info advanced; repeated Next clicks created one account/link; Documents showed the resolved holder. A substituted screening provider failure blocked final completion and a successful retry completed the flow. Reference providers/screens were fixtures; account creation and identity matching were real local routes and SQL. Google-font loading and form seeding were test-only changes in an isolated copy.
