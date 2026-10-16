# External Treasury-bill transfer wiring

Implementation only, October 16, 2026. No live transfer was submitted or tested during this change. The QA sample remains evidence of initial processing, not settlement, a real Treasury-bill holding or valid production settlement instructions.

## Release order

1. Review and apply `supabase/migrations/20261016174415_external_asset_transfer_records.sql` to the intended AGM database through the approved Supabase migration workflow. It is **not applied** by this change. The migration creates an RLS-enabled server-only table and identity sequence; there is no public Data API policy. The application uses its existing server-side database connection and API scope enforcement.
2. Run schema preflight using the new API metadata. Deploy the API before the Dashboard. Old API deployments do not implement the new history/status/ticket contract.
3. Deploy Dashboard with its existing production transport (`DEV_MODE` unset/false -> `https://api.agmtechnology.com`). IBKR connector uses `https://api.ibkr.com` and the stored account’s supported master credentials; no QA accounts, receivers, BICs or IDs are seeded into application records.
4. Perform separately authorized application/transfer acceptance. Nothing in the source change authorizes a production transfer test.

## Application API

All routes require an authenticated operator and existing account/all scope or exact route scope. An accounts/read token alone is insufficient.

- `POST /accounts/ibkr/external_asset_transfer`: `{request_id: UUID, account_id: AGM account UUID, currency, trade_date, settle_date, pset_bic, positions:[{conid,quantity}], contra_broker_info:{accountType,brokerName,brokerAccountId,country,contactEmail,contactPhone}}`. Source IBKR account/master are resolved from the saved AGM account. Recheck selected BILL holdings/currency/available quantities before the single signed v2 OUT submission. Returned value is the durable transfer record, including submission_state and last_error. HTTP success is not proof of broker acceptance: read the record’s state.
- `GET /accounts/ibkr/external_asset_transfers?account_id=<AGM UUID>`: latest 100 saved records for that account; no broker request.
- `POST /accounts/ibkr/external_asset_transfer/status`: `{account_id,transfer_id}`. Query the broker by the saved official instructionId, falling back to the original clientInstructionId only when no official ID was captured. Enrich that record without issuing another transfer.
- `POST /accounts/ibkr/external_asset_transfer/ticket`: `{account_id,transfer_id,message_center_ticket_reference,ticket_created_at?,ticket_url?,portal_transfer_reference?,receiving_broker_bmo_acceptance?}`. Ticket reference is text; letters and leading zeroes survive. Optional actual creation time is distinct from capture time. This saves metadata; it does not create an IBKR ticket.

## Stored references and recovery

`id` is the browser-generated idempotency UUID, not an IBKR instruction. `client_instruction_id` is allocated by a database sequence. `instruction_id`, `instruction_set_id`, `ib_reference_id` and the separately assigned Message Center ticket reference retain distinct fields and labels. API identities are serialized as strings to avoid browser precision loss.

Commit SUBMITTING and request details before the broker call. Atomic insert/UUID uniqueness prevents concurrent requests with the same UUID from posting twice. Existing UUIDs return their saved records. There is no automatic POST retry. A network failure, malformed response or uncertain upstream error leaves UNKNOWN/SUBMITTING for investigation. Reload saved records and refresh status; never create another request solely because the result is late or a recovery lookup initially returns not found. A crash before the broker call can also leave SUBMITTING: that conservative ambiguity requires operator reconciliation, not automatic resubmission.

The request snapshot retains account/direction, position conids, quantities, descriptions and available CUSIP/ISIN/maturity, currency, receiver/contact and settlement details. Quantities retain IBKR position units, not inferred face value. Missing security identifiers/maturity remain explicitly unavailable in drafts. Status updates retain official references, returned clearing state and last error. PROCESSED alone never sets settlement_verified; explicit clearingState SETTLED does.

Dashboard loads positions only when the transfer modal opens, refreshing them on each opening. Account Banking has no saved-transfer history section and makes no history reads on mount, reopen, submission or status/ticket updates. The modal uses the current transfer record returned by each action. Dashboard uses a review checkbox and an explicit Submit transfer to IBKR button; a positive BILL holding must be selected. Request UUID is retained after a missing HTTP response. Another submission is blocked while the current request has an unknown/in-flight outcome in the mounted view; historical records no longer gate the form. Durable UUID deduplication and persistence before broker submission remain server-side. After creating a ticket manually in Message Center, save its exact assigned reference; only then does the follow-up email become available with instruction ID + ticket reference. No email address is guessed and no email/ticket is sent automatically.

Mike Diaz’s separate account-specific guidance remains an operational check: API/portal equivalence, T+1 coordination and the literal 10 EST clearing cutoff, verified ProServe recipient, and receiving-broker acceptance from BMO. The application records the latter as operator evidence; it does not infer it from broker processing status.

## Validation boundary

The initial integration received static source checks. The subsequent focused UI follow-ups exercised the real React/Radix dialog with synthetic API reads, including position eligibility and modal-only position loading with no saved-history reads; no transfer mutations occurred. No broker submission, live application route execution, database migration or deployed acceptance is included. Before operational use, exercise auth/route scopes, PostgreSQL identity allocation and simultaneous duplicate UUIDs, BILL/currency/quantity/date/BIC guards, signed payload and credential selection, 202/rejection/timeout recovery, persisted records through the API after reload (the Dashboard history section is removed), status identity checks, distinct ticket references (including leading zeroes), and follow-up gating. Use synthetic dependencies first; a real transfer requires separate authorization.

## Verified references

- [IBKR signed v2 external transfer](https://www.interactivebrokers.com/docs/web-api/api-reference/account-management/account-management-banking/create-external-asset-transfers-2)
- [Status by instructionId](https://www.interactivebrokers.com/docs/web-api/api-reference/account-management/account-management-banking/get-instructions)
- [Broker Portal ticket creation](https://www.ibkrguides.com/brokerportal/messagecenter/creatingaticket.htm)
