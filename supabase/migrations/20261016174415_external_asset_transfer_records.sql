-- Pending deployment. No broker calls or existing-record updates.
begin;
create table public.external_asset_transfer (
    id uuid primary key,
    account_id uuid not null references public.account(id),
    master_account text not null,
    ibkr_account_id text not null,
    client_instruction_id bigint generated always as identity (start with 1000000000000000) unique not null,
    submission_state text not null check (submission_state in ('SUBMITTING','SUBMITTED','UNKNOWN','REJECTED')),
    request_data jsonb not null,
    instruction_id text,
    instruction_set_id text,
    ib_reference_id text,
    instruction_status text,
    clearing_state text,
    last_error jsonb,
    message_center_ticket_reference text,
    ticket_created_at text,
    ticket_recorded_at text,
    ticket_url text,
    portal_transfer_reference text,
    receiving_broker_bmo_acceptance boolean,
    created text not null,
    updated text not null
);
create index external_asset_transfer_account_created_idx on public.external_asset_transfer(account_id, created desc);
create unique index external_asset_transfer_official_idx on public.external_asset_transfer(master_account, instruction_id) where instruction_id is not null;
alter table public.external_asset_transfer enable row level security;
-- Application uses its existing server-side Postgres connection and API authorization.
-- No browser Data API access or permissive policies.
revoke all on public.external_asset_transfer from public, anon, authenticated;
revoke all on sequence public.external_asset_transfer_client_instruction_id_seq from public, anon, authenticated;
commit;
