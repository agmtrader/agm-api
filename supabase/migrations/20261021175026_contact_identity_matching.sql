-- Additive private identity storage. No existing contacts are changed/backfilled.
create table public.contact_identity (
    contact_id uuid primary key references public.contact(id) on delete cascade on update cascade,
    date_of_birth date not null,
    source_account_id uuid references public.account(id) on delete set null on update cascade,
    source text not null check (source = 'application'),
    verification_status text not null check (verification_status = 'applicant_declared'),
    created text not null,
    updated text not null
);
create index contact_identity_source_account_idx on public.contact_identity(source_account_id);
create table public.contact_identity_document (
    id uuid primary key,
    contact_id uuid not null references public.contact(id) on delete cascade on update cascade,
    document_type text not null check (document_type in ('passport','nationalCard','driversLicense')),
    issuing_country text not null check (issuing_country ~ '^[A-Z]{3}$'),
    identifier text not null check (identifier ~ '^[A-Z0-9]{1,128}$'),
    source_account_id uuid references public.account(id) on delete set null on update cascade,
    source text not null check (source = 'application'),
    verification_status text not null check (verification_status = 'applicant_declared'),
    created text not null,
    updated text not null
);
create unique index contact_identity_document_scoped_id_idx
    on public.contact_identity_document(document_type, issuing_country, identifier);
create index contact_identity_document_contact_idx on public.contact_identity_document(contact_id);
create index contact_identity_document_source_account_idx on public.contact_identity_document(source_account_id);
alter table public.contact_identity enable row level security;
alter table public.contact_identity_document enable row level security;
-- Application uses its existing server-side SQL connection and scoped API.
-- Neither identity table is available through browser Data API roles.
revoke all on public.contact_identity, public.contact_identity_document from public, anon, authenticated;
