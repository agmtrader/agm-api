-- Manually confirmed WhatsApp sends; no customer/provider call is involved.
CREATE TABLE public.document_review_message (
    id uuid PRIMARY KEY,
    created text NOT NULL,
    updated text NOT NULL,
    account_id uuid NOT NULL REFERENCES public.account(id) ON DELETE CASCADE,
    contact_id uuid NOT NULL REFERENCES public.contact(id) ON DELETE CASCADE,
    recipient_contact_id uuid NOT NULL REFERENCES public.contact(id) ON DELETE RESTRICT,
    recipient_name text NOT NULL CHECK (length(btrim(recipient_name)) BETWEEN 1 AND 300),
    recipient_phone text NOT NULL CHECK (length(btrim(recipient_phone)) BETWEEN 1 AND 80),
    channel text NOT NULL CHECK (channel = 'whatsapp'),
    status text NOT NULL CHECK (status = 'sent_manual'),
    template text NOT NULL CHECK (template IN ('initial', 'follow_up')),
    holder_scenario text NOT NULL CHECK (holder_scenario IN ('own', 'secondary', 'company')),
    language text NOT NULL CHECK (language IN ('en', 'es')),
    missing_document_keys jsonb NOT NULL CHECK (
        jsonb_typeof(missing_document_keys) = 'array'
        AND jsonb_array_length(missing_document_keys) > 0
        AND missing_document_keys <@ '["poi","poe","poa","sow"]'::jsonb
        AND NOT (missing_document_keys ? 'poi' AND missing_document_keys ? 'poe')
    ),
    message_text text NOT NULL CHECK (length(btrim(message_text)) BETWEEN 1 AND 12000),
    sent_at text NOT NULL,
    recorded_by uuid REFERENCES public."user"(id) ON DELETE SET NULL,
    recorded_by_name text NOT NULL
);
CREATE INDEX document_review_message_account_contact_sent_idx
    ON public.document_review_message (account_id, contact_id, sent_at DESC);
CREATE INDEX document_review_message_recipient_idx ON public.document_review_message (recipient_contact_id);
CREATE INDEX document_review_message_recorded_by_idx ON public.document_review_message (recorded_by);
ALTER TABLE public.document_review_message ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.document_review_message FROM anon, authenticated;
COMMENT ON TABLE public.document_review_message IS
    'AGM API-only manual WhatsApp send evidence. Sent is reviewer-confirmed, not provider delivery/read evidence. Copying creates no record.';
