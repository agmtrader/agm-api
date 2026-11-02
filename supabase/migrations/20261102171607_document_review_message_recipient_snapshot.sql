-- Retain the recipient name/phone snapshot without blocking normal contact deletion.
ALTER TABLE public.document_review_message
    DROP CONSTRAINT document_review_message_recipient_contact_id_fkey,
    ALTER COLUMN recipient_contact_id DROP NOT NULL,
    ADD CONSTRAINT document_review_message_recipient_contact_id_fkey
        FOREIGN KEY (recipient_contact_id) REFERENCES public.contact(id) ON DELETE SET NULL;
