-- Apply through the Supabase migration workflow before deploying the API.
CREATE TABLE public.sls_outreach_email (
  id uuid PRIMARY KEY,
  created text NOT NULL,
  updated text NOT NULL,
  fingerprint text NOT NULL,
  account_id uuid NOT NULL REFERENCES public.account(id),
  contact_id uuid NOT NULL REFERENCES public.contact(id),
  actor_id uuid NOT NULL REFERENCES public."user"(id),
  recipient_email text NOT NULL,
  purpose text NOT NULL CHECK (purpose IN ('tasks', 'sls', 'both')),
  language text NOT NULL CHECK (language IN ('es', 'en')),
  subject text NOT NULL,
  message text NOT NULL,
  tasks jsonb NOT NULL CHECK (jsonb_typeof(tasks) = 'array'),
  status text NOT NULL CHECK (status IN ('pending', 'sent', 'unknown')),
  provider_message_id text,
  sent_at text,
  CHECK ((status = 'sent') = (provider_message_id IS NOT NULL AND sent_at IS NOT NULL))
);
CREATE INDEX sls_outreach_email_account_history ON public.sls_outreach_email (account_id, created DESC, id DESC);
CREATE UNIQUE INDEX sls_outreach_email_unresolved_notice ON public.sls_outreach_email (fingerprint) WHERE status IN ('pending', 'unknown');
ALTER TABLE public.sls_outreach_email ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.sls_outreach_email FROM PUBLIC, anon, authenticated;
