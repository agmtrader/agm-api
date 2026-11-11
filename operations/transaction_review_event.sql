-- Apply through the Supabase migration workflow before deploying the API.
CREATE SCHEMA agm_compliance;
REVOKE ALL ON SCHEMA agm_compliance FROM PUBLIC, anon, authenticated;
CREATE TABLE agm_compliance.transaction_review_event (
  id uuid PRIMARY KEY,
  account_id uuid NOT NULL REFERENCES public.account(id),
  transaction_id text NOT NULL CHECK (length(trim(transaction_id)) > 0),
  status text NOT NULL CHECK (status IN ('flagged', 'escalated', 'resolved')),
  event_type text NOT NULL CHECK (event_type IN ('review', 'notification')),
  parent_event_id uuid REFERENCES agm_compliance.transaction_review_event(id),
  notification_status text CHECK (notification_status IS NULL OR notification_status IN ('accepted', 'unknown')),
  note text NOT NULL CHECK (length(trim(note)) > 0),
  follow_up_date text,
  actor_id uuid NOT NULL REFERENCES public."user"(id),
  actor_email text NOT NULL,
  created_at text NOT NULL,
  transaction_snapshot jsonb NOT NULL
);
CREATE INDEX transaction_review_event_lookup ON agm_compliance.transaction_review_event (account_id, transaction_id, created_at DESC, id DESC);
ALTER TABLE agm_compliance.transaction_review_event ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON agm_compliance.transaction_review_event FROM PUBLIC, anon, authenticated;
