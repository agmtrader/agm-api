ALTER TABLE public.risk_profile
  ADD COLUMN estimated_deposit numeric(18,2)
  CONSTRAINT risk_profile_estimated_deposit_positive
  CHECK (estimated_deposit > 0 AND estimated_deposit < 'Infinity'::numeric);

COMMENT ON COLUMN public.risk_profile.estimated_deposit IS
  'Applicant-declared estimated initial deposit in USD; null when not provided.';
