#!/usr/bin/env bash
# Validate the exact candidate image before changing service traffic.
set -euo pipefail

task_image=${1:?Usage: bash dev/deploy_api.sh IMAGE [COMMIT]}
task_commit=${2:-}
if [[ "$task_image" != *@* ]]; then
  # Read only the registry tag mapping. `images describe` also queries
  # Container Analysis metadata, which is unrelated to deployment identity.
  task_tag=latest
  if [[ "${task_image##*/}" == *:* ]]; then
    task_tag=${task_image##*:}
    task_image=${task_image%:*}
  fi
  task_digest=$(gcloud artifacts docker tags list "$task_image" \
    --project=agm-datalake --filter="tag.basename()=$task_tag" \
    --format='value(version.basename())')
  task_image="$task_image@$task_digest"
fi
if [[ ! "$task_image" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo 'Could not resolve an immutable candidate image digest' >&2
  exit 1
fi

task_directory=$(mktemp -d)
task_suffix=${task_directory##*/}
task_suffix=$(printf '%s' "$task_suffix" | tr 'ABCDEFGHIJKLMNOPQRSTUVWXYZ.' 'abcdefghijklmnopqrstuvwxyz-')
task_job="agm-api-schema-$task_suffix"
cleanup() {
  # Retain the execution outcome in deployment/Cloud Logging evidence.
  gcloud run jobs delete "$task_job" --project=agm-datalake \
    --region=us-central1 --quiet >/dev/null || echo "Could not clean up preflight job $task_job" >&2
  rmdir "$task_directory"
}
trap cleanup EXIT

gcloud run jobs deploy "$task_job" \
  --project=agm-datalake --region=us-central1 --image="$task_image" \
  --command=python --args=-m,dev.schema_preflight \
  --tasks=1 --parallelism=1 --max-retries=0 --task-timeout=300s \
  --cpu=1 --memory=2Gi \
  --service-account=agm-api@agm-datalake.iam.gserviceaccount.com \
  --set-env-vars=DEV_MODE=false \
  --vpc-connector=agm-qa-connector --vpc-egress=all-traffic --quiet
gcloud run jobs execute "$task_job" --project=agm-datalake \
  --region=us-central1 --wait --quiet

# Reached only after schema validation succeeds, using the same image digest.
task_labels=()
if [[ -n "$task_commit" ]]; then
  task_labels=(--labels="managed-by=github-actions,commit-sha=$task_commit")
fi
gcloud run services update agm-api \
  --project=agm-datalake --region=us-central1 --platform=managed \
  --concurrency=4 --min-instances=0 --max-instances=10 --timeout=300 \
  --cpu=1 --memory=2Gi --port=5000 \
  --service-account=agm-api@agm-datalake.iam.gserviceaccount.com \
  --set-env-vars=DEV_MODE=false \
  --vpc-connector=agm-qa-connector --vpc-egress=all-traffic \
  --cpu-boost --ingress=all --image="$task_image" \
  "${task_labels[@]}" --quiet
