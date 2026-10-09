# API container build cache

`cloudbuild.yaml` uses Docker Buildx to import/export a registry cache at
`us-central1-docker.pkg.dev/agm-datalake/cloud-run-source-deploy/agm-api:buildcache`.
It uses the existing Cloud Build identity and repository permissions.

Dependencies remain before application code in the Dockerfile. When
`requirements.txt` and preceding instructions are unchanged, BuildKit can reuse
`RUN pip install --no-cache-dir -r requirements.txt`. The pip flag controls pip's
local download cache; it does not disable Docker layer caching. Source changes
rebuild the subsequent application layers. Base image or dependency changes can
invalidate earlier layers. An absent cache is a normal first-build condition;
registry authorization and build/export failures remain visible in build logs.

Buildx pushes the candidate image directly. `dev/deploy_api.sh` still validates
that exact image digest with schema preflight before updating the service.
The build cache tag is never passed to the deployment script.

A short-lived registry token is passed through `/builder/home`, outside the
Docker build context, and removed after Docker login. Docker's credential config
also stays outside the application image and expires with the ephemeral worker.

To populate an independent cache for diagnosis, pass `_CACHE_TAG` in Cloud Build
substitutions along with `_IMAGE_TAG`. Use a new cache tag to force a cold-cache
comparison. Only trusted build identities should write the cache. Registry cache
storage is billable; retention/cleanup must preserve the active cache tag if reuse
is desired. Cache hits do not eliminate worker queue time, tool image pulls,
application image transfers, or schema-preflight/deployment time.

Validation should use two separate Cloud Build workers, with unchanged dependency
inputs and an application-file change on the second run. Retain the build IDs,
logs showing `CACHED` for dependency installation, and timings in issue #282.
