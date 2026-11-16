#!/usr/bin/env bash
# Fail fast when the OSS credentials required by `.github/workflows/oss.yml` are missing.
#
# Why this exists (issue #25): a missing secret used to surface as an obscure provider
# error several minutes into a Gradle run -- "invalid credentials" or, worse, a
# SignatureDoesNotMatch raised deep inside the object-store SPI. That message points at
# the code, not at the configuration, so the failure was routinely misdiagnosed. Checking
# the five variables up front costs nothing and names the actual culprit.
#
# Usage (CI): each variable is passed through the step `env:` block from `secrets.*`.
# Usage (local):   OSS_ENDPOINT=... OSS_BUCKET=... ./scripts/check-oss-secrets.sh
#
# Secret values are never printed -- only variable names. GitHub masks them anyway, but a
# script that cannot leak is better than one that relies on masking.
set -euo pipefail

required=(
  OSS_ENDPOINT
  OSS_BUCKET
  OSS_REGION
  OSS_ACCESS_KEY_ID
  OSS_ACCESS_KEY_SECRET
)

missing=()
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    missing+=("$name")
  fi
done

if (( ${#missing[@]} > 0 )); then
  echo "::error::missing repository secret(s): ${missing[*]}"
  cat >&2 <<'EOF'

These are configured under Settings -> Secrets and variables -> Actions.

They must come from a RAM sub-account whose policy is scoped to the CI bucket only
(`yangbase-ci*`), never from the root account. The suite creates and drops buckets and
wipes them between test classes, so a root AccessKey here is a blast radius problem,
not a convenience.

See docs/en/ci.md ("OSS conformance") for the exact policy document.
EOF
  exit 1
fi

echo "ok: all ${#required[@]} OSS secrets are present (values not printed)"
