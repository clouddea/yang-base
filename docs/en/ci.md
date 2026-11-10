---
---

# Continuous Integration

[English](ci.md) | [简体中文](../zh/ci.md)

Five workflows live in `.github/workflows/`. Four of them need no credentials at all; the fifth talks to a real object storage account and therefore needs a specifically scoped one.

| Workflow | Trigger | Credentials | What it proves |
| --- | --- | --- | --- |
| `ci.yml` | push, pull request | none | build, unit + integration tests, coverage gate |
| `bench.yml` | pull request, manual | none | latency/request-count benchmarks against the in-repo backend |
| `docker.yml` | manual, release | Docker Hub token | the published image starts, answers a query, and shuts down |
| `release.yml` | tag `v*` | Docker Hub token | preflight, artifacts, GitHub Release body |
| `oss.yml` | nightly, manual | **Aliyun OSS AccessKey** | the same conformance suite, on real cloud storage |

`ci.yml` runs on `pull_request`, which includes pull requests from forks, so it must never see a cloud credential. That is why the real-backend checks live in `oss.yml` and never run on `pull_request` — a boundary that is structural rather than a matter of care.

## OSS conformance (`oss.yml`)

### Why it is nightly and not per-commit

Every test class wipes its bucket in `setUp`. A round trip from a GitHub runner to a bucket in Hangzhou costs 100–300 ms, and a wipe is LIST plus one DELETE (which itself costs a HEAD) per object. Running this on every push would mean a real bill, real flakiness from network jitter, and a red build that tells you nothing about your diff. Once a night is enough to answer the only question this workflow asks: *does the code still agree with the real cloud?*

### The five secrets

| Secret | Meaning |
| --- | --- |
| `OSS_ENDPOINT` | e.g. `oss-cn-hangzhou.aliyuncs.com` |
| `OSS_BUCKET` | prefix of the CI buckets; jobs append their own suffix |
| `OSS_REGION` | e.g. `cn-hangzhou` |
| `OSS_ACCESS_KEY_ID` | AccessKey of the **CI RAM sub-account** |
| `OSS_ACCESS_KEY_SECRET` | its secret |

Jobs never share a bucket: the conformance job uses `${OSS_BUCKET}-conformance`, and each integration shard uses `${OSS_BUCKET}-<group>`. Sharing one bucket is not "slower" — each class wipes the bucket, so two runners sharing one would delete each other's live data while the tests run, and the resulting `ObjectNotFoundException` looks like a conformance bug rather than a scheduling bug. Bucket names must contain `-ci`; a guard step refuses to continue otherwise, which is a crude brake but it does stop a mis-set secret from wiping a bucket that holds data.

### Why a sub-account and not the root AccessKey

The suite creates and deletes buckets, and blindly wipes them. An AccessKey from the account root turns a single mistyped bucket name into a destroyed account. Issue #25 tracks this. The sub-account only needs:

```json
{
  "Version": "1",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["oss:ListBuckets"],
      "Resource": ["acs:oss:*:*:*"]
    },
    {
      "Effect": "Allow",
      "Action": ["oss:*"],
      "Resource": [
        "acs:oss:*:*:yangbase-ci*",
        "acs:oss:*:*:yangbase-ci*/*"
      ]
    }
  ]
}
```

`oss:ListBuckets` cannot be scoped to a bucket prefix in Aliyun IAM; it is the one account-wide permission that has to stay. Everything else is bounded by the `yangbase-ci*` prefix.

Rotate the key on a schedule. Because the key is only used by `oss.yml`, rotation is a one-place change: update the secret, trigger the workflow manually, confirm it is green.

### Preflight

Both jobs start with `scripts/check-oss-secrets.sh`. It checks that the five variables are non-empty and, if not, fails immediately with the list of missing names and where to set them. Without it, a missing secret surfaced minutes later as `SignatureDoesNotMatch` or `invalid credentials` from inside the object-store SPI — a message that points at the code, not at the configuration, and was misdiagnosed more than once.

The script prints names only, never values. Run it locally the same way CI does:

```bash
OSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com \
OSS_BUCKET=yangbase-ci \
OSS_REGION=cn-hangzhou \
OSS_ACCESS_KEY_ID=x \
OSS_ACCESS_KEY_SECRET=y \
./scripts/check-oss-secrets.sh
```

### Manual run

Use `workflow_dispatch` with `skip_integration: true` when you only want to validate credentials and the conformance suite — the five-shard integration matrix is the expensive half.
