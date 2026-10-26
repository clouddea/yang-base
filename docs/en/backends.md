# Object Storage Backends

[English](backends.md) | [简体中文](../zh/backends.md)

YangBase keeps **all durable state** in object storage and treats it as the only source of truth. That makes the backend part of the correctness surface, not a deployment detail: writer election, manifest pointer switches and fencing are all built on the backend's conditional-write semantics.

This page covers which backends work, what "works" means precisely, and how to verify a new one instead of trusting vendor documentation.

---

## 1. Supported backends

| Backend | Status | Notes |
|---|---|---|
| In-memory | ✅ test-only | Reference semantics; not durable, not for real use |
| Local filesystem | ✅ test/dev only | Requires `--i-know-this-is-not-durable`; used by tests and single-node experiments |
| AWS S3 | ✅ supported | `If-None-Match: *` for atomic create, `If-Match` for CAS |
| MinIO | ✅ **verified** | Path-style addressing; integration suite runs against it |
| Aliyun OSS | ✅ **verified on the real service** | Rejects `If-None-Match`; uses `x-oss-forbid-overwrite` (409); **no ETag CAS** → pointer switch needs `--i-accept-nonatomic-pointer-switch` |
| Tencent COS | ⚠️ expected to work, **not yet verified** | S3-compatible; run the conformance suite before relying on it |
| Huawei OBS | ⚠️ expected to work, **not yet verified** | S3-compatible; run the conformance suite |
| Cloudflare R2 | ⚠️ expected to work, **not yet verified** | LIST is eventually consistent — that capability must be declared `false` |
| Backblaze B2 (S3 API) | ⚠️ expected to work, **not yet verified** | Endpoint/region come from the bucket's S3 endpoint |

"Verified" means: someone ran the conformance suite and the full integration suite against the real service, and the results are recorded in the project history. "Expected to work" means only that the service speaks the S3 API — it is a hypothesis, not a claim.

---

## 2. Configuring a backend: provider presets

Connecting a new backend used to mean filling in four things by hand — endpoint, region, addressing style, capability declaration — and two of them **fail silently when wrong**: path-style addressing against Aliyun OSS returns `SecondLevelDomainForbidden`, and S3-style conditional headers against OSS are rejected outright (400) rather than ignored.

`--s3-provider` encodes those four into one preset:

```bash
# Aliyun OSS: endpoint is derived from the region, virtual-host addressing,
# atomic create via x-oss-forbid-overwrite, no ETag CAS
yb-server --s3-provider oss --s3-region cn-hangzhou \
          --s3-bucket my-bucket --s3-access-key "$AK" --s3-secret-key "$SK"

# MinIO / self-hosted gateway: endpoint required, path-style addressing
yb-server --s3-provider minio --s3-endpoint http://127.0.0.1:9000 \
          --s3-bucket yangbase --s3-access-key "$AK" --s3-secret-key "$SK"
```

| Provider id | Endpoint | Default region | Addressing | Capabilities | Verified |
|---|---|---|---|---|---|
| `aws` (default) | SDK-resolved | `us-east-1` | auto | full S3 | ✅ |
| `minio` | must be given | `us-east-1` | path | full S3 | ✅ |
| `oss` | `https://oss-{region}.aliyuncs.com` | `cn-hangzhou` | virtual-host | **no ETag CAS**, `FORBID_OVERWRITE` | ✅ on the real service |
| `cos` | `https://cos.{region}.myqcloud.com` | `ap-guangzhou` | virtual-host | full S3 (assumed) | ❌ not yet |
| `obs` | `https://obs.{region}.myhuaweicloud.com` | `cn-north-4` | virtual-host | full S3 (assumed) | ❌ not yet |
| `r2` | must be given (`https://<account>.r2.cloudflarestorage.com`) | `auto` | path | full S3 (assumed) | ❌ not yet |
| `b2` | `https://s3.{region}.backblazeb2.com` | `us-west-004` | path | full S3 (assumed) | ❌ not yet |

Explicit flags always win over the preset (`--s3-endpoint`, `--s3-region`, `--s3-path-style`). `--s3-oss` is kept as a deprecated alias for `--s3-provider oss`.

**Presets declare an _upper bound_ of capabilities, not a promise.** The startup probe measures the real backend and intersects (`declared ∩ measured`), so a preset that is too optimistic gets corrected — or the server refuses to start. That is why `cos`/`obs`/`r2`/`b2` can ship with "full S3" declared while still being marked *not verified*: the declaration means "try standard S3 semantics first", not "we tested it".

---

## 3. What the backend contract actually is

The SPI is deliberately narrow — five operations:

```kotlin
suspend fun get(key: String, range: ByteRange? = null): ByteArray
suspend fun put(key: String, data: ByteArray, precondition: Precondition? = null): ETag
suspend fun delete(key: String): Boolean      // idempotent; false if absent
suspend fun list(prefix: String, pageToken: String? = null, limit: Int = 1000): ListPage
suspend fun head(key: String): ObjectMeta
```

`Precondition` has exactly two forms, and their **failure modes differ**:

| Precondition | Meaning | On conflict |
|---|---|---|
| `MustNotExist` | create only if absent | definite failure — do **not** retry |
| `MustMatch(etag)` | compare-and-swap | definite failure — do **not** retry |

Backends express these differently and that difference is imposed by the service, not by us. That is why the capability record exists:

```kotlin
data class ObjectStoreCapabilities(
    val conditionalCreate: Boolean,
    val atomicCreateStyle: AtomicCreateStyle,   // IF_NONE_MATCH or FORBID_OVERWRITE
    val conditionalUpdate: Boolean,             // ETag CAS
    val strongReadAfterWrite: Boolean,
    val strongListConsistency: Boolean,
    val lowLatencyTier: Boolean,
    val lowLatencyTierSupportsConditionalWrite: Boolean,
    val conditionalWriteVerified: Boolean,
    val requestsAreBilled: Boolean,
    val serverSideCopy: Boolean,
    val eventNotification: Boolean,
)
```

Two rules follow from this, and both are enforced as bugs, not conventions:

1. **Code must check a capability before relying on it.** A path that uses CAS on a backend without `conditionalUpdate` must either refuse to start or take the explicitly-degraded route — never silently fall through.
2. **Unsupported preconditions must throw `CapabilityNotSupportedException`.** Silently ignoring `If-Match` is the single most dangerous backend behaviour: the system keeps running and quietly loses updates. (Google Cloud Storage's S3 interoperability layer is the known example.)

---

## 4. Two different failures, two different responses

| Situation | Detection | Response |
|---|---|---|
| Backend **honestly declares** it lacks a capability | Capability probe, or the declared preset | Degrade **explicitly**: either refuse to start with an actionable message, or require a flag that says "I accept this weakness" (`--i-accept-nonatomic-pointer-switch`) |
| Backend **claims** a capability and does not deliver | Conformance suite / capability probe | **Refuse to start.** There is no safe degradation for "pretends to do CAS" |

The distinction matters because the two produce the same symptom later (corrupted state under concurrency) but only one of them is survivable.

---

## 5. Running the conformance suite

The suite lives in `yb-objectstore` and runs as part of `./gradlew build`:

```bash
./gradlew :yb-objectstore:test --tests '*ObjectStoreConformanceTest*'
```

It always exercises four local rigs (memory, local filesystem, embedded S3 in standard style, embedded S3 in OSS style). To point it at a real backend, set environment variables:

```bash
export YANGBASE_CONFORMANCE_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com
export YANGBASE_CONFORMANCE_BUCKET=my-bucket
export YANGBASE_CONFORMANCE_ACCESS_KEY=...
export YANGBASE_CONFORMANCE_SECRET_KEY=...
export YANGBASE_CONFORMANCE_REGION=cn-hangzhou
export YANGBASE_CONFORMANCE_STYLE=oss          # or omit for plain S3 semantics
export YANGBASE_CONFORMANCE_PATH_STYLE=false   # OSS requires virtual-host addressing
export YANGBASE_CONFORMANCE_NAME=aliyun-oss-hz
./gradlew :yb-objectstore:test --tests '*ObjectStoreConformanceTest*'
```

The run writes a Markdown matrix to `yb-objectstore/build/reports/conformance/matrix.md`:

- ✅ the check passed
- ⊘ skipped because the backend **declares** it does not support that capability (the reason is printed in the cell)
- ❌ the backend declares support but the behaviour differs — this is the class of bug that corrupts data silently

CI publishes the matrix into the job summary and uploads it as an artifact. **The compatibility matrix is the output of a real run, not a hand-maintained table.**

### What the checks cover

Basic semantics (roundtrip, empty object, 1 MiB object, missing key, HEAD metadata, overwrite changes ETag, delete idempotence, range reads), listing (prefix filtering, lexicographic order, pagination without duplicates or gaps, optional sizes), key charset (CJK, spaces, `+`/`%`, slashes), consistency (read-your-writes, delete visible immediately), and the conditional-write invariants:

- `MustNotExist` under concurrency: **exactly one** writer wins
- `MustMatch` with a stale ETag: the write **fails** and the stored value is unchanged
- undeclared preconditions: **rejected**, never silently ignored

---

## 6. Verifying a new backend

1. Run the conformance suite against it with the environment variables above. Fix ❌ rows or declare the capability honestly as `false`.
2. Run the integration suite against it — `./gradlew integrationTest` with the store pointed at the backend. This exercises the LSM/transaction layers, not only the SPI.
3. Write down what you observed (endpoint quirks, which preconditions are rejected, consistent LIST or not) in this table.
4. Only then mark it "verified".

If a backend turns out to be unable to satisfy a **critical** capability (no atomic create at all), it cannot host the manifest, and YangBase should refuse to start on it rather than pretend.
