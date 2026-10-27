# Versioning and Release Policy

[English](versioning.md) | [简体中文](../zh/versioning.md)

> Effective 2026-10-27. Owner: the maintainer. This document is the authoritative record of
> how YangBase is versioned, how a version is branched, and what must be true before a version
> is released. The branching mechanics live in [development-process.md](../zh/development-process.md);
> this document adds the release-train rules on top of them.

---

## 1. What the three numbers mean

`MAJOR.MINOR.PATCH` — same meaning as [Semantic Versioning](https://semver.org/), specialised for a
database where the "public interface" includes the **data on disk** and the **wire protocol**, not
just Java classes.

| Component | Bumped when | Examples |
|---|---|---|
| **MAJOR** | A change that is **not upgrade-compatible**: object layout / recovery incompatibility (new binary must migrate old data, or old binary cannot read new data), removal or semantic change of SQL surface users depend on, MySQL wire-protocol incompatibility, breaking change to the `ObjectStore` SPI, removal or changed meaning of a CLI flag or config key | `0.x → 1.0`, `1.x → 2.0` |
| **MINOR** | New behaviour, **backward compatible**: new SQL construct, new object-store backend, new index type, performance or space improvements, new metrics, new CLI subcommand | `0.1 → 0.2` |
| **PATCH** | **Fixes only**: no new feature, no new config key, no behavioural change other than the defect being fixed | `0.2.0 → 0.2.1` |

Two consequences that people get wrong:

- **A data-format change is a MAJOR bump even if no API changed.** Adding a field to the manifest,
  changing the `.idx` header, changing the key prefix scheme — all of those are MAJOR. Shipping one
  in a MINOR means someone's upgrade silently reads garbage.
- **A bug fix that changes observable results is a PATCH only if the old result was wrong.** If the
  fix changes a documented behaviour, it is a MINOR (or a MAJOR, if users must act).

While the project is in `0.x` the compatibility promises are weaker — but the rules above still
apply, because the point of versioning is that an upgrade is predictable, not that a number is
large.

---

## 2. Cadence

- **One release every Friday, 18:00 Asia/Shanghai.** Each weekly release bumps MINOR during `0.x`
  (`0.1.0`, `0.2.0`, `0.3.0`, …), so users always know a specific Friday is a specific version.
- **PATCH releases happen off-train**, only for an urgent fix on a released version. No fixed day.
- **MAJOR** is not on the train. `1.0.0` ships when the production gate is met (target 2026-12 for
  the tag, 2027-09 for "production ready" in the annual OKR sense); after that, MAJOR bumps are rare
  and announced ahead of time.
- If a week produced nothing worth shipping, **the release is skipped, not padded**. Skipping is
  fine and is recorded in the release notes of the next version ("no release on <date>"). Shipping
  an empty version to keep a cadence is worse than breaking the cadence.

---

## 3. Branch model and the freeze rule

```text
main  ──●──────────────●──────────────●──    only release merges; every commit is tagged
         \            /              /
     dev/0.1.0 \      /  dev/0.2.0   /
                \    /              /
                 ●──●──────────────●          integration branches, one per version
```

- `dev/X.Y.Z` is cut from `main` immediately after the previous `vX.Y.Z` tag.
- Work happens on short-lived `feature/X.Y.Z/<slug>`, `fix/…`, `docs/…`, `test/…` branches and is
  squash-merged into `dev/X.Y.Z`.
- Release: PR `dev/X.Y.Z → main` titled `release: X.Y.Z` → merge commit → annotated tag `vX.Y.Z` on
  `main` → `.github/workflows/release.yml` builds, verifies and publishes the GitHub Release.

### 🔒 The hard rule

**Once `vX.Y.Z` is tagged, version `X.Y.Z` is closed. No further commits.**

- `dev/X.Y.Z` is **frozen** — read-only. It is listed in
  [`.github/frozen-branches.txt`](../../.github/frozen-branches.txt), and CI fails any pull request
  that targets a frozen branch. It is kept for 90 days as a reference, then deleted.
- The tag is protected: it cannot be moved or deleted (repository ruleset `protect-release-tags`).
- A new feature or a new fix **never** lands on a released version. It opens the next version:
  fix → next PATCH (`dev/X.Y.(Z+1)`), feature → next MINOR (`dev/X.(Y+1).0`).
- Found a defect in a release? It is fixed forward and shipped as a new version. "Just one more
  commit on the release branch" is exactly the thing this rule forbids.

Two narrow exceptions, both of which keep the code frozen:

1. **Release-notes typo** — edit the GitHub Release description, not the repository.
2. **A broken release/publish workflow** — fix it on `main` via PR; it takes effect for the *next*
   version, not the one already published.

Why so strict: a tag that keeps moving is a tag nobody can reproduce. The whole value of
`v0.3.0` is that a stranger can check it out in a year and get byte-identical sources.

---

## 4. Release gate — no release with a known major defect

Every release candidate must satisfy **all** of:

1. **CI green on `dev/X.Y.Z`** — `build · test · coverage`, `integration tests · embedded S3`, and
   the backend conformance matrix. A red or flaky run means not shipped.
2. **No open issue labelled `severity: critical` or `severity: major`** targeted at this milestone.
   Such an issue blocks the release until it is fixed, or is explicitly **demoted** in writing
   (a comment explaining why it is not release-blocking, and what the user impact is). Demotion is
   a decision with a name on it, not a silent label edit.
3. **No known data-loss, correctness or silent-wrong-result defect** of any severity. Those are
   never "known issues" — they are blockers.
4. **Flaky tests count as defects.** An intermittently failing test blocks the release until it is
   fixed or quarantined with a tracking issue. Raising a timeout to make CI green is not a fix.
5. **`CHANGELOG.md` has a `## [X.Y.Z]` section** written for humans: what changed, what breaks,
   what is still broken. "Known issues" is mandatory when anything is known.
6. **Docs updated** for anything user-visible, with `docs/en` and `docs/zh` in sync.
7. **`version.txt` equals the tag**, and `scripts/release-preflight.sh --version X.Y.Z` passes.

The preflight script automates 1, 2, 5 and 7; 3, 4 and 6 need a human to look.

---

## 5. Changelog

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/): `Added`,
`Changed`, `Fixed`, `Deprecated`, `Removed`, `Security`, plus a `Known issues` section that this
project treats as mandatory. Entries land under `## [Unreleased]` in the same PR that changes the
behaviour — not at release time from `git log`, which produces a list of commit subjects rather
than a list of things a user cares about.

At release, `## [Unreleased]` is renamed to `## [X.Y.Z] - YYYY-MM-DD` and a fresh empty
`Unreleased` section is added. `release.yml` extracts that section as the GitHub Release body.

---

## 6. Checklist

```bash
# ── every day: work goes to a short-lived branch, PR into dev/X.Y.Z ──
git switch dev/0.2.0 && git pull
git switch -c feature/0.2.0/<slug>

# ── Friday 18:00: release ──
scripts/release-preflight.sh --version 0.2.0      # gate: CI, issues, changelog, version, tag
gh pr create --base main --head dev/0.2.0 --title "release: 0.2.0"
gh api -X PUT repos/clouddea/yang-base/pulls/<N>/merge -f merge_method=merge
git switch main && git pull
git tag -a v0.2.0 -m "YangBase 0.2.0" && git push origin v0.2.0
#   → release.yml: verify tag == version.txt, run build + integrationTest,
#     publish the GitHub Release with the CHANGELOG section, upload the distributions

# ── immediately after: freeze and open the next window ──
echo "dev/0.2.0" >> .github/frozen-branches.txt     # commit on main via PR
git switch main && git switch -c dev/0.3.0          # next MINOR window opens
```

Next scheduled releases: see the milestones on GitHub (`0.2.0`, `0.3.0`, … each due on a Friday).
