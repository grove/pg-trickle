# Migration Procedure: `grove/pg-trickle` → `trickle-labs/pg-trickle`

**Status:** Phase 1 complete (PR #775 merged or pending)  
**Reference:** [REPORT_TRICKLE_LABS.md](REPORT_TRICKLE_LABS.md)

---

## Before you start

- [ ] PR #775 is **merged into `main`**
- [ ] You have owner access to the `grove` GitHub organisation
- [ ] You have the `gh` CLI installed and authenticated (`gh auth status`)

---

## Phase 2 — External service setup

Do this before or on the same day as the transfer. None of these steps affect
live users.

### 2.1 Create the `trickle-labs` GitHub organisation

1. Go to https://github.com/organizations/new
2. Organisation name: **`trickle-labs`**
3. Plan: Free (or match current `grove` plan)
4. Click **Create organisation**

### 2.2 Enable GHCR for the new org

GHCR (GitHub Container Registry) is enabled automatically, but packages must
be set to allow public access if your images are public.

1. Go to https://github.com/organizations/trickle-labs/settings/packages
2. Set **Package creation** → Allow members to create public packages
3. No other manual action needed — `GITHUB_TOKEN` with `packages: write`
   handles pushes automatically after transfer

### 2.3 Set up Codecov

1. Go to https://codecov.io and sign in with GitHub
2. Click **Configure GitHub App** → install/authorise for the `trickle-labs`
   organisation
3. After the transfer Codecov will auto-detect `trickle-labs/pg-trickle`;
   you may need to visit https://app.codecov.io/github/trickle-labs/pg-trickle
   and click **Activate**
4. Under **Settings → General** for the new project, copy the **Repository
   Upload Token** — you will need it in Phase 3

### 2.4 Collect secrets you will need to re-add

GitHub does **not** copy secrets when transferring a repository.  Write these
down or store them in a password manager now.

| Secret name | Where to get it | Required by |
|---|---|---|
| `CODECOV_TOKEN` | Codecov → repo settings (step 2.3) | `coverage.yml` |
| `PGXN_USERNAME` | Your PGXN account | `pgxn.yml`, `justfile` |
| `PGXN_PASSWORD` | Your PGXN account | `pgxn.yml`, `justfile` |

---

## Phase 3 — Repository transfer

**Pick a low-traffic time.** GitHub Pages will be unavailable for a few
minutes while DNS propagates. CI will queue but not fail.

### 3.1 Transfer the repository

1. Go to: https://github.com/grove/pg-trickle/settings
2. Scroll to **Danger Zone**
3. Click **Transfer repository**
4. New owner: `trickle-labs`
5. Type `grove/pg-trickle` to confirm
6. Click **I understand, transfer this repository**

> GitHub creates a 301 redirect from `grove/pg-trickle` automatically.
> Existing `git clone` URLs keep working as long as no one re-creates
> `grove/pg-trickle`.

### 3.2 Re-add secrets (do this immediately after transfer)

```bash
# Set the new repo as target for all commands
export REPO=trickle-labs/pg-trickle

gh secret set CODECOV_TOKEN  --repo "$REPO"
# Paste the token from Codecov step 2.3, then press Enter

gh secret set PGXN_USERNAME  --repo "$REPO"
# Paste your PGXN username

gh secret set PGXN_PASSWORD  --repo "$REPO"
# Paste your PGXN password
```

Verify:

```bash
gh secret list --repo trickle-labs/pg-trickle
```

Expected output: three secrets listed (CODECOV_TOKEN, PGXN_USERNAME,
PGXN_PASSWORD).

### 3.3 Update your local git remote

```bash
git remote set-url origin https://github.com/trickle-labs/pg-trickle.git
git remote -v   # confirm
```

### 3.4 Re-configure branch protection (if needed)

Repository Settings transfer across, but team-based permissions do not.

1. Go to https://github.com/trickle-labs/pg-trickle/settings/branches
2. Verify the `main` branch protection rule is present
3. Re-add any required reviewers or CODEOWNERS if they were team-based

---

## Phase 4 — Post-transfer verification

Work through these in order. Each step depends on the previous one.

### 4.1 Rebuild the GHCR builder image

The builder image at `ghcr.io/grove/pg-trickle/builder:pg18` is frozen.
CI needs a new one at `ghcr.io/trickle-labs/pg-trickle/builder:pg18`.

```bash
gh workflow run builder-image.yml \
  --repo trickle-labs/pg-trickle \
  --ref main
```

Wait for it to complete (~10 min):

```bash
gh run list --repo trickle-labs/pg-trickle --workflow=builder-image.yml --limit 3
```

### 4.2 Run a full CI pass

```bash
gh workflow run ci.yml \
  --repo trickle-labs/pg-trickle \
  --ref main
```

Watch progress:

```bash
gh run watch --repo trickle-labs/pg-trickle
```

All jobs (unit, integration, light E2E, benchmarks) should pass. If the
builder image from 4.1 is not done yet, CI will do a slower cold build —
that is fine, just slower.

### 4.3 Verify GitHub Pages

1. Go to https://github.com/trickle-labs/pg-trickle/actions and find the
   **docs** workflow; it should have triggered automatically on the push to
   `main` after the transfer
2. If it did not trigger, run it manually:
   ```bash
   gh workflow run docs.yml --repo trickle-labs/pg-trickle --ref main
   ```
3. After it completes, verify:
   - https://trickle-labs.github.io/pg-trickle/ — main docs
   - https://trickle-labs.github.io/pg-trickle/blog/ — blog index

### 4.4 Verify Codecov badge

1. Visit https://codecov.io/gh/trickle-labs/pg-trickle
2. The coverage report should appear within a few minutes of the CI run
   finishing
3. The README badge at the top of the repo page should turn green

If the badge stays grey after 30 min: double-check `CODECOV_TOKEN` is set
(step 3.2) and re-run the `coverage.yml` workflow:

```bash
gh workflow run coverage.yml --repo trickle-labs/pg-trickle --ref main
```

### 4.5 Verify PGXN upload

PGXN uploads happen automatically on release. No immediate action needed
unless a release is imminent. When the next release is cut, check:

```bash
gh run list --repo trickle-labs/pg-trickle --workflow=pgxn.yml --limit 5
```

and confirm the upload succeeded at https://pgxn.org/dist/pg_trickle/.

### 4.6 Re-index DeepWiki

1. Visit https://deepwiki.com/trickle-labs/pg-trickle
2. If not yet indexed, click **Index repository** (or the equivalent prompt)
3. Update `README.md`'s DeepWiki badge link if it still shows the old slug
   (PR #775 already updated the badge URL; this step is just a browser check)

### 4.7 Check external integrations

Quick manual checks — none of these should require code changes, but confirm
they resolve to the new org:

| Service | URL to check |
|---|---|
| GitHub Actions (CI) | https://github.com/trickle-labs/pg-trickle/actions |
| CodeQL | Look for `codeql.yml` run in Actions; uses `github.repository` expression, no change needed |
| Dependabot | https://github.com/trickle-labs/pg-trickle/security/dependabot |

### 4.8 Announce the migration

1. Post a notice in any community channels (Discord, Slack, etc.)
2. Update `CONTRIBUTING.md` with a note like:

   > **Repository moved.** The canonical URL is now
   > `https://github.com/trickle-labs/pg-trickle`.
   > Update your local remote with:
   > ```bash
   > git remote set-url origin https://github.com/trickle-labs/pg-trickle.git
   > ```

---

## Quick checklist

```
Phase 2 — External services
  [ ] trickle-labs org created on GitHub
  [ ] GHCR package settings configured
  [ ] Codecov app authorized for trickle-labs
  [ ] CODECOV_TOKEN copied from Codecov
  [ ] PGXN credentials noted

Phase 3 — Transfer
  [ ] PR #775 merged into main
  [ ] Repository transferred to trickle-labs
  [ ] CODECOV_TOKEN secret added
  [ ] PGXN_USERNAME secret added
  [ ] PGXN_PASSWORD secret added
  [ ] Local git remote updated
  [ ] Branch protection rules verified

Phase 4 — Verification
  [ ] builder-image.yml workflow run successfully
  [ ] ci.yml full pass — all jobs green
  [ ] GitHub Pages live at trickle-labs.github.io/pg-trickle/
  [ ] Codecov badge resolving
  [ ] DeepWiki re-indexed
  [ ] CONTRIBUTING.md updated with migration notice
  [ ] Announcement sent
```
