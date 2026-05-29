---
sidebar_position: 4
---

# Creating a Fluss Rust Client Release

This document describes in detail how to create a release of the **Fluss clients** (fluss-rust, fluss-python, fluss-cpp) from the [fluss-rust](https://github.com/apache/fluss-rust) repository. It is based on the [Creating a Fluss Release](https://fluss.apache.org/community/how-to-release/creating-a-fluss-release/) guide of the Apache Fluss project and the [release guide of Apache OpenDAL](https://nightlies.apache.org/opendal/opendal-docs-stable/community/release/); releases are source archives plus CI-published crates.io and PyPI.

Publishing software has legal consequences. This guide complements the foundation-wide [Product Release Policy](https://www.apache.org/legal/release-policy.html) and [Release Distribution Policy](https://infra.apache.org/release-distribution.html).

## Overview

![Release process overview](/img/release-guide.png)

The release process consists of:

1. [Decide to release](#decide-to-release)
2. [Prepare for the release](#prepare-for-the-release)
3. [Build a release candidate](#build-a-release-candidate)
4. [Vote on the release candidate](#vote-on-the-release-candidate)
5. [If necessary, fix any issues and go back to step 3](#fix-any-issues)
6. [Finalize the release](#finalize-the-release)
7. [Promote the release](#promote-the-release)

## Decide to release

Deciding to release and selecting a Release Manager is the first step. This is a consensus-based decision of the community.

Anybody can propose a release (e.g. on the dev [mailing list](https://fluss.apache.org/community/welcome/)), giving a short rationale and nominating a committer as Release Manager (including themselves). Any objections should be resolved by consensus before starting.

**Checklist to proceed**

- [ ] Community agrees to release
- [ ] A Release Manager is selected

## Prepare for the release

### 0. One-time Release Manager setup

Before your first release, perform one-time configuration. See **[Release Manager Preparation](https://fluss.apache.org/community/how-to-release/release-manager-preparation/)** (GPG key, etc.). For fluss-rust you do **not** need Nexus/Maven; you only need GPG for signing the source archive and (optionally) git signing.

For GitHub Actions publishing, configure the repository secret `CARGO_REGISTRY_TOKEN` with a crates.io API token from an account allowed to publish `fluss-rs`. The `Release Rust` workflow uses this secret directly when a release tag is pushed.

**Checklist (one-time)**

- [ ] GPG key set up and published to [KEYS](https://downloads.apache.org/incubator/fluss/KEYS) or Apache account
- [ ] Git configured to use your GPG key for signing tags
- [ ] GitHub Actions secret `CARGO_REGISTRY_TOKEN` configured for crates.io publishing

### 1. Install Rust (and optional: just)

The release script (`just release` or `./scripts/release.sh`) uses `git archive` and `gpg`; building or verifying the project locally requires **Rust**. Install the [Rust toolchain](https://rustup.rs/) (the version should match [rust-toolchain.toml](https://github.com/apache/fluss-rust/blob/main/rust-toolchain.toml) in the repo). The dependency list script (`scripts/dependencies.py`) requires **Python 3.11+**.

```bash
rustc --version
cargo --version
```

To use `just release`, install [just](https://github.com/casey/just) (e.g. `cargo install just` or your system package manager). If you prefer not to use just, run `./scripts/release.sh $RELEASE_VERSION` instead.

### 2. Optional: Create a new Milestone in GitHub

If the project uses GitHub milestones for release tracking, create a new milestone for the **next** version (e.g. `v0.2` if you are releasing `0.1.x`). This helps contributors target issues to the correct release.

### 3. Optional: Triage release-blocking issues

Check open issues that might block the release. Resolve, defer to the next milestone, or mark as blocker and do not proceed until they are fixed.

### 4. Clone fluss-rust into a fresh workspace

Use a clean clone to avoid local changes affecting the release.

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust
```

### 5. Set up environment variables

Set these once and use them in all following commands. (Bash syntax.)

```bash
RELEASE_VERSION="0.1.0"
SHORT_RELEASE_VERSION="0.1"
RELEASE_TAG="v${RELEASE_VERSION}"
SVN_RELEASE_DIR="fluss-rust-${RELEASE_VERSION}"
# Only set if there is a previous release (for compare link in DISCUSS / release notes)
LAST_VERSION="0.0.9"
NEXT_VERSION="0.2.0"
```

For the **first release** there is no previous version; leave `LAST_VERSION` unset or omit it when using the compare link in the DISCUSS thread and release notes.

### 6. Generate dependencies list

[ASF release policy](https://www.apache.org/legal/release-policy.html) requires that every release comply with [ASF licensing policy](https://www.apache.org/legal/resolved.html) and that an **audit be performed before a full release**. Generating and committing a dependency list (and using cargo-deny) documents third-party components and supports this requirement.

Do this on `main` **before** creating the release branch. Then both the release branch (when created from `main`) and `main` will have the same dependency list.

1. Download and set up [cargo-deny](https://embarkstudios.github.io/cargo-deny/cli/index.html) (see cargo-deny docs).
2. Run the script to update the dependency list (requires **Python 3.11+** for the release tooling), then commit on `main`:

```bash
git checkout main
git pull
python3 scripts/dependencies.py generate
git add **/DEPENDENCIES*.tsv
# Bash: run  shopt -s globstar  first so ** matches subdirs
git commit -m "chore: update dependency list for release ${RELEASE_VERSION}"
git push origin main
```

To only check licenses (no file update): `python3 scripts/dependencies.py check`.

### 7. Optional: Start a [DISCUSS] thread

On [Fluss Discussions](https://github.com/apache/fluss-rust/discussions) or the dev list:

- **Subject:** `[DISCUSS] Release Apache Fluss clients (fluss-rust, fluss-python, fluss-cpp) $RELEASE_VERSION`
- **Body:** Short rationale; if there is a previous release, add compare link: `https://github.com/apache/fluss-rust/compare/v${LAST_VERSION}...main`. Ask for comments.

### 8. Create a release branch

From `main`, create a release branch. All release artifacts will be built from this branch. The tag (RC or release) is created later when building the release candidate.

```bash
git checkout main
git pull
git checkout -b release-${SHORT_RELEASE_VERSION}
git push origin release-${SHORT_RELEASE_VERSION}
```

Do **not** create or push the release/RC tag yet; that happens in [Build a release candidate](#build-a-release-candidate) after the source artifacts are staged.

### 9. Bump version on main for the next development cycle

So that `main` moves to the next version immediately after the release branch is cut, run the bump script and commit:

```bash
git checkout main
git pull

./scripts/bump-version.sh $RELEASE_VERSION $NEXT_VERSION

git add Cargo.toml
git commit -m "Bump version to ${NEXT_VERSION}"
git push origin main
```

The script updates the root `Cargo.toml` ([workspace.package] and [workspace.dependencies] fluss-rs). crates/fluss and bindings inherit `version` from the workspace.

### 10. Optional: Create PRs for release blog and download page

You can open a pull request in the **Apache Fluss** repository for the release blog (announcement). If the project website has a download page, also create a PR to add the new version there. **Do not merge these PRs until the release is finalized.**

---

**Checklist to proceed to the next step**

- [ ] Rust (and optionally just) installed and on PATH
- [ ] Python 3.11+ for dependency list script
- [ ] No release-blocking issues (or triaged)
- [ ] Environment variables set
- [ ] Release branch created and pushed
- [ ] Main branch bumped to `NEXT_VERSION` and pushed
- [ ] Dependencies list generated and committed on main
- [ ] (Optional) DISCUSS thread and/or tracking issue created
- [ ] (Optional) PRs for blog and download page created but not merged

## Build a release candidate

Each release candidate is built from the release branch, signed, and staged to the dev area of dist.apache.org. If an RC fails the vote, fix issues and repeat this section with an incremented `RC_NUM` (see [Fix any issues](#fix-any-issues)).

### 1. Set RC environment variables

Set these when building a **release candidate**. Start with `RC_NUM=1`; if the vote fails and you build a new candidate, increment to `2`, then `3`, etc.

```bash
export RC_NUM="1"
export RC_TAG="v${RELEASE_VERSION}-rc${RC_NUM}"
export SVN_RC_DIR="fluss-rust-${RELEASE_VERSION}-rc${RC_NUM}"
```

For a **direct release** (no RC), skip these and use `RELEASE_TAG` and `SVN_RELEASE_DIR` from the Prepare step instead.

### 2. Check out the release branch and create the tag

Check out the release branch at the commit you want to release, create the signed tag, then push it. Use `RC_TAG` for a release candidate or `RELEASE_TAG` for a direct release. Pushing the tag triggers GitHub Actions (for an RC tag, fluss-python is published to TestPyPI).

```bash
git checkout release-${SHORT_RELEASE_VERSION}
git pull
git tag -s $RC_TAG -m "${RC_TAG}"
git push origin $RC_TAG
```

Check CI: [Actions](https://github.com/apache/fluss-rust/actions) (Release Rust, Release Python).

### 3. Create source release artifacts

From the repository root (on the release branch, at the commit you tagged):

```bash
just release $RELEASE_VERSION
# Or: ./scripts/release.sh $RELEASE_VERSION
```

This creates under `dist/`:

- `fluss-rust-${RELEASE_VERSION}-incubating.tgz`
- `fluss-rust-${RELEASE_VERSION}-incubating.tgz.sha512`
- `fluss-rust-${RELEASE_VERSION}-incubating.tgz.asc`

(Incubator policy requires the word "incubating" in release artifact names.)

Verify with: `gpg --verify dist/fluss-rust-${RELEASE_VERSION}-incubating.tgz.asc dist/fluss-rust-${RELEASE_VERSION}-incubating.tgz`

### 4. Stage artifacts to SVN (dist.apache.org dev)

From the **fluss-rust** repo root, check out the Fluss dev area and add the release artifacts.

```bash
svn checkout https://dist.apache.org/repos/dist/dev/incubator/fluss fluss-dist-dev --depth=immediates
cd fluss-dist-dev
mkdir $SVN_RC_DIR
cp ../dist/fluss-rust-${RELEASE_VERSION}-incubating.* $SVN_RC_DIR/
svn add $SVN_RC_DIR
svn status
svn commit -m "Add fluss-rust ${RELEASE_VERSION} RC${RC_NUM}"
```

Verify: [https://dist.apache.org/repos/dist/dev/incubator/fluss/](https://dist.apache.org/repos/dist/dev/incubator/fluss/)

---

**Checklist to proceed to the next step**

- [ ] Source distribution built and signed under `dist/`
- [ ] Artifacts staged to [dist.apache.org dev](https://dist.apache.org/repos/dist/dev/incubator/fluss/) under `$SVN_RC_DIR`
- [ ] RC (or release) tag pushed to GitHub
- [ ] CI for Release Rust / Release Python succeeded

## Vote on the release candidate

Share the release candidate for community review. If the project is in incubation, a [two-phase vote](https://incubator.apache.org/cookbook/#two_phase_vote_on_podling_releases) (Fluss community then Incubator PMC) may be required; otherwise one community vote is enough.

### Fluss community vote

Start the vote on the dev@ mailing list.

**Subject:** `[VOTE] Release Apache Fluss clients (fluss-rust, fluss-python, fluss-cpp) ${RELEASE_VERSION} (RC${RC_NUM})`

**Body template:**

```
Hi everyone,

Please review and vote on release candidate #${RC_NUM} for Apache Fluss clients (fluss-rust, fluss-python, fluss-cpp) ${RELEASE_VERSION}.

[ ] +1 Approve the release
[ ] +0 No opinion
[ ] -1 Do not approve (please provide specific comments)

The release candidate (source distribution) is available at:
* https://dist.apache.org/repos/dist/dev/incubator/fluss/$SVN_RC_DIR/

KEYS for signature verification:
* https://downloads.apache.org/incubator/fluss/KEYS

Git tag:
* https://github.com/apache/fluss-rust/releases/tag/$RC_TAG

PyPI (release) / TestPyPI (RC):
* https://pypi.org/project/pyfluss/
* https://test.pypi.org/project/pyfluss/

Please download, verify, and test. Verification steps are in [How to Verify a Release Candidate](verifying-a-release-candidate.md).

The vote will be open for at least 72 hours. It is adopted by majority approval with at least 3 PPMC affirmative votes (or as per project policy).

Thanks,
Release Manager
```

If issues are found, cancel the vote and go to [Fix any issues](#fix-any-issues). If the vote passes, close it and tally the result in a follow-up:

**Subject:** `[RESULT][VOTE] Release Apache Fluss clients ${RELEASE_VERSION} (RC${RC_NUM})`

**Body:** Summarize binding and non-binding votes and link to the vote thread.

### Incubator PMC vote (if applicable)

If the project is in incubation, start a vote on general@incubator.apache.org after the Fluss community vote passes. Use the same structure: link to the community vote thread, release candidate URL, KEYS, tag, and ask IPMC to vote within 72 hours. Then send the result to the same list.

---

**Checklist to proceed to finalization**

- [ ] Community vote passed (at least 3 binding +1, more +1 than -1)
- [ ] (If incubating) Incubator PMC vote passed

## Fix any issues

If the vote revealed issues:

1. Fix them on `main` (or the release branch) via normal PRs; cherry-pick fixes into the release branch as needed.
2. Remove the old RC from dist.apache.org dev (optional but recommended):

```bash
svn checkout https://dist.apache.org/repos/dist/dev/incubator/fluss fluss-dist-dev --depth=immediates
cd fluss-dist-dev
svn remove $SVN_RC_DIR
svn commit -m "Remove fluss-rust ${RELEASE_VERSION} RC${RC_NUM} (superseded)"
```

3. Increment `RC_NUM` (e.g. set `RC_NUM="2"`), recreate `RC_TAG` and `SVN_RC_DIR`, then go back to [Build a release candidate](#build-a-release-candidate) and repeat until a candidate is approved.

**Checklist**

- [ ] Issues resolved and changes merged/cherry-picked to the release branch
- [ ] New RC built and voted on (or same RC re-voted if only minor fixes)

## Finalize the release

Once a release candidate has been approved, finalize the release.

### 1. Push the release git tag (if the vote was on an RC)

If the community voted on an RC tag, create and push the formal release tag so CI publishes to crates.io and PyPI:

```bash
git checkout $RC_TAG
git tag -s $RELEASE_TAG -m "Release fluss-rust, fluss-python, fluss-cpp ${RELEASE_VERSION}"
git push origin $RELEASE_TAG
```

### 2. Deploy source artifacts to the release repository

Move the staged artifacts from dev to release:

```bash
svn mv -m "Release fluss-rust ${RELEASE_VERSION}" \
  https://dist.apache.org/repos/dist/dev/incubator/fluss/$SVN_RC_DIR \
  https://dist.apache.org/repos/dist/release/incubator/fluss/$SVN_RELEASE_DIR
```

(Only PPMC members may have write access to the release repository; if you get permission errors, ask on the mailing list.)

### 3. Remove old RC(s) from dev (optional)

Clean up the dev area so only the current RC or the moved release remains:

```bash
cd fluss-dist-dev
svn remove $SVN_RC_DIR
svn commit -m "Remove RC after release fluss-rust ${RELEASE_VERSION}"
```

### 4. Verify language artifacts

- **fluss-rust:** [crates.io/crates/fluss-rs](https://crates.io/crates/fluss-rs) shows version `$RELEASE_VERSION`
- **fluss-python:** [PyPI – pyfluss](https://pypi.org/project/pyfluss/) shows version `$RELEASE_VERSION`
- **fluss-cpp:** Distributed via the source archive; no separate registry

### 5. Create GitHub Release

1. Go to [Releases → New release](https://github.com/apache/fluss-rust/releases/new).
2. Choose tag `$RELEASE_TAG`.
3. Set the target to the release branch `release-${RELEASE_VERSION}` (i.e., the branch/commit used to create `$RELEASE_TAG`).
4. Click **Generate release notes**, then add: notable changes, breaking changes (if any) from component upgrade docs, **official download link** (source archive and verification), and install instructions for fluss-rust, fluss-python, fluss-cpp.
    - **Download link:** `https://downloads.apache.org/incubator/fluss/fluss-rust-${RELEASE_VERSION}/` (or the project download page). In the release description, include checksums and GPG verification steps.
5. Click **Publish release**.

### 6. Update CHANGELOG.md on main

Add an entry for `$RELEASE_VERSION` with the list of changes (use [Generate Release Note](generate-release-note.md) from the release tag). Commit and push to `main`.

---

**Checklist to proceed to promotion**

- [ ] Release tag pushed; CI published to crates.io and PyPI
- [ ] Source artifacts in [dist release](https://dist.apache.org/repos/dist/release/incubator/fluss/)
- [ ] GitHub Release created
- [ ] CHANGELOG.md updated on main

## Promote the release

### Merge website PRs

Merge the pull requests for the release blog and download page that were created in [Prepare for the release](#10-optional-create-prs-for-release-blog-and-download-page).

### Announce the release

Wait at least 24 hours after finalizing, per [ASF release policy](https://www.apache.org/legal/release-policy.html#release-announcements).

- Announce on the dev mailing list that the release is complete.
- Announce on [Fluss Discussions – Announcements](https://github.com/apache/fluss-rust/discussions) (if that category exists).
- Send the release announcement to **announce@apache.org**.

Use the `@apache.org` email address and **plain text** for the body; otherwise the list may reject the message.

**Subject:** `[ANNOUNCE] Release Apache Fluss clients (fluss-rust, fluss-python, fluss-cpp) ${RELEASE_VERSION}`

**Body template:**

```
The Apache Fluss community is pleased to announce the release of Apache Fluss clients (fluss-rust, fluss-python, fluss-cpp) ${RELEASE_VERSION}.

This release includes ...
(Notable changes; link to CHANGELOG or release notes.)

Download and verification:
* https://downloads.apache.org/incubator/fluss/$SVN_RELEASE_DIR/
* KEYS: https://downloads.apache.org/incubator/fluss/KEYS (or https://downloads.apache.org/fluss/KEYS after graduation)

Rust:    cargo add fluss-rs
Python:  pip install pyfluss
C++:     build from source (see project documentation)

Release notes: https://github.com/apache/fluss-rust/releases/tag/$RELEASE_TAG

Thanks to all contributors!

Release Manager
```

---

**Checklist to declare the process completed**

- [ ] Release announced on dev list and (if applicable) user list
- [ ] Release announced on announce@apache.org
- [ ] Release blog published (if applicable)
- [ ] Download page updated (if applicable)

## Improve the process

After finishing the release, consider what could be improved (simplifications, clearer steps, automation). Propose changes on the dev list or via a pull request to this guide.

## See also

- [Release Manager Preparation](https://fluss.apache.org/community/how-to-release/release-manager-preparation/) — GPG and one-time setup
- [How to Verify a Release Candidate](verifying-a-release-candidate.md) — Verify signatures, checksums, build, and tests for a release candidate
- [ASF Release Policy](https://www.apache.org/legal/release-policy.html)
