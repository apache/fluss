# Generate Release Note

Use GitHub's **Generate release notes** to produce a draft from merged PRs between tags. Categories (Added, Fixed, Docs, etc.) are configured in [.github/release.yml](https://github.com/apache/fluss-rust/blob/main/.github/release.yml).

1. Go to [Creating a Fluss Client Release](create-release.md).
2. In **Choose a tag**, pick the release tag (e.g. `v0.1.0`).
3. Click **Generate release notes**.
4. Copy the generated content for **CHANGELOG.md** or the GitHub Release description. When publishing the release, add the official download link, checksums/verification, and install instructions (see [Creating a Fluss Rust Client Release](create-release.md)).

See [Creating a Fluss Rust Client Release](create-release.md) and [GitHub: Automatically generated release notes](https://docs.github.com/en/repositories/releasing-projects-on-github/automatically-generated-release-notes).
