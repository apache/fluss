---
title: Verifying a Fluss Shaded Release
sidebar_position: 3
---

# Verifying a Fluss Shaded Release

## Validating distributions

Release vote email includes links to:

- Distribution archives (source, admin, server) on dist.apache.org
- Signature files (.asc)
- Checksum files (.sha512)
- KEYS file

After downloading the distributions archives, signatures, checksums, and KEYS file, here are the instructions on how to verify signatures, checksums.

## Verifying signatures

First, import the keys in your local keyring:

```bash
curl https://downloads.apache.org/incubator/fluss/KEYS -o KEYS
gpg --import KEYS
```

Next, verify all `.asc` files:

```bash
for i in *.tgz; do echo $i; gpg --verify $i.asc $i; done
```
If the verification is successful, you will see a message like this:

```
fluss-shaded-1.0-incubating-src.tgz
gpg: Signature made Mon 01 Jan 2024 12:00:00 PM UTC
gpg:                using RSA key E2C45417BED5C104154F341085BACB5AEFAE3202
gpg: Good signature from "Jark Wu (CODE SIGNING KEY) <jark@apache.org>"
```

## Verifying checksums

Next, verify all the checksums:

```bash
for i in *.tgz; do echo $i; sha512sum --check  $i.sha512*; done
```

If the verification is successful, you will see a message like this:

```
fluss-shaded-1.0-incubating-src.tgz
fluss-shaded-1.0-incubating-src.tgz: OK
```

## Verifying build

Unzip the source release archive (`fluss-shaded-1.0-incubating-src.tgz`), and verify that the source release builds correctly (may with different Java version and Maven version), you can run the following commands:

```bash
mvn clean package -DskipTests
```

## Verifying LICENSE/NOTICE

Unzip the source release archive, and verify that:

1. Check the LICENSE and NOTICE files are correct.
2. All files have ASF license headers if necessary.
3. All dependencies must be checked for their license and the license must be ASL 2.0 compatible (http://www.apache.org/legal/resolved.html#category-x)
4. Compatible non-ASL 2.0 licenses should be contained in the `META-INF/licenses` directory of the respective module
5. The LICENSE and NOTICE files in the root directory refer to dependencies in the source release, i.e., files in the git repository (such as fonts, css, JavaScript, images)


## Testing Against Staged Maven Artifacts

Update the root `pom.xml` of the maven project (like the apache/fluss project) to include the staged repository in the `<repositories>` section. You can do this by adding a new repository entry like this:

```xml
<repositories>
    <repository>
        <id>fluss-shaded-staging</id>
        <name>Temporary Staging Repo</name>
        <url>https://repository.apache.org/content/repositories/orgapachefluss-${STAGED_REPO_ID}/</url>
    </repository>
</repositories>
```

And then you can use the staged maven artifacts as dependencies in the project and verify the new dependencies work.

## Voting

Votes are cast by replying on the vote email on the dev mailing list, with either +1, 0, -1.

In addition to your vote, it’s customary to specify if your vote is binding or non-binding. Only members of the PPMC and mentors have formally binding votes, and IPMC on the vote on the Incubator general mailing list. If you’re unsure, you can specify that your vote is non-binding. You can find more details on https://www.apache.org/foundation/voting.html.

Besides, it is recommended to include a list of checklist you have verified for your vote. This helps the community to understand what you have checked and what is still missing.
