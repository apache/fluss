---
title: Creating a Fluss Shaded Release
sidebar_position: 2
---

# Creating a Fluss Shaded Release

:::note
This is based on the release guide of the [Apache Flink project](https://cwiki.apache.org/confluence/display/FLINK/Creating+a+Flink+Release).
:::

The Apache Fluss (Incubating) project periodically declares and publishes releases. A release is one or more packages of the project artifact(s) that are approved for general public distribution and use. The Fluss community treats releases with great importance. They are a public face of the project and most users interact with the project only through the releases. Releases are signed off by the entire Fluss community in a public vote.

Each release is executed by a Release Manager, who is selected/proposed by the Fluss PPMC members. This document describes the process that the Release Manager follows to perform a release. Any changes to this process should be discussed and adopted on the dev@ mailing list.

Please remember that publishing software has legal consequences. This guide complements the foundation-wide [Product Release Policy](https://www.apache.org/legal/release-policy.html) and [Release Distribution Policy](https://infra.apache.org/release-distribution.html).

## Overview

![](../assets/release-guide.png)

The release process consists of several steps:

1. [Decide to release](#decide-to-release)
2. [Prepare for the release](#prepare-for-the-release)
3. [Build a release candidate](#build-a-release-candidate)
4. [Vote on the release candidate](#vote-on-the-release-candidate)
5. [If necessary, fix any issues and go back to step 3](#fix-any-issues)
6. [Finalize the release](#finalize-the-release)
7. [Promote the release](#promote-the-release)

## Decide to release

Deciding to release and selecting a Release Manager is the first step of the release process. This is a consensus-based decision of the entire community.

Anybody can propose a release on the dev@ mailing list, giving a solid argument and nominating a committer as the Release Manager (including themselves). There’s no formal process, no vote requirements, and no timing requirements. Any objections should be resolved by consensus before starting the release.

In general, the community prefers to have a rotating set of 3-5 Release Managers. Keeping a small core set of managers allows enough people to build expertise in this area and improve processes over time, without Release Managers needing to re-learn the processes for each release. That said, if you are a committer interested in serving the community in this way, please reach out to the community on the dev@ mailing list.

**Checklist to proceed to the next step**
- Community agrees to release
- Community selects a Release Manager

## Prepare for the release

#### 0. One-time Release Manager setup

Before your first release, you should perform one-time configuration steps. This will set up your security keys for signing the release and access to various release repositories. Please check the **[Release Manager Preparation](release-manager-preparation.md)** guide for details.

To prepare for each release, you should audit the project status in the GitHub issue tracker, and do necessary bookkeeping. Finally, you should create a release branch from which individual release candidates will be built.

#### 1. Setup environment variables

Set up a few environment variables to simplify commands that follow. (We use bash Unix syntax in this guide.)

```bash
RELEASE_VERSION="1.0-incubating"
RELEASE_VERSION_SHORT="1.0"
NEXT_VERSION="2.0-incubating"
```

#### 2. Verify Java and Maven Version

All of the following steps require to use **Maven 3.8.6** and **Java 8**. Modify your `PATH` environment variable accordingly if needed.

#### 3. Clone fluss-shaded into a fresh workspace

Clone the Fluss repository into a fresh workspace. This is important to ensure that you are working with a clean state.

```bash
git clone git@github.com:apache/fluss-shaded.git
```

#### 4. Create a release branch

Create a release branch from the `main` branch. This branch will be used to build the release candidate.

```bash
$ cd tools
tools $ git checkout main
tools $ git checkout -b release-${RELEASE_VERSION_SHORT}
tools $ git push origin release-${RELEASE_VERSION_SHORT}
```

#### 5. Bump version for the main branch

After creating the release branch, you should bump the version of the main branch to the next version. This is important to ensure that the next development cycle starts with the correct version.

```bash
tools $ git checkout main
tools $ OLD_VERSION=$RELEASE_VERSION NEW_VERSION=$NEXT_VERSION releasing/update_branch_version.sh
tools $ git push origin main
```

The newly created branch and updated master branch need to be pushed to the official repository.

**Checklist to proceed to the next step**

- Release Manager’s GPG key is published to [dist.apache.org](https://dist.apache.org/repos/dist/release/incubator/fluss/KEYS)
- Release Manager’s GPG key is configured in git configuration
- Release Manager's GPG key is configured as the default gpg key.
- Release Manager has `org.apache.fluss` listed under `Staging Profiles` in Nexus
- Release Manager’s Nexus User Token is configured in `settings.xml`
- There are no release blocking GitHub issues
- Release branch (`release-x.0`) has been created and pushed
- Main branch has been updated to the next version and pushed

## Build a release candidate

The core of the release process is the build-vote-fix cycle. Each cycle produces one release candidate. The Release Manager repeats this cycle until the community approves one release candidate, which is then finalized.

### 1. Create RC branch and tag

Set up a few environment variables to simplify Maven commands that follow. This identifies the release candidate being built. Start with `RC_NUM` equal to 1 and increment it for each candidate.

```bash
RC_NUM="1"
TAG="v${RELEASE_VERSION}-rc${RC_NUM}"
```

Now, checkout from the release branch, and create a release candidate local branch:

```bash
$ git checkout release-${RELEASE_VERSION_SHORT}
$ cd tools
tools $ RELEASE_CANDIDATE=$RC_NUM  RELEASE_VERSION=$RELEASE_VERSION releasing/create_release_branch.sh
```

Tag the release commit:

```bash
git tag -s ${TAG} -m "${TAG}"
```

### 2. Stage source release files

First, we build the source release:

```bash
tools $ RELEASE_VERSION=$RELEASE_VERSION releasing/create_source_release.sh
```

This command creates a source tarball and signs it under `tools/release` directory.

Next, copy the source release files to the dev repository of dist.apache.org.

(1) If you have not already, check out the Fluss section of the dev repository on dist.apache.org via Subversion. In a fresh directory (e.g., `tools/target`):

```bash
tools $ mkdir target
tools $ cd target
tools/target $ svn checkout https://dist.apache.org/repos/dist/dev/incubator/fluss --depth=immediates
```

(2) Make a directory for the new release:

```bash
mkdir fluss/fluss-shaded-${RELEASE_VERSION}-rc${RC_NUM}
```

(3) Copy fluss-shaded source distributions, hashes, and GPG signature:

```bash
tools/target $ mv ../release/* fluss/fluss-shaded-${RELEASE_VERSION}-rc${RC_NUM}
```

(4) Add and commit all the files.

```bash
cd fluss
svn add fluss-shaded-${RELEASE_VERSION}-rc${RC_NUM}
svn commit -m "Add fluss-shaded-${RELEASE_VERSION}-rc${RC_NUM}"
```

(5) Verify the files are present: https://dist.apache.org/repos/dist/dev/incubator/fluss/


### 3. Stage maven artifacts


Next, we stage the maven artifacts:

```bash
tools $ releasing/deploy_staging_jars.sh
```

Review all staged artifacts in the staging repositories(https://repository.apache.org/#stagingRepositories). They should contain all relevant parts for each module, including pom.xml, jar, test jar, source, test source, javadoc, etc. Carefully review any new artifacts. (fluss-shaded doesn’t deploy test jar, source jar, and javadoc jar, see the [issue](https://github.com/apache/fluss-shaded/blob/main/README.md#sources) for more details)

Close the staging repository on Apache Nexus. When prompted for a description, enter `Apache Fluss (Incubating), version X, release candidate Y`. You can find the staging repository URL (`https://repository.apache.org/content/repositories/orgapachefluss-[STAGING_ID]/`) once the staging repository is closed successfully.

![](../assets/nexus-staging.png)


### 4. Push the RC tag

```bash
git push <remote> refs/tags/{$TAG}
```


**Checklist to proceed to the next step**

- Maven artifacts deployed to the staging repository of [repository.apache.org](https://repository.apache.org/content/repositories/)
- Source distribution deployed to the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/incubator/fluss/)
- RC tag pushed to the [official repository](https://github.com/apache/fluss-shaded/tags)


## Vote on the release candidate

Once you have built and individually reviewed the release candidate, please share it for the community-wide review. Please review foundation-wide [voting guidelines](https://www.apache.org/foundation/voting.html) for more information.

Fluss is an incubating project, so the release requires a [two-phase vote](https://incubator.apache.org/cookbook/#two_phase_vote_on_podling_releases), first by the Fluss community, and then by the Incubator PMC.

### Fluss Community Vote

Start the review-and-vote thread on the dev@ mailing list. Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: dev@fluss.apache.org
Subject: [VOTE] Release fluss-shaded 1.0-incubating (RC3)

Hi everyone,

Please review and vote on the release candidate #3 for the fluss-shaded version 1.0-incubating, as follows:
[ ] +1, Approve the release
[ ] -1, Do not approve the release (please provide specific comments)


The complete staging area is available for your review, which includes:
* the official Apache source release and binary convenience releases to be deployed to dist.apache.org [1], which are signed with the key with fingerprint FFFFFFFF [2],
* all artifacts to be deployed to the Maven Central Repository [3],
* source code tag "release-1.0-incubating-rc3" [4],

The vote will be open for at least 72 hours. It is adopted by majority approval, with at least 3 PPMC affirmative votes.

Thanks,
Release Manager

[1] link
[2] https://dist.apache.org/repos/dist/release/incubator/fluss/KEYS
[3] link
[4] link
```

**If there are any issues found in the release candidate**, reply on the vote thread to cancel the vote. There’s no need to wait 72 hours. Proceed to the [Fix Issues](#fix-any-issues) step below and address the problem. However, some issues don’t require cancellation. For example, if an issue is found in the website pull request, just correct it on the spot and the vote can continue as-is.

For cancelling a release, the release manager needs to send an email to the release candidate thread, stating that the release candidate is officially cancelled. Next, all artifacts created specifically for the RC in the previous steps need to be removed:

- Delete the staging repository in Nexus
- Remove the source / binary RC files from dist.apache.org
- Delete the source code tag in git

**If there are no issues**, reply on the vote thread to close the voting. Then, tally the votes in a separate email. Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: dev@fluss.apache.org
Subject: [RESULT][VOTE] Release fluss-shaded 1.0-incubating (RC3)

I'm happy to announce that we have unanimously approved this release.

There are XXX approving votes, XXX of which are binding:
* approver 1
* approver 2
* approver 3
* approver 4

There are no disapproving votes.

Thanks everyone!

A new vote is starting in the Apache Incubator general mailing list.
```

### Incubator PMC Vote

Once the Fluss community has approved the release candidate, the release manager should start a vote on the general@incubator.apache.org mailing list to get the approval from the Incubator PMC. Here’s an email template; please adjust as you see fit.

```
From: Release Manager
To: general@incubator.apache.org
Subject: [VOTE] Release Apache Fluss Shaded 1.0-incubating (RC3)

Hi everyone,

The Apache Fluss community has voted and approved the release of
Apache Fluss Shaded 1.0-incubating (rc3). We now kindly request the IPMC
members to review and vote for this release.

Apache Fluss (Incubating) - A streaming storage built for real-time analytics which can serve as the real-time data layer for Lakehouse architectures.

Fluss community vote thread:
* https://lists.apache.org/thread/<VOTE THREAD>

Vote result thread:
* https://lists.apache.org/thread/<VOTE RESULT>

The release candidate:
* https://dist.apache.org/repos/dist/dev/incubator/fluss/fluss-shaded-1.0-incubating-rc3/

Git tag for the release:
* https://github.com/apache/fluss-shaded/releases/tag/v1.0-incubating-rc3

Git commit for the release:
* https://github.com/apache/fluss-shaded/commit/<COMMIT>

Maven staging repository:
* https://repository.apache.org/content/repositories/orgapachefluss-<ID>/

The artifacts signed with PGP key [FFFFFFFF], corresponding to
[jark@apache.org], that can be found in keys file:
https://downloads.apache.org/incubator/fluss/KEYS

Please download, verify and test.

Please vote in the next 72 hours.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about Apache Fluss, please see https://fluss.apache.org/

Checklist for reference:

[ ] Download links are valid.
[ ] Checksums and signatures.
[ ] LICENSE/NOTICE files exist
[ ] No unexpected binary files
[ ] All source files have ASF headers
[ ] Can compile from source

Thanks!

Regards,
Release Manager
```

Binding votes are the votes from the IPMC members. Similar to the previous vote, send the result on the Incubator general mailing list:

```
From: Release Manager
To: general@incubator.apache.org
Subject: [RESULT][VOTE] Release Apache Fluss Shaded 1.0-incubating (RC3)

I'm happy to announce that we have unanimously approved this release.

There are XXX approving votes, XXX of which are binding:
* approver 1
* approver 2
* approver 3
* approver 4

There are no disapproving votes.

Thanks everyone!
```

**Checklist to proceed to the finalization step**
- Community votes to release the proposed candidate, with at least three approving PPMC votes
- Incubator votes to release the proposed candidate, with at least three approving IPMC votes



## Fix any issues

Any issues identified during the community review and vote should be fixed in this step.

Code changes should be proposed as standard pull requests to the `main` branch and reviewed using the normal contributing process. Then, relevant changes should be **cherry-picked into the release branch**. The cherry-pick commits should then be proposed as the pull requests against the release branch, again reviewed and merged using the normal contributing process.

Once all issues have been resolved, you should go back and build a new release candidate with these changes.

**Checklist to proceed to the next step**
- Issues identified during vote have been resolved, with fixes committed to the release branch.

## Finalize the release

Once the release candidate has been reviewed and approved by the community, the release should be finalized. This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

#### Deploy artifacts to Maven Central Repository

Use the [Apache Nexus](https://repository.apache.org/) repository to release the staged binary artifacts to the Maven Central repository. In the `Staging Repositories` section, find the relevant release candidate `orgapachefluss-XXX` entry and click `Release`. Drop all other release candidates that are not being released.

#### Deploy source and binary releases to dist.apache.org

Copy the source and binary releases from the dev repository to the release repository at dist.apache.org using Subversion.

```bash
svn move -m "Release Fluss Shaded ${RELEASE_VERSION}" https://dist.apache.org/repos/dist/dev/incubator/fluss/fluss-shaded-${RELEASE_VERSION}-rc${RC_NUM} https://dist.apache.org/repos/dist/release/incubator/fluss/fluss-shaded-${RELEASE_VERSION}
```
(Note: Only PPMC members have access to the release repository. If you do not have access, ask on the mailing list for assistance.)

#### Remove old release candidates from dist.apache.org

Remove the old release candidates from https://dist.apache.org/repos/dist/dev/incubator/fluss using Subversion.

```bash
svn checkout https://dist.apache.org/repos/dist/dev/incubator/fluss --depth=immediates
cd fluss
svn remove fluss-shaded-${RELEASE_VERSION}-rc*
svn commit -m "Remove old release candidates for Apache Fluss Shaded ${RELEASE_VERSION}"
```

#### Git tag

Create and push a new Git tag for the released version by copying the tag for the final release candidate, as follows:

```bash
git tag -s "v${RELEASE_VERSION}" refs/tags/${TAG}^{} -m "Release Fluss Shaded ${RELEASE_VERSION}"
git push <remote> refs/tags/v${RELEASE_VERSION}
```

**Checklist to proceed to the next step**
- Maven artifacts released and indexed in the Maven Central Repository (usually takes about a day to show up)
- Source & binary distributions available in the release repository of https://dist.apache.org/repos/dist/release/incubator/fluss/
- Dev repository https://dist.apache.org/repos/dist/dev/incubator/fluss/ is empty
- Release tagged in the source code repository

## Promote the release

Once the release has been finalized, the last step of the process is to promote the release within the project and beyond. Please wait for 24h after finalizing the release in accordance with the [ASF release policy](https://www.apache.org/legal/release-policy.html#release-announcements).

Announce on the dev@ mailing list that the release has been finished.

Announce on the release on the user@ mailing list, listing major improvements and contributions.

Announce the release on the announce@apache.org mailing list.

```
From: Release Manager
To: dev@fluss.apache.org, user@fluss.apache.org, announce@apache.org
Subject: [ANNOUNCE] Apache Fluss Shaded 1.0-incubating released

The Apache Fluss community is very happy to announce the release of Apache Fluss Shaded 1.0-incubating.

Apache Fluss is a streaming storage built for real-time analytics which can serve as the real-time data layer for Lakehouse architectures.

The release is available for download at:
https://downloads.apache.org/incubator/fluss/fluss-shaded-1.0-incubating/

Maven artifacts for Fluss Shaded can be found at:

https://central.sonatype.com/search?q=g%3Aorg.apache.fluss+shade&smo=true

We would like to thank all contributors of the Apache Fluss community who made this release possible!

Feel free to reach out to the release managers (or respond to this thread) with feedback on the release process. Our goal is to constantly improve the release process. Feedback on what could be improved or things that didn't go so well are appreciated.

Regards,
Release Manager
```

#### Recordkeeping

Use [reporter.apache.org](https://reporter.apache.org/addrelease.html?fluss) to seed the information about the release into future project reports.

(Note: Only PMC members have access report releases. If you do not have access, ask on the mailing list for assistance.)

***Checklist to declare the process completed***
- Release announced on the dev@ and user@ mailing list.
- Release recorded in reporter.apache.org.
- Completion declared on the dev@ mailing list.

## Improve the process

It is important that we improve the release processes over time. Once you’ve finished the release, please take a step back and look what areas of this process and be improved. Perhaps some part of the process can be simplified. Perhaps parts of this guide can be clarified.

If we have specific ideas, please start a discussion on the dev@ mailing list and/or propose a pull request to update this guide. Thanks!