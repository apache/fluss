---
title: Contribute FIPs
sidebar_position: 3
---

# Contribute FIPs

Fluss Improvement Proposals (FIPs) are used for substantial features, architectural changes, and other decisions that benefit from a durable design record and broad community review.

If your change is small, self-contained, or limited to local documentation fixes, you usually do **not** need a FIP. For larger work that changes public behavior, introduces new subsystems, or needs cross-component consensus, a FIP is often the right path.

## When to Use a FIP

Consider writing a FIP when the change:

- introduces a major new feature
- changes architecture or long-term project direction
- affects multiple components or user-facing APIs
- needs explicit tradeoff discussion before implementation
- would benefit from a stable document that future contributors can reference

For routine bug fixes, small enhancements, and isolated refactors, follow the normal [Contribute Code](contribute-code.md) process instead.

## Start with Discussion

Before writing a full proposal, start a community discussion:

- open a feature issue in [GitHub Issues](https://github.com/apache/fluss/issues/new?template=feature.yml)
- summarize the motivation, scope, and open questions
- ask for early feedback in [GitHub Discussions](https://github.com/apache/fluss/discussions) or on the [developer mailing list](../welcome.mdx#mailing-lists)

This early step helps validate that the problem is worth solving and avoids spending time on a proposal that is pointed in the wrong direction.

## Where FIPs Live

Published Fluss Improvement Proposals are tracked on the Fluss Confluence space:

- [Fluss Improvement Proposals](https://cwiki.apache.org/confluence/display/FLUSS/Fluss+Improvement+Proposals)

When preparing a new proposal, link the design discussion, implementation pull requests, and any follow-up documentation work back to the relevant FIP entry.

## Recommended Proposal Structure

Even if the exact formatting evolves, a strong FIP usually covers:

1. **Summary**: a short description of the proposal
2. **Motivation**: the user or system problem being solved
3. **Goals and non-goals**: what is in scope and what is intentionally out of scope
4. **Design**: the proposed behavior, architecture, and interfaces
5. **Compatibility and migration**: impact on existing users, upgrades, and defaults
6. **Alternatives considered**: why other options were rejected
7. **Testing and rollout**: how the change will be validated and adopted
8. **Open questions**: anything still needing community input

## Review Process

Use the FIP to drive consensus before or alongside implementation:

- keep design discussion in public channels
- update the proposal when major decisions change
- link implementation pull requests back to the proposal
- avoid merging large implementation work before there is clear consensus on the design

For complex proposals, it is normal to iterate on the document multiple times before code review begins.

## After Consensus

Once the community agrees on the direction:

1. create or update the implementation issue
2. open one or more pull requests in manageable pieces
3. reference the FIP in the pull request description
4. update user-facing docs as part of the implementation rollout

The FIP should remain a durable reference for why the system works the way it does, not just a one-time approval artifact.
