---
title: Contribute Code
sidebar_position: 1
---

# Contribute Code

Fluss is maintained, improved, and extended by code contributions of volunteers. We welcome contributions to Fluss, but due to preserve the high quality of the code base, we follow a contribution process that is explained in this document.

:::warning
Please read this document carefully before starting to work on a code contribution. Follow the process and guidelines explained below. Contributing to Fluss does not start with opening a pull request. We expect contributors to reach out to us first to discuss the overall approach together. Without consensus with the Fluss committers, contributions might require substantial rework or will not be reviewed.
:::

## Looking for what to contribute

- If you have a good idea for the contribution, you can proceed to [the code contribution process](#code-contribution-process).
- If you are looking for what you could contribute, you can browse [open issues](https://github.com/alibaba/fluss/issues), which are not assigned, and then follow [the code contribution process](#code-contribution-process).
- If you are very new to the Fluss project and ready to tackle some open issues, we've collected some [good first issues](https://github.com/alibaba/fluss/contribute) for you.


## Code Contribution Process

### Discuss

Create an issue and reach consensus.

**To request an issue, please note that it is not just a "please assign it to me", you need to explain your understanding of the issue, and your design, and if possible, you need to provide your POC code.**

### Implement

Implement the change according to the Code Style and Quality(refer to the [Flink doc](https://flink.apache.org/how-to-contribute/code-style-and-quality-preamble/) Guide and the approach agreed upon in the issue.

1. Only start working on the implementation if there is consensus on the approach (e.g. you are assigned to the ticket)
2. If you are newer, can refer to [ide setup](/community/dev/ide-setup) to setup a Fluss dev environment.

### Review
Create the pull request and work with the reviewer. 

1. Make sure no unrelated or unnecessary reformatting changes are included.
2. Please ensure that the test passing.
3. Please don't resolve conversation.

### Merge
A committer of Fluss checks if the contribution fulfills the requirements and merges the code to the codebase.

## Pull Request Guide

Considerations before opening a pull request:

- Make sure that the pull request corresponds to a [GitHub issue](https://github.com/alibaba/fluss/issues). Exceptions are made for typos in JavaDoc or documentation files, which need no issue.

- Name the pull request in the format "[component] Title of the pull request", where *[component]* should be replaced by the name of the component being changed. Typically, this corresponds to the component label assigned to the issue (e.g., [kv], [log], [client], [flink]). Skip *[component]* if you are unsure about which is the best component. **Hotfixes** should be named for example `[hotfix][docs] Expand JavaDoc for PunctuatedWatermarkGenerator`.

- Fill out the pull request template to describe the changes contributed by the pull request. Please describe it such that the reviewer understands the problem and solution from the description, not only from the code. That will give reviewers the context they need to do the review.

- Make sure that the change passes the automated tests, i.e., `mvn clean verify` passes.

- Each pull request should address only one issue, not mix up code from multiple issues.


There is a separate guide on [how to review a pull request](how-to-contribute/review-pull-requests.md), including our pull request review process. As a code author, you should prepare your pull request to meet all requirements.

You need to make sure that all GitHub Actions CI checks must pass for a pull request. If there are any failures, it is essential to investigate the exception stack by in the CI logs. Additionally, GitHub Actions automatically collects and uploads Maven logs (logs printed by log4j in the code) to GitHub Artifacts (see the "Artifacts" section in this [build](https://github.com/alibaba/fluss/actions/runs/13761957739)). These logs are particularly valuable for debugging issues that are difficult to reproduce in a local environment.



