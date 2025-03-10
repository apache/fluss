---
sidebar_label: Review Pull Requests
sidebar_position: 1
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# How to Review a Pull Request

This guide is for all committers and contributors that want to help with reviewing code contributions. Thank you for your effort - good reviews are one of the most important and crucial parts of an open source project. This guide should help the community to make reviews such that:

* Contributors have a good contribution experience.
* Our reviews are structured and check all important aspects of a contribution.
* We make sure to keep a high code quality in Fluss.
* We avoid situations where contributors and reviewers spend a lot of time refining a contribution that gets rejected later.

## Review Checklist

Every review needs to check the following six aspects. **We encourage to check these aspects in order, to avoid
spending time on detailed code quality reviews when formal requirements are not met or there is no consensus in
the community to accept the change.**

### 1. Is the Contribution Well-Described?

Check whether the contribution is sufficiently well-described to support a good review. Trivial changes and fixes
do not need a long description. If the implementation is exactly according to a prior discussion on issue or the
development mailing list, only a short reference to that discussion is needed.

If the implementation is different from the agreed approach in the consensus discussion, a detailed description of
the implementation is required for any further review of the contribution.

### 2. Does the Contribution Need Attention from some Specific Committers?

Some changes require attention and approval from specific committers.

If the pull request needs specific attention, one of the tagged committers/contributors should give the final approval.

### 3. Is the Overall Code Quality Good, Meeting Standard we Want to Maintain in Fluss?

- Does the code follow the right software engineering practices? Is the code correct, robust, maintainable, testable?
- Are the changes performance aware, when changing a performance sensitive part?
- Are the changes sufficiently covered by tests? Are the tests executing fast?
- If dependencies have been changed, were the NOTICE files updated?

Code guidelines can be found in the [Flink Java Code Style and Quality Guide](https://flink.apache.org/how-to-contribute/code-style-and-quality-java/).

### 4. Are the documentation updated?

If the pull request introduces a new feature, the feature should be documented.
