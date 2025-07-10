---
title: Bug Reports and Feature Requests
sidebar_position: 1
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Bug Reports and Feature Requests

You can report an issue in the [Fluss issue tracker](https://github.com/apache/fluss/issues).

## Issue Reporting Guide

Considerations before opening an issue:

- Make sure that the issue corresponds to a genuine bug or enhancement request. Exceptions are made for typos in documentation files, which can be reported directly.

- Provide a clear and descriptive title. Use component labels (such as `component=server`, `component=client`, `component=connector`, `component=docs`) to help categorize your issue and make it easier to filter and search.

- Fill out the issue template to describe the problem or enhancement clearly. Please describe it such that maintainers understand the context and impact from the description, not only from reproduction steps.

- Make sure that you have searched existing issues to avoid duplicates.

- Each issue should address only one problem or enhancement, not mix up multiple unrelated topics.

## Looking for what to report

- If you have encountered a bug, you can proceed to [the bug reporting process](https://github.com/apache/fluss/issues/new?template=bug.yml).
- If you have a feature idea, you can proceed to [the feature request process](https://github.com/apache/fluss/issues/new?template=feature.yml).

## How to report a bug

When filing an issue, make sure to answer these five questions:

**Environment:**
1. What version of Fluss are you using?
2. What operating system and processor architecture are you using?

**Steps to reproduce:**
3. What did you do?

**Expected vs Actual behavior:**
4. What did you expect to see?
5. What did you see instead?

Troubleshooting questions should be posted on:
* [Slack (#troubleshooting)](https://join.slack.com/t/fluss-hq/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw)
* [GitHub Discussions](https://github.com/apache/fluss/discussions)

## How to suggest a feature or enhancement

Fluss aims to provide a unified streaming storage for real-time analytics, offering seamless data ingestion, processing, and querying capabilities with strong consistency guarantees and efficient storage management.

If you're looking for a feature that doesn't exist in Fluss, you're probably not alone. Others likely have similar needs. Please open a [GitHub Issue](https://github.com/apache/fluss/issues/new) describing the feature you'd like to see, why you need it, and how it should work.

When creating your feature request, start by clearly documenting your requirements and avoid jumping straight to implementation details. For substantial features or architectural changes that need broader community input, consider submitting a [Fluss Improvement Proposal (FIP)](https://cwiki.apache.org/confluence/display/FLUSS/Fluss+Improvement+Proposals). FIPs offer a formal process to propose, discuss, and document significant project enhancements.
