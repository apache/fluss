---
title: Contribute Documentation
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

# Contribute Documentation

The Fluss website serves as the primary interface between the project and its community, with several key purposes:

* Introducing Fluss as a streaming storage solution for real-time analytics
* Providing technical documentation for users and developers
* Facilitating downloads and deployment guidance
* Fostering community engagement and contributions
* Showcasing Fluss features, use cases, and roadmap

This website is built with Docusaurus, a modern static website generator optimized for documentation. For advanced features and tools, visit the [official Docusaurus documentation](https://docusaurus.io/).

This guide will walk you through the process of contributing to the Fluss documentation, from setting up your environment to submitting your changes.

## Build Documentation

### Fork and Clone
Fork the [Fluss repository](https://github.com/alibaba/fluss)
    
``` bash
git clone https://github.com/<your-username>/fluss.git
cd fluss
```

### Create a Branch
```bash
git checkout -b feature-branch-name
```

### Install Dependencies
```bash
cd website
npm install
```

### Directory Structure
The Fluss documentation is organized as follows:
```
fluss/website/
├── docs/                    # Main documentation (current version)
│   ├── intro.md
│   ├── concepts/
│   ├── quickstart/
│   └── ...
├── blog/                    # Release and feature blogs (not versioned)
├── community/               # Community content (not versioned)
├── static/                  # Static assets
│   └── img/                 # Images for documentation
├── versioned_docs/          # Previous versions of documentation
│   ├── version-0.6/
│   └── version-0.5/
├── versioned_sidebars/      # Sidebar configurations for different versions
├── src/                     # Source files for the website
│   └── plugins/             # Custom plugins including version replacement
├── build_versioned_docs.sh  # Script for building versioned docs
├── docusaurus.config.ts     # Main configuration for Docusaurus website
├── sidebars.ts              # Sidebar configuration
└── package.json             # Node.js dependencies and scripts
```

### Validation (including versioned docs)
- Edit or create Markdown files in the appropriate directories
- Generate versioned docs (simulates what CI does)
```bash
./build_versioned_docs.sh
```
- Then build the complete site which checks broken links
``` bash
npm run build -- --no-minify
```
  
### Preview Changes
- Start the local development server to preview changes
```bash
npm run start
```
- View your changes at [http://localhost:3000/fluss-docs/](http://localhost:3000/fluss-docs/)

## Versioned Documentation
- Only content in `website/docs/* `is versioned
- Each version corresponds to a branch (e.g., release-0.5, release-0.6)
- Fixes for `website/docs/*` documentation related to old versions should be backported to their corresponding release-x.y branches
- The main branch contains the latest (unreleased) documentation
- Other pages (community, roadmap) are only maintained in the main branch

### Updating Existing Versions
- Checkout the corresponding release branch
``` bash
git checkout release-0.5
```
- Make changes to files in `website/docs/`
- Create a pull request to that specific release branch

### Version Expressions
Fluss documentation uses placeholder variables that are automatically replaced during the build process. Note that these variables are only available for documentation under `website/docs/*`.
- `$FLUSS_VERSION$`: Expands to the full version (e.g., "0.6.0")
- `$FLUSS_VERSION_SHORT$`: Expands to the short version (e.g., "0.6")

For example, to link to a specific version of Fluss binary:

```bash
tar -xzf fluss-$FLUSS_VERSION$-bin.tgz
```

The above code block will be rendered as follows for 0.6 version:

```bash
tar -xzf fluss-0.6.0-bin.tgz
```

## Documentation Linking

### Do's
- Use file paths with .md extensions
``` markdown
[Table Design](table-design/overview.md)
```

- Use paths relative to the docs/ directory
``` markdown
[Flink Engine](engine-flink/getting-started.md)
```


### Don'ts
- Use absolute links like `/docs/` or `/blog/`
- Use URLs without file extensions

### Link Validation
- Pull request CI checks for broken links using `.github/workflows/docs-check.yaml`
- Fix any broken links identified in the CI build
- Test locally with `npm run build` in the website directory

## Submitting Your Contribution
1. Commit your changes with a descriptive message:
```bash
git add .
git commit -m "[docs]: add/update documentation for feature..."
```
2. Push to your fork:
```bash
git push origin feature-branch-name
```
3. Create a pull request on GitHub from your branch to the appropriate branch
- For latest docs: target the main branch
- For version-specific changes: target the specific release-x.y branch

## Deployment Process
- The `.github/workflows/docs-deploy.yaml` workflow automatically deploys documentation
- Triggers when changes are pushed to `website/*` in any `release-*` branches or `main`
- Runs `build_versioned_docs.sh` to collect versioned docs from all release branches
