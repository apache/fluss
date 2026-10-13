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

# Fluss Website

This website is built using [Docusaurus](https://docusaurus.io/), a modern static website generator.

### Requirements

- [Node.js](https://nodejs.org/en/download/) version 20.0 or above (which can be checked by running `node -v`). You can use [nvm](https://github.com/nvm-sh/nvm) for managing multiple Node versions on a single machine installed.
  - When installing Node.js, you are recommended to check all checkboxes related to dependencies.


### Installation

```bash
npm install
```

### Local Development (Current Version Only)

To preview only the **current development version** of the docs (i.e. `docs/` on your local branch), without any versioned docs or blog:

```bash
npm run start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

> **Note:** This mode does **not** include versioned docs or blog content. The version dropdown in the navbar will only show the `Next` version.

### Local Preview with Multi-Version Docs and Blog (Optional)

To preview the full website locally with **all released versioned docs** and **blog posts**, you need to run the following setup scripts before building.

#### 1. Build Versioned Docs

This script clones the remote repository, checks out each `release-x.y` branch, and copies their `website/docs/` into `versioned_docs/`. It also generates `versions.json` and versioned sidebar files.

```bash
bash build_versioned_docs.sh
```

This may take a few minutes as it clones the full repository. After completion, you will see:
- `versioned_docs/version-x.y/` directories for each release
- `versioned_sidebars/version-x.y-sidebars.json` files
- An updated `versions.json`

#### 2. Set Up Blog Content

Blog posts are maintained in a separate repository ([apache/fluss-blog](https://github.com/apache/fluss-blog)). This script clones it and places the `blog/` directory into `website/blog/`.

```bash
bash setup_blog.sh
```

You can customize the blog source using environment variables:

```bash
# Use a different repo or branch
BLOG_REPO=https://github.com/<your-fork>/fluss-blog.git BLOG_BRANCH=my-branch bash setup_blog.sh
```

> **Note:** If `blog/` already exists, the script will automatically remove it and re-clone the latest content.

### Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

Using SSH:

```bash
USE_SSH=true npm run deploy
```

Not using SSH:

```bash
GIT_USER=<Your GitHub username> npm run deploy
```

If you are using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.
