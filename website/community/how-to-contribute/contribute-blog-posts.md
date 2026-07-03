---
title: Contribute Blog Posts
sidebar_position: 4
---

# Contribute Blog Posts

The [Apache Fluss blog](https://fluss.apache.org/blog) is a space for the community to share use cases, deep dives, release announcements, and project updates. Blog posts are authored by community members and reviewed by committers before being published.

This guide walks you through submitting a blog post, from proposing the topic to getting it live on the site.

Blog content lives in its own repository, [apache/fluss-blog](https://github.com/apache/fluss-blog), which is a Docusaurus 3 site built and deployed separately from the main documentation.

## Propose Your Topic First

Before writing, open an issue in [apache/fluss-blog](https://github.com/apache/fluss-blog/issues) describing the post you would like to write. Include:

- A working title.
- A short summary (a few sentences) of what you plan to cover.
- Whether the post is a tutorial, case study, deep dive, release note, or something else.
- A target length and any code samples or diagrams you plan to include.

Getting feedback early avoids two people writing the same post and gives committers a chance to flag overlap with upcoming announcements.

## Fork and Clone the Blog Repository

```bash
git clone https://github.com/<your-username>/fluss-blog.git
cd fluss-blog
npm install
npm run start
```

The development server runs at `http://localhost:3000` with hot reload, so edits show up immediately.

## Repository Layout

```
├── blog/                    # Blog content
│   ├── YYYY-MM-DD-slug.md   # Blog posts (Markdown or MDX)
│   ├── assets/              # Post-specific images and media
│   ├── releases/            # Release announcement posts
│   ├── static/              # Blog-related static files (for example, avatars)
│   ├── authors.yml          # Author profiles
│   └── tags.yml             # Tag definitions
├── static/                  # Global static assets (logo, favicon)
├── src/css/                 # Custom CSS
├── docusaurus.config.ts     # Site configuration
└── package.json
```

## Write the Post

### 1. Create the Markdown file

Add a new file under `blog/` using this naming convention:

```
blog/YYYY-MM-DD-my-post-slug.md
```

Use the date you expect the post to be merged and a short, URL-safe slug.

### 2. Add frontmatter

Every post must start with YAML frontmatter:

```yaml
---
slug: my-post-slug
title: "My Blog Post Title"
date: YYYY-MM-DD
authors: [your_key]
tags: [apache-fluss]
image: ./assets/my_post/banner.png
---
```

- `slug` — URL path for the post (for example, `/blog/my-post-slug`).
- `authors` — List of author keys defined in `blog/authors.yml`.
- `tags` — List of tag keys defined in `blog/tags.yml`.
- `image` — Optional cover image used for social sharing (Open Graph previews).

### 3. Add images

Place post-specific images in `blog/assets/<post_name>/` and reference them with relative paths from the post:

```markdown
![My Diagram](assets/my_post/diagram.png)
```

Keep images reasonably sized (compressed PNG or SVG) so the site stays fast to load.

### 4. Add yourself as an author

If this is your first post, add an entry to `blog/authors.yml`:

```yaml
your_key:
  name: Your Name
  title: Your Title
  url: https://github.com/your-github
  image_url: /avatars/your-avatar.png
```

Place your avatar image in `blog/static/avatars/`.

### 5. Register new tags

If your post uses a tag that does not yet exist, define it in `blog/tags.yml`:

```yaml
my-new-tag:
  label: 'My New Tag'
```

Reuse existing tags when you can; only add new ones when none of them fit.

## Preview and Build Locally

```bash
# Production build
npm run build

# Preview the production build locally
npm run serve
```

Run the production build before opening a pull request; the Docusaurus build catches broken links, missing frontmatter fields, and other issues the dev server does not.

## Submitting Your Post

1. Commit your changes with a descriptive message:

```bash
git add .
git commit -m "blog: add post on <topic>"
```

2. Push to your fork:

```bash
git push origin your-branch-name
```

3. Open a pull request against [apache/fluss-blog](https://github.com/apache/fluss-blog), linking the topic issue you opened earlier.

## Review and Publication

Committers review the post for technical accuracy, tone, and fit with the broader project narrative. Expect light copy edits; substantive feedback comes first in the linked issue.

Once the pull request merges into `main`, a CI pipeline automatically builds and publishes the new content to the [Apache Fluss website](https://fluss.apache.org/blog). No manual deploy step is required.
