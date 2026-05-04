---
title: Contribute Blog Posts
sidebar_position: 2
---

# Contribute Blog Posts

Fluss uses blog posts to announce releases, explain new technical capabilities, publish design deep-dives, and share community stories.

The website itself lives in the main [apache/fluss](https://github.com/apache/fluss) repository, but blog content is maintained in a separate repository: [apache/fluss-blog](https://github.com/apache/fluss-blog).

This guide explains the simplest contribution flow for blog posts.

## When to Write a Blog Post

Blog posts are a good fit for:

- release announcements
- deep dives on new features or design choices
- tutorials and getting-started walkthroughs
- production stories and user adoption writeups
- ecosystem updates, integrations, and contributor spotlights

If you are unsure whether an idea belongs in the blog, open a discussion first on [GitHub Discussions](https://github.com/apache/fluss/discussions) or ask on the [developer mailing list](../welcome.mdx#mailing-lists).

## Repository Layout

The [apache/fluss-blog](https://github.com/apache/fluss-blog) repository contains the following important paths:

```text
blog/
  authors.yml
  tags.yml
  YYYY-MM-DD-my-post.md
  releases/
  assets/
  static/
```

- `blog/`: the actual post files
- `blog/releases/`: release announcement posts
- `blog/assets/`: post-specific diagrams and screenshots
- `blog/authors.yml`: author metadata
- `blog/tags.yml`: tag definitions
- `blog/static/`: shared blog assets such as avatar images

## Contribution Flow

### 1. Discuss the post idea

For release posts or larger technical articles, start by sharing the topic and rough outline with the community. This helps avoid duplicate work and makes it easier to get feedback on scope and timing.

### 2. Fork and clone the blog repository

```bash
git clone https://github.com/<your-username>/fluss-blog.git
cd fluss-blog
```

### 3. Create a branch

```bash
git checkout -b docs/blog-post-topic
```

### 4. Create the post file

Create a new Markdown or MDX file under `blog/` using the `YYYY-MM-DD-slug.md` naming pattern:

```text
blog/2026-05-04-my-post-slug.md
```

Release announcements usually live under `blog/releases/`.

### 5. Add frontmatter

Every post should begin with frontmatter similar to:

```yaml
---
slug: my-post-slug
title: "My Blog Post Title"
date: 2026-05-04
authors: [your-author-key]
tags: [apache-fluss]
image: ./assets/my-post/banner.png
---
```

Use the `authors` and `tags` keys that are defined in `blog/authors.yml` and `blog/tags.yml`.

### 6. Add media and author metadata

- Put post-specific diagrams and screenshots in `blog/assets/<post-name>/`
- If you are a new author, add yourself to `blog/authors.yml`
- Add your avatar image to `blog/static/avatars/` if needed
- Register any new tags in `blog/tags.yml`

## Writing Guidelines

Try to keep the post:

- technically accurate
- concrete about the user problem or design tradeoff
- consistent with Apache project tone
- easy to scan with headings, code snippets, and diagrams where useful

Good posts usually explain:

1. what problem is being solved
2. why the change matters
3. how it works
4. how readers can try it themselves

## Preview the Blog Locally

From the `fluss-blog` repository:

```bash
npm install
npm run start
```

This starts a local Docusaurus preview so you can check formatting, links, and images before opening a pull request.

## Submit the Pull Request

Once the post is ready:

1. commit the changes with a descriptive message
2. push the branch to your fork
3. open a pull request against `apache/fluss-blog`

In the pull request description, include:

- the audience for the post
- whether it is time-sensitive
- any images or assets that reviewers should pay special attention to

## How This Connects to the Main Website

The main website repository can pull blog content into local previews through `website/setup_blog.sh`. That is useful when you want to check the post in the full multi-version website context.
