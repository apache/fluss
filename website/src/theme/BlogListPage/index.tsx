/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, {useMemo, useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import useIsBrowser from '@docusaurus/useIsBrowser';
import {useHistory, useLocation} from '@docusaurus/router';
import Layout from '@theme/Layout';
import SearchMetadata from '@theme/SearchMetadata';
import BlogListPageStructuredData from '@theme/BlogListPage/StructuredData';
import type {Props} from '@theme/BlogListPage';

import styles from './styles.module.css';

type Post = Props['items'][number]['content'];
type Tag = Post['metadata']['tags'][number];
const PAGE_SIZE = 9;
const TAG_ORDER = ['Announcement', 'Case Study', 'Engineering', 'Guides'];
const dateFormat = new Intl.DateTimeFormat('en', {
  month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC',
});

function PostImage({post, featured = false}: {post: Post; featured?: boolean}): React.JSX.Element {
  const [failed, setFailed] = useState(false);
  const cover = post.assets.image ?? post.metadata.frontMatter.image;
  const imageUrl = useBaseUrl(cover ?? '/img/logo/svg/white_color_logo.svg');
  const logoUrl = useBaseUrl('/img/logo/svg/white_color_logo.svg');
  const hasCover = Boolean(cover) && !failed;

  return (
    <Link to={post.metadata.permalink} className={clsx(styles.imageLink, !hasCover && styles.fallback)}
      tabIndex={-1} aria-hidden="true">
      <img src={hasCover ? imageUrl : logoUrl} alt="" loading={featured ? 'eager' : 'lazy'}
        decoding="async" onError={hasCover ? () => setFailed(true) : undefined} />
      {!hasCover && <span className={styles.fallbackLabel}>THE FLUSS BLOG</span>}
    </Link>
  );
}

function PostMeta({post}: {post: Post}): React.JSX.Element {
  const {authors, date} = post.metadata;
  return (
    <div className={styles.meta}>
      <span>
        {authors.map((author, index) => (
          <React.Fragment key={index}>
            {index > 0 && ', '}
            {author.url
              ? <Link href={author.url}>{author.name}</Link>
              : author.name}
          </React.Fragment>
        ))}
      </span>
      {authors.length > 0 && <span aria-hidden="true">·</span>}
      <time dateTime={date}>{dateFormat.format(new Date(date))}</time>
    </div>
  );
}

function PostTags({tags, onSelect}: {tags: readonly Tag[]; onSelect: (label: string) => void}): React.JSX.Element {
  return (
    <div className={styles.tags}>
      {tags.map((tag) => (
        <button type="button" key={tag.permalink} onClick={() => onSelect(tag.label)}
          aria-label={`Filter by ${tag.label}`} className={styles.tag}>
          {tag.label}
        </button>
      ))}
      {tags.length === 0 && <span className={styles.tag}>Article</span>}
    </div>
  );
}

function PostCard({post, featured = false, onSelect}: {
  post: Post; featured?: boolean; onSelect: (label: string) => void;
}): React.JSX.Element {
  const {title, permalink, description, tags} = post.metadata;
  const Heading = featured ? 'h2' : 'h3';
  return (
    <article className={clsx(styles.card, featured && styles.featured)}>
      <PostImage post={post} featured={featured} />
      <div className={styles.cardBody}>
        <Heading className={styles.cardTitle}><Link to={permalink}>{title}</Link></Heading>
        <PostMeta post={post} />
        <p className={styles.description}>{description}</p>
        <PostTags tags={tags} onSelect={onSelect} />
      </div>
    </article>
  );
}

function PostGrid({posts, selectedTag, onSelect}: {
  posts: Post[]; selectedTag: string; onSelect: (label: string) => void;
}): React.JSX.Element {
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);
  return (
    <section aria-label={selectedTag ? `${selectedTag} articles` : 'All articles'} className={styles.archive}>
      <div className={styles.archiveHeader}>
        <p role="status">{posts.length} {posts.length === 1 ? 'article' : 'articles'}</p>
      </div>
      <div className={styles.grid}>
        {posts.slice(0, visibleCount).map((post) => (
          <PostCard key={post.metadata.permalink} post={post} onSelect={onSelect} />
        ))}
      </div>
      {posts.length === 0 && <p className={styles.empty}>No articles with this tag. <button type="button" onClick={() => onSelect('')}>View all articles</button></p>}
      {visibleCount < posts.length && (
        <div className={styles.loadMore}>
          <button type="button" onClick={() => setVisibleCount((count) => count + PAGE_SIZE)}>
            Load more articles <span aria-hidden="true">↓</span>
          </button>
          <span>Showing {Math.min(visibleCount, posts.length)} of {posts.length}</span>
        </div>
      )}
    </section>
  );
}

export default function BlogListPage(props: Props): React.JSX.Element {
  const history = useHistory();
  const location = useLocation();
  const isBrowser = useIsBrowser();
  const selectedTag = isBrowser ? new URLSearchParams(location.search).get('tag') ?? '' : '';
  const posts = useMemo(() => props.items.map(({content}) => content)
    .sort((a, b) => Date.parse(b.metadata.date) - Date.parse(a.metadata.date)), [props.items]);
  const tags = useMemo(() => {
    const byLabel = new Map<string, {label: string; count: number}>();
    posts.forEach((post) => post.metadata.tags.forEach(({label}) => {
      const tag = byLabel.get(label) ?? {label, count: 0};
      tag.count += 1;
      byLabel.set(label, tag);
    }));
    return [...byLabel.values()].sort((a, b) => {
      const aIndex = TAG_ORDER.indexOf(a.label);
      const bIndex = TAG_ORDER.indexOf(b.label);
      return (aIndex < 0 ? TAG_ORDER.length : aIndex)
        - (bIndex < 0 ? TAG_ORDER.length : bIndex)
        || a.label.localeCompare(b.label);
    });
  }, [posts]);

  function selectTag(label: string): void {
    const params = new URLSearchParams(location.search);
    if (label) {
      params.set('tag', label);
    } else {
      params.delete('tag');
    }
    history.push({...location, search: params.toString() ? `?${params}` : ''});
  }

  const filteredPosts = selectedTag
    ? posts.filter((post) => post.metadata.tags.some(({label}) => label === selectedTag))
    : posts.slice(1);

  return (
    <Layout title="Blog" description={props.metadata.blogDescription}>
      <SearchMetadata tag="blog_posts_list" />
      <BlogListPageStructuredData {...props} />
      <main className={styles.page}>
        <header className={styles.pageHeader}>
          <div>
            <h1>The Fluss Blog<span>.</span></h1>
            <p>Engineering, ideas, and stories from the streaming frontier.</p>
          </div>
        </header>

        {posts[0] && <PostCard post={posts[0]} featured onSelect={selectTag} />}

        <div className={styles.filters} role="group" aria-label="Filter articles by tag">
          <button type="button" className={clsx(styles.filter, !selectedTag && styles.activeFilter)}
            aria-pressed={!selectedTag} onClick={() => selectTag('')}>
            All posts <span>{posts.length}</span>
          </button>
          {tags.map(({label, count}) => (
            <button type="button" key={label}
              className={clsx(styles.filter, selectedTag === label && styles.activeFilter)}
              aria-pressed={selectedTag === label} onClick={() => selectTag(label)}>
              {label} <span>{count}</span>
            </button>
          ))}
        </div>

        <PostGrid key={selectedTag} posts={filteredPosts} selectedTag={selectedTag} onSelect={selectTag} />
      </main>
    </Layout>
  );
}
