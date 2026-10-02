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

import type {ProcessBlogPostsFn} from '@docusaurus/plugin-content-blog';
import {createExcerpt} from '@docusaurus/utils';

// Older posts often put a banner at the start of the body without setting
// front matter. Let Docusaurus bundle that image just like an explicit cover.
// Only use a leading image, so diagrams inside an article do not become covers.
export const prepareBlogPosts: ProcessBlogPostsFn = async ({blogPosts}) =>
  blogPosts.map((post) => {
    const body = post.content.trimStart();
    const introduction = body.replace(/^(?:#{1,6}[ \t]+[^\n]*\r?\n\s*)+/, '');
    const banner = introduction.match(/^!\[[^\]]*\]\(([^\s)]+)\)/)?.[1];
    const cover = post.metadata.frontMatter.image ?? banner;
    // Docusaurus only bundles image assets beginning with "./". This also
    // makes parent-relative covers ("../assets/...") work for release posts.
    const image = cover && !/^(?:[a-z][a-z\d+.-]*:|\/|\.\/)/i.test(cover)
      ? `./${cover}`
      : cover;

    // Docusaurus can use an image's alt text (often just "Banner") as the
    // default excerpt. Skip standalone images and headings for card summaries.
    const prose = body
      .replace(/<!--[\s\S]*?-->/g, '')
      .replace(/^\s*!\[[^\n]*\]\([^\n]*\)\s*$/gm, '')
      .replace(/^#{1,6}\s+.*$/gm, '')
      .replace(/^\*\*[^*\r\n]+:\*\*[ \t]*$/gm, '')
      .replace(/^[ \t]*>[ \t]?/gm, '');
    const paragraph = prose.split(/\r?\n\s*\r?\n/).find((text) => createExcerpt(text));
    const description = post.metadata.frontMatter.description
      ?? createExcerpt((paragraph ?? '').replace(/\r?\n/g, ' '))
      ?? post.metadata.description;
    return {
      ...post,
      metadata: {
        ...post.metadata,
        description,
        frontMatter: {...post.metadata.frontMatter, image},
      },
    };
  });
