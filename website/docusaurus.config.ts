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

import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import lightTheme from './src/utils/prismLight';
import darkTheme from './src/utils/prismDark';
import versionReplace from './src/plugins/remark-version-replace/index';
import { loadVersionData } from './src/utils/versionData';
const { versionsMap, latestVersion } = loadVersionData();

const config: Config = {
  title: 'Apache Fluss™ (Incubating)',
  tagline: 'The streaming storage layer for real-time analytics and the lakehouse',
  favicon: 'img/logo/fluss_favicon.svg',

  headTags: [
    {
      tagName: 'meta',
      attributes: {
        name: 'description',
        content:
          'Apache Fluss is an open-source columnar streaming storage system. Sub-second freshness, primary-key tables, first-class Apache Flink integration, and native tiering to Apache Iceberg and Apache Paimon.',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:title',
        content: 'Apache Fluss · Streaming Storage for the Real-Time Lakehouse',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:description',
        content:
          'Open-source columnar streaming storage with sub-second freshness, primary-key tables, Flink integration, and native tiering to Iceberg and Paimon.',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        property: 'og:type',
        content: 'website',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'twitter:card',
        content: 'summary_large_image',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'twitter:title',
        content: 'Apache Fluss · Streaming Storage for the Real-Time Lakehouse',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'twitter:description',
        content:
          'Open-source columnar streaming storage with sub-second freshness, primary-key tables, Flink integration, and native tiering to Iceberg and Paimon.',
      },
    },
    {
      tagName: 'meta',
      attributes: {
        name: 'theme-color',
        content: '#102856',
      },
    },
  ],

  // Set the production url of your site here
  url: 'https://fluss.apache.org/',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'apache', // Usually your GitHub org/user name.
  projectName: 'fluss-website', // Usually your repo name.
  deploymentBranch: 'asf-site',
  trailingSlash: true,

  onBrokenLinks: 'throw',

  // Serve blog-dependent static resources (avatars) from blog/static/
  // Blog content is cloned from a separate repo via setup_blog.sh
  staticDirectories: ['static', 'blog/static'],

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn'
    }
  },

  presets: [
    [
      'classic',
      {
        docs: {
            sidebarPath: './sidebars.ts',
            remarkPlugins: [versionReplace],
            lastVersion: latestVersion,
            versions: versionsMap
        },
        blog: {
          showReadingTime: false,
          feedOptions: {
            type: ['rss', 'atom'],
            xslt: true,
          },
          onInlineTags: 'warn',
          onInlineAuthors: 'warn',
          onUntruncatedBlogPosts: 'warn',
          blogSidebarCount: 'ALL',
          blogSidebarTitle: 'All our posts',
        },
        theme: {
          customCss: './src/css/custom.css'
        },
      } satisfies Preset.Options,
    ],
  ],
  plugins: [
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'community',
        path: 'community',
        routeBasePath: 'community',
        sidebarPath: './sidebarsCommunity.js',
        // editUrl intentionally omitted so the "Edit this page" link does
        // not appear at the bottom of community pages (mirrors the docs and
        // blog presets, which also leave editUrl unset).
      },
    ],
    [
      '@docusaurus/plugin-content-pages',
      {
        id: 'learn-pages',
        path: 'learn',
        routeBasePath: 'learn',
      },
    ],
    [
      '@docusaurus/plugin-pwa',
      {
          debug: false,
          offlineModeActivationStrategies: [
            'appInstalled',
            'standalone',
            'queryString',
          ],
          pwaHead: [
            { tagName: 'link', rel: 'icon', href: '/img/logo/fluss_favicon.svg' },
            { tagName: 'link', rel: 'manifest', href: '/manifest.json' },
            { tagName: 'meta', name: 'theme-color', content: '#102856' },
          ],
      },
    ],
    [
      '@docusaurus/plugin-client-redirects',
      {
          // Create redirects from the available routes that have already been created
          createRedirects(existingPath) {
            // Only evaluate paths related to documentation
            if (!existingPath.startsWith('/docs/')) {
              return undefined;
            }

            // Extract the relative path after /docs/
            const relativeDocsPath = existingPath.substring(6);
            const firstSegment = relativeDocsPath.split('/')[0];

            // Exclude any known version identifiers aligned with existing routes
            const existingVersionedRoutes = ['next', ...Object.keys(versionsMap)];
            if (existingVersionedRoutes.includes(firstSegment)) {
              return undefined;
            }

            const redirects = [
              // Redirect the explicit versioned path to the implicit unversioned path
              `/docs/${latestVersion}${existingPath.replace('/docs', '')}`,
            ];

            // Preserve the previously published URLs for docs pages that were
            // moved/renamed. These are keyed off the new (existing) route, so the
            // redirect target is always valid, and the old URL is preserved once
            // the restructured version becomes the latest unversioned release.
            const renameRules = [
              { from: '/maintenance/filesystems/', to: '/maintenance/tiered-storage/filesystems/' },
              { from: '/streaming-lakehouse/integrate-data-lakes/formats/', to: '/streaming-lakehouse/datalake-formats/' },
              { from: '/streaming-lakehouse/integrate-data-lakes/catalogs/', to: '/streaming-lakehouse/datalake-catalogs/' },
            ];
            for (const rule of renameRules) {
              if (existingPath.includes(rule.to)) {
                redirects.push(existingPath.replace(rule.to, rule.from));
              }
            }

            return redirects;
        },
      },
    ],

  ],
  themeConfig: {
    image: 'img/logo/png/colored_logo.png',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: '',
      logo: {
        alt: 'Fluss',
        src: 'img/logo/svg/white_color_logo.svg',
        srcDark: 'img/logo/svg/white_color_logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {to: '/blog', label: 'Blog', position: 'left'},
        {
          label: 'Learn',
          position: 'left',
          type: 'dropdown',
          items: [
            {
              label: 'Talks',
              to: '/learn/talks',
            },
            {
              label: 'Videos',
              to: '/learn/videos',
            },
          ],
        },
        {to: '/community/welcome', label: 'Community', position: 'left'},
        {to: '/roadmap', label: 'Roadmap', position: 'left'},
        {to: '/downloads', label: 'Downloads', position: 'left'},
        {to: '/javadoc', label: 'Javadocs', position: 'left',  target: '_blank'},
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
        },
        {
          href: 'https://github.com/apache/fluss',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Product',
          items: [
            {label: 'Documentation', to: '/docs/quickstart/flink'},
            {label: 'Quickstart', to: '/docs/quickstart/flink'},
            {label: 'Roadmap', to: '/roadmap'},
            {label: 'Downloads', to: '/downloads'},
            {label: 'Blog', to: '/blog'},
          ],
        },
        {
          title: 'Community',
          items: [
            {label: 'GitHub', href: 'https://github.com/apache/fluss'},
            {label: 'Slack', href: 'https://join.slack.com/t/apache-fluss/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw'},
            {label: 'Welcome', to: '/community/welcome'},
            {label: 'Contribute', to: '/community/welcome'},
          ],
        },
        {
          title: 'Resources',
          items: [
            {label: 'Talks', to: '/learn/talks'},
            {label: 'Videos', to: '/learn/videos'},
            {label: 'Issues', href: 'https://github.com/apache/fluss/issues'},
            {label: 'Releases', href: 'https://github.com/apache/fluss/releases'},
          ],
        },
        {
          title: 'Apache',
          items: [
            {label: 'Foundation', href: 'https://www.apache.org/'},
            {label: 'License', href: 'https://www.apache.org/licenses/'},
            {label: 'Events', href: 'https://events.apache.org'},
            {label: 'Donate', href: 'https://www.apache.org/foundation/sponsorship.html'},
            {label: 'Sponsors', href: 'https://www.apache.org/foundation/thanks.html'},
            {label: 'Security', href: 'https://www.apache.org/security/'},
            {label: 'Privacy', href: 'https://privacy.apache.org/policies/privacy-policy-public.html'},
          ],
        },
      ],
      logo: {
        width: 200,
        src: "/img/apache-incubator.svg",
        href: "https://incubator.apache.org/",
        alt: "Apache Incubator logo"
      },
      copyright: `<br><p>Apache Fluss is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.</p>
                  <p>Copyright © ${new Date().getFullYear()} The Apache Software Foundation, Licensed under the Apache License, Version 2.0.</p>
                  <p>Apache, the names of Apache projects, and the feather logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries. All other marks mentioned may be trademarks or registered trademarks of their respective owners.</p>`,
    },
    prism: {
      theme: lightTheme,
      darkTheme: darkTheme,
      additionalLanguages: ['java', 'bash', 'scala', 'rust', 'toml', 'cmake']
    },
    algolia: {
      appId: "X8KSGGLJW1",
      apiKey: "5d0685995a3cb0052f32a59216ad3d35",
      indexName: "fluss",
      contextualSearch: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;