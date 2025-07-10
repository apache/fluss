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

import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import versionReplace from './src/plugins/remark-version-replace/index';

const config: Config = {
  title: 'Apache Fluss™ (incubating)',
  tagline: 'Streaming Storage for Real-Time Analytics',
  favicon: 'img/logo/fluss_favicon.svg',

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
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },



  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: ({docPath}) =>
              `https://github.com/apache/fluss/edit/main/website/docs/${docPath}`,
          remarkPlugins: [versionReplace],
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
        editUrl: ({docPath}) => {
          return `https://github.com/apache/fluss/edit/main/website/community/${docPath}`;
        },
        // ... other options
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
          debug: true,
          offlineModeActivationStrategies: [
            'appInstalled',
            'standalone',
            'queryString',
          ],
          pwaHead: [
            { tagName: 'link', rel: 'icon', href: '/img/logo.svg' },
            { tagName: 'link', rel: 'manifest', href: '/manifest.json' },
            { tagName: 'meta', name: 'theme-color', content: '#0071e3' },
          ],
      },
    ],
  ],
  themeConfig: {
    // Replace with your project's social card
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
    },
    navbar: {
      title: '',
      logo: {
        alt: 'Fluss',
        src: 'img/logo/svg/colored_logo.svg',
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
      copyright: `Copyright © ${new Date().getFullYear()} The Apache Software Foundation.
      Apache®, Apache Flink®, Flink®, Apache Kafka®, Kafka®, Spark® and associated open source project names and logos are trademarks of the Apache Software Foundation.`,
    },
    prism: {
      theme: prismThemes.vsDark,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['java', 'bash']
    },
    algolia: {
      appId: "D8RXQUTC99",
      apiKey: "8039cbe25ae878764cbace303aa800e0",
      indexName: "alibabaio",
      contextualSearch: true,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;