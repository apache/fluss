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

import type {PrismTheme} from 'prism-react-renderer';

const lightTheme: PrismTheme = {
  plain: {
    color: '#36464e',
    backgroundColor: '#f5f5f5',
  },
  styles: [
    {
      types: ['comment', 'prolog', 'doctype', 'cdata'],
      style: { color: '#8e908c', fontStyle: 'italic' },
    },
    {
      types: ['namespace'],
      style: { opacity: 0.7 },
    },
    {
      types: ['string', 'attr-value', 'char', 'inserted'],
      style: { color: '#1c7d4d' },
    },
    {
      types: ['number', 'boolean'],
      style: { color: '#d52a2a' },
    },
    {
      types: ['keyword', 'atrule', 'selector'],
      style: { color: '#3f6ec6' },
    },
    {
      types: ['function', 'class-name', 'tag'],
      style: { color: '#a846b9' },
    },
    {
      types: ['builtin', 'constant', 'variable', 'property'],
      style: { color: '#6e59d9' },
    },
    {
      types: ['operator', 'punctuation'],
      style: { color: 'rgba(0, 0, 0, 0.54)' },
    },
    {
      types: ['regex', 'important', 'deleted'],
      style: { color: '#db1457' },
    },
    {
      types: ['attr-name'],
      style: { color: '#3f6ec6' },
    },
  ],
};

export default lightTheme;
