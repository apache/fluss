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

/**
 * Light Prism theme tuned to the Apache Fluss blue palette.
 * Pairs with --fluss-* tokens defined in src/css/custom.css.
 */
const lightTheme: PrismTheme = {
  plain: {
    color: '#0A0F1C',           // --fluss-ink-950
    backgroundColor: '#F8FAFC',
  },
  styles: [
    {
      types: ['comment', 'prolog', 'doctype', 'cdata'],
      style: {color: '#64748B', fontStyle: 'italic'},
    },
    {
      types: ['namespace'],
      style: {opacity: 0.7},
    },
    {
      types: ['string', 'attr-value', 'char', 'inserted'],
      style: {color: '#0E7C66'},
    },
    {
      types: ['number', 'boolean'],
      style: {color: '#B45309'},
    },
    {
      types: ['keyword', 'atrule', 'selector'],
      style: {color: '#194670'}, // --fluss-blue-700
    },
    {
      types: ['function', 'class-name', 'tag'],
      style: {color: '#7C3AED'}, // --fluss-violet
    },
    {
      types: ['builtin', 'constant', 'variable', 'property'],
      style: {color: '#12325C'}, // --fluss-blue-800
    },
    {
      types: ['operator', 'punctuation'],
      style: {color: '#475569'},
    },
    {
      types: ['regex', 'important', 'deleted'],
      style: {color: '#BE123C'},
    },
    {
      types: ['attr-name'],
      style: {color: '#1C5078'}, // --fluss-blue-600
    },
    {
      types: ['symbol', 'url'],
      style: {color: '#0E7490'},
    },
  ],
};

export default lightTheme;
