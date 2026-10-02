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

import React from 'react';
import {useLocation} from '@docusaurus/router';
import ColorModeToggle from '@theme-original/ColorModeToggle';

/**
 * Swizzled wrapper around the default Docusaurus colour-mode toggle.
 *
 * The landing page (`/`) is authored as a single (always-light) design with
 * no dark-mode variant (see `useHomeBodyClass` in `src/pages/index.tsx`,
 * which also pins `<html data-theme="light">` while mounted), so the toggle
 * would be a no-op there. Instead of hiding it via CSS, we skip rendering
 * the button entirely on the homepage so it never appears in the DOM.
 *
 * Docs / blog / community pages keep the toggle.
 */
export default function ColorModeToggleWrapper(props) {
  const {pathname} = useLocation();
  if (pathname === '/') {
    return null;
  }
  return <ColorModeToggle {...props} />;
}
