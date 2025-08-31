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

import {visit} from 'unist-util-visit'
import escapeStringRegexp from 'escape-string-regexp'
const VERSIONS = require('../../../fluss-versions.json');

// convert the versions into a Map<version, {version, fullVersion, shortVersion}>
const versionsMap = new Map();
VERSIONS.map((version) => {
    versionsMap.set(version.versionName, version);
});

function getDocsVersionName(pathname) {
    const parts = pathname.split('/');
    const websiteIndex = parts.lastIndexOf('website');

    if (websiteIndex === -1 || websiteIndex + 1 >= parts.length) return '';

    const docsName = parts[websiteIndex + 1];
    if (docsName === 'docs') {
        return 'next';
    } else if (docsName === 'versioned_docs' && websiteIndex + 2 < parts.length) {
        return parts[websiteIndex + 2];
    } else {
        return '';
    }
}

const plugin = (options) => {
    const transformer = async (ast, vfile) => {
        const versionName = getDocsVersionName(vfile.path);
        const version = versionsMap.get(versionName);

        if (!version) {
            return;
        }

        const replacements = {
            "$FLUSS_VERSION$": version.fullVersion,
            "$FLUSS_VERSION_SHORT$": version.shortVersion,
            "$FLUSS_DOCKER_VERSION$": version.dockerVersion
        };

        // RegExp to find any replacement keys.
        const regexp = RegExp(
            '(' +
            Object.keys(replacements)
                .map(key => escapeStringRegexp(key))
                .join('|') +
            ')',
            'g',
        )

        const replacer = (_match, name) => replacements[name]

        // Go through all text, html, code, inline code, and links.
        visit(ast, ['text', 'html', 'code', 'inlineCode', 'link'], node => {
            if (node.type === 'link') {
                // For links, the text value is replaced by text node, so we change the
                // URL value.
                node.url = node.url.replace(regexp, replacer)
            } else {
                // For all other nodes, replace the node value.
                node.value = node.value.replace(regexp, replacer)
            }
        });
    };
    return transformer;
};

export default plugin;
