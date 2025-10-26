/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const pathToFluss = '../../fluss-versions.json';
const pathToVersions = '../../versions.json';

export function loadVersionData(): { versionsMap: Record<string, any>, latestVersion: string} {
    const fluss_versions: Array<any> = require(pathToFluss);
    const flussVersionMap = new Map<string, any>();
    fluss_versions.forEach((v) => {
        flussVersionMap.set(v.shortVersion, v.released);
    });

    let versions: Array<string> = [];
    try {
        versions = require(pathToVersions);
    } catch (e) {
        versions = [];
    }

    let latestVersion: string = 'current';
    const versionsMap: Record<string, any> = Object.fromEntries([
        ['current', { label: 'Next', path: 'next', banner: 'unreleased' }],
        ...versions.map((item: string) => {
            if (flussVersionMap.get(item)) {
                if (latestVersion === 'current') {
                    latestVersion = item;
                    return [item, { label: item, banner: 'none' }];
                } else {
                    return [item, { label: item, path: item, banner: 'unmaintained' }];
                }
            } else {
                return [item, { label: item + ' 🚧', path: item, banner: 'unreleased' }];
            }
        }),
    ]);

    return {versionsMap, latestVersion};
}