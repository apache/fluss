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
import useBaseUrl from '@docusaurus/useBaseUrl';
import {logoArtwork, whiteLogoArtwork, type UserStory} from '@site/src/data/userStories';

/** Render the selected original artwork with consistent clear space. */
export default function BrandLogo({
    story,
    white = false,
    className,
}: {
    story: UserStory;
    white?: boolean;
    className?: string;
}): JSX.Element {
    const artwork = white ? whiteLogoArtwork[story.organizationId] : logoArtwork[story.organizationId];
    const filename = white
        ? `white/${artwork.file}`
        : artwork.file ?? `${story.organizationId}.png`;
    const src = useBaseUrl(`/img/users/${filename}`);

    return (
        <svg
            className={className}
            viewBox={artwork.viewBox}
            role="img"
            aria-label={story.name}
            preserveAspectRatio="xMidYMid meet"
        >
            <image href={src} width={artwork.width} height={artwork.height} />
        </svg>
    );
}
