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

import React, { useState } from 'react';

export default function YouTubeConsent({ videoId, title }) {
    const [loaded, setLoaded] = useState(false);

    const loadYouTube = () => {
        setLoaded(true);
    };

    return (
        <div className="youtube-consent-container">
            {!loaded ? (
                <div
                    onClick={loadYouTube}
                    title="Click to Load YouTube Video"
                    style={{
                        textAlign: "center",
                        width: '560px',
                        height: '315px',
                        cursor: 'pointer',
                        backgroundImage: `url("/img/videos/placeholder_${videoId}.jpg")`,
                        backgroundSize: 'cover',
                    }}
                >
                </div>
            ) : (
                <div className="youtube-iframe-wrapper">
                    <iframe
                        width="560"
                        height="315"
                        src={`https://www.youtube-nocookie.com/embed/${videoId}`}
                        title={title}
                        frameBorder="0"
                        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
                        allowFullScreen
                    />
                </div>
            )}
        </div>
    );
}