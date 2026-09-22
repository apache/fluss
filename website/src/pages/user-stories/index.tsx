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
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import BrandLogo from '@site/src/components/UserStory/BrandLogo';
import {userDiscussion, userStories, type StoryCategory} from '@site/src/data/userStories';
import styles from '@site/src/components/UserStory/styles.module.css';

const groups: {category: StoryCategory; title: string; description: string}[] = [
    {
        category: 'End user',
        title: 'Built with Fluss',
        description: 'Real-world applications, shared by the teams building them.',
    },
    {
        category: 'Service provider',
        title: 'Services built on Fluss',
        description: 'Platforms that help organizations deploy and operate Apache Fluss.',
    },
    {
        category: 'Ecosystem integration',
        title: 'Connected to Fluss',
        description: 'Projects bringing Fluss data into the wider data ecosystem.',
    },
];

export default function UserStories(): JSX.Element {
    return (
        <Layout
            title="User Stories"
            description="Explore how organizations use Apache Fluss for real-time analytics, AI applications, and streaming Lakehouses."
        >
            <main className={styles.page}>
                <header className={styles.hero}>
                    <div className={styles.container}>
                        <span className={styles.eyebrow}>The Fluss community</span>
                        <h1>User Stories</h1>
                        <p>
                            From real-time analytics to AI-powered applications.
                            <br />
                            Discover what teams are building with Apache Fluss.
                        </p>
                    </div>
                </header>
                <div className={styles.container}>
                    {groups.map((group) => (
                        <section
                            key={group.category}
                            className={styles.storyGroup}
                            aria-label={group.title}
                        >
                            <div className={styles.groupHeading}>
                                <h2>{group.title}</h2>
                                <p>{group.description}</p>
                            </div>
                            <div className={styles.grid}>
                                {userStories
                                    .filter((story) => story.category === group.category)
                                    .map((story) => (
                                        <article key={story.id} className={styles.card}>
                                            <Link
                                                to={story.href}
                                                className={styles.cardLink}
                                                aria-label={`${story.name}: ${story.title}`}
                                            >
                                                <div className={styles.logoPanel}>
                                                    <BrandLogo
                                                        story={story}
                                                        className={styles.cardLogo}
                                                    />
                                                </div>
                                                <div className={styles.cardBody}>
                                                    <div className={styles.organization}>
                                                        <span>{story.name}</span>
                                                    </div>
                                                    <h3>{story.title}</h3>
                                                    <p>{story.summary}</p>
                                                    <div className={styles.tags}>
                                                        {story.tags.map((tag) => (
                                                            <span key={tag}>{tag}</span>
                                                        ))}
                                                    </div>
                                                    <span className={styles.readLink}>
                                                        Read story <span aria-hidden="true">→</span>
                                                    </span>
                                                </div>
                                            </Link>
                                        </article>
                                    ))}
                            </div>
                        </section>
                    ))}
                    <aside className={styles.contribute}>
                        <div>
                            <h2>What are you building?</h2>
                            <p>
                                Share your experience with Fluss and help the next team get started.
                            </p>
                        </div>
                        <Link to={userDiscussion}>
                            Share your story <span aria-hidden="true">↗</span>
                        </Link>
                    </aside>
                </div>
            </main>
        </Layout>
    );
}
