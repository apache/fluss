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
import BrandLogo from './BrandLogo';
import {userStories, whiteLogoArtwork} from '@site/src/data/userStories';
import styles from './styles.module.css';

/** A short case study based on the organization's public community submission. */
export default function UserStory({id}: {id: string}): JSX.Element {
    const story = userStories.find((entry) => entry.id === id);
    if (!story) {
        throw new Error(`No local user story for ${id}`);
    }
    const hasWhiteLogo = Boolean(whiteLogoArtwork[story.organizationId]);

    return (
        <Layout title={`${story.name}: ${story.title}`} description={story.summary}>
            <main className={styles.page}>
                <header className={styles.detailHero}>
                    <div className={styles.readingWidth}>
                        <Link to="/user-stories/" className={styles.backLink}>
                            ← User Stories
                        </Link>
                        <div className={hasWhiteLogo ? undefined : styles.detailLogoPanel}>
                            <BrandLogo
                                story={story}
                                white={hasWhiteLogo}
                                className={styles.detailLogo}
                            />
                        </div>
                        <div className={styles.detailMeta}>
                            {story.name} · {story.region}
                        </div>
                        <h1>{story.title}</h1>
                        <p>{story.summary}</p>
                    </div>
                </header>
                <article className={styles.readingWidth}>
                    <div className={styles.articleBody}>
                        <section>
                            <h2>
                                About <Link to={story.website}>{story.name}</Link>
                            </h2>
                            <p>{story.about}</p>
                        </section>
                        {story.sections.map((section) => (
                            <section key={section.heading}>
                                <h2>{section.heading}</h2>
                                <p>{section.text}</p>
                            </section>
                        ))}
                        {story.references && story.references.length > 0 && (
                            <section>
                                <h2>Further reading</h2>
                                <ul className={styles.references}>
                                    {story.references.map((reference) => (
                                        <li key={reference.href}>
                                            <Link to={reference.href}>
                                                <span>{reference.title}</span>
                                                <span aria-hidden="true">↗</span>
                                            </Link>
                                        </li>
                                    ))}
                                </ul>
                            </section>
                        )}
                        <aside className={styles.source}>
                            <strong>Shared by {story.name}</strong>
                            <p>
                                This overview draws on the team’s public community submission and
                                linked resources. Deployment details and results reflect the
                                workloads described in those sources.
                            </p>
                            <Link to={story.submission}>
                                Read the original submission <span aria-hidden="true">↗</span>
                            </Link>
                        </aside>
                        <Link to="/user-stories/" className={styles.readLink}>
                            Explore more user stories →
                        </Link>
                    </div>
                </article>
            </main>
        </Layout>
    );
}
