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

import clsx from 'clsx';
import React from 'react';
import styles from './styles.module.css';
import Heading from '@theme/Heading';

type Pillar = {
    number: string;
    title: string;
    summary: string;
    body: string;
    basis: string;
    Svg: React.ComponentType<React.ComponentProps<'svg'>>;
};

/* Body copy was previously 35 to 50 words per card, which made the section
   hard to scan (Jark feedback, PR #3226). Trimmed to ~22 words each so
   the six pillars can be grasped in a single visual pass. */
const PILLARS: Pillar[] = [
    {
        number: '01',
        title: 'Streamhouse Architecture',
        summary: 'Reusable tables for streaming, serving, and analytics.',
        body: 'Independent engines and applications share maintained datasets through supported interfaces, reducing repeated ingestion and reconstruction of equivalent data.',
        basis: 'Shared table schemas, row semantics, and supported access paths.',
        Svg: require('@site/static/img/feature_update.svg').default,
    },
    {
        number: '02',
        title: 'Lakestream Foundation',
        summary: 'Fresh and historical data as one logical table.',
        body: 'Streaming and lakehouse representations remain coordinated through shared metadata, managed tiering, and supported reads across their different freshness layers.',
        basis: 'Table metadata, committed tiering progress, and Union Read integrations.',
        Svg: require('@site/static/img/feature_lake.svg').default,
    },
    {
        number: '03',
        title: 'Compute / Storage Separation',
        summary: 'Independent engines operating on shared tables.',
        body: 'Engines run transformations and publish reusable result tables. Private execution state, including windows and timers, remains the responsibility of each computation.',
        basis: 'Separate compute and storage services with supported table interfaces.',
        Svg: require('@site/static/img/feature_real_time.svg').default,
    },
    {
        number: '04',
        title: 'Columnar Streaming Analytics',
        summary: 'Pruning that compounds.',
        body: 'Server-side projection, predicate pushdown, and partition pruning on Arrow-format streams compound into order-of-magnitude I/O and network savings.',
        basis: 'ARROW log format and the compound pruning stack on the TabletServer.',
        Svg: require('@site/static/img/feature_column.svg').default,
    },
    {
        number: '05',
        title: 'Feature & Context Stores',
        summary: 'Maintained data for ML serving and AI context.',
        body: 'Applications retrieve shared features and context through supported interfaces while owning their retrieval policies, decision logs, and any specialized indexes.',
        basis: 'Primary-key lookups, streaming reads, and lake format integrations.',
        Svg: require('@site/static/img/feature_query.svg').default,
    },
    {
        number: '06',
        title: 'Ecosystem Openness',
        summary: 'Documented formats and supported APIs.',
        body: 'Compatible engines read streams, committed lake data, or both through a supported union integration. Capabilities depend on the engine and lake format.',
        basis: 'Streaming APIs, open lake formats, and catalog integrations.',
        Svg: require('@site/static/img/feature_changelog.svg').default,
    },
];

function PillarCard({number, title, summary, body, basis, Svg}: Pillar) {
    return (
        <article className={styles.card}>
            <div className={styles.cardTop}>
                <div className={styles.iconWrap} aria-hidden="true">
                    <Svg className={styles.icon} role="img" />
                </div>
                <span className={styles.number} aria-hidden="true">{number}</span>
            </div>
            <Heading as="h3" className={styles.title}>{title}</Heading>
            <p className={styles.summary}>{summary}</p>
            <p className={styles.body}>{body}</p>
            <p className={styles.basis}>
                <span className={styles.basisLabel}>Architectural basis</span>
                {basis}
            </p>
        </article>
    );
}

export default function HomepageFeatures(): JSX.Element {
    return (
        <section className={styles.features}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.header}>
                    <span className={styles.eyebrow}>Six capability pillars</span>
                    <Heading as="h2" className={styles.heading}>
                        The benefits, grounded in the architecture.
                    </Heading>
                    <p className={styles.lead}>
                        Apache Fluss enables shared table maintenance and access
                        across streaming and lakehouse storage, with independent
                        engines providing computation, queries, and serving.
                    </p>
                </div>

                <div className={styles.grid}>
                    {PILLARS.map((p) => (
                        <PillarCard key={p.number} {...p} />
                    ))}
                </div>
            </div>
        </section>
    );
}
