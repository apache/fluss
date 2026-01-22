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


type FeatureItem = {
  title: string;
  content: string;
  Svg: React.ComponentType<React.ComponentProps<'svg'>>;
};

const FeatureList: FeatureItem[] = [
    {
        title: 'Sub-Second Data Freshness',
        content:
            'Continuous ingestion and immediate availability of data enable low-latency analytics and real-time decision-making at scale.',
        Svg: require('@site/static/img/feature_real_time.svg').default
    },
    {
        title: 'Streaming & Lakehouse Unification',
        content:
            'Streaming-native storage with low-latency access on top of the lakehouse, using tables as a single abstraction to unify real-time and historical data across engines.',
        Svg: require('@site/static/img/feature_lake.svg').default
    },
    {
        title: 'Columnar Streaming',
        content:
            'Based on Apache Arrow it allows database primitives on data streams and techniques like column pruning and predicate pushdown. This ensures engines read only the data they need, minimizing I/O and network costs.',
        Svg: require('@site/static/img/feature_column.svg').default
    },
    {
        title: 'Compute–Storage Separation',
        content:
            'Stream processors focus on pure computation while Fluss manages state and storage, with features like deduplication, partial updates, delta joins, and aggregation merge engines.',
        Svg: require('@site/static/img/feature_update.svg').default
    },
    {
        title: 'ML & AI–Ready Storage',
        content:
            'A unified storage layer supporting row-based, columnar, vector, and multi-modal data, enabling real-time feature stores and a centralized data repository for ML and AI systems.',
        Svg: require('@site/static/img/feature_query.svg').default
    },
    {
        title: 'Changelogs & Decision Tracking',
        content:
            'Built-in changelog generation provides an append-only history of state and decision evolution, enabling auditing, reproducibility, and deep system observability.',
        Svg: require('@site/static/img/feature_changelog.svg').default
    },
];

function Feature({ title, content, Svg }: FeatureItem) {
    return (
        <div className={clsx('col col--4')}>
            <div className={styles.core_features_icon}>
              <Svg className={styles.featureSvg} role="img" />
            </div>
            <div className={styles.core_features}>
                <div className={styles.core_features_title}>{title}</div>
                <div className={styles.core_features_content}>{content}</div>
            </div>
        </div>
    );
}

export default function HomepageFeatures(): JSX.Element {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="text--center padding-horiz--md">
          <Heading as="h1">Key Features</Heading>
        </div>
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
