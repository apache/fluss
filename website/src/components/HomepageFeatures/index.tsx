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
        title: 'Sub-Second Latency',
        content:
            'Fluss supports low-latency streaming reads and writes, similar to Apache Kafka. Combined with Apache Flink, Fluss enables the creation of high-throughput, low-latency streaming data warehouses, optimized for real-time applications.',
        Svg: require('@site/static/img/feature_real_time.svg').default
    },
    {
        title: 'Columnar Stream',
        content:
            'Fluss stores streaming data in a columnar format, delivering up to 10x improvement in streaming read performance. Networking costs are significantly reduced through efficient pushdown projections.',
        Svg: require('@site/static/img/feature_column.svg').default
    },
    {
        title: 'Streaming & Lakehouse Unification',
        content:
            'Fluss unifies data streaming and the data Lakehouse by serving streaming data on top of the Lakehouse. This allows for low latencies on the Lakehouse and powerful analytics to data streams.',
        Svg: require('@site/static/img/feature_lake.svg').default
    },
    {
        title: 'Real-Time Updates',
        content:
            'The PrimaryKey Table supports real-time streaming updates for large-scale data. It also enables cost-efficient partial updates, making it ideal for enriching wide tables without expensive join operations.',
        Svg: require('@site/static/img/feature_update.svg').default
    },
    {
        title: 'Changelog Generation & Tracking',
        content:
            'Updates generate complete changelogs that can be directly consumed by streaming processors in real time. This allows to streamline streaming analytics workflows and reduce operational costs.',
        Svg: require('@site/static/img/feature_changelog.svg').default
    },
    {
        title: 'Lookup Queries',
        content:
            'Fluss supports ultra-high QPS for primary key point lookups, making it an ideal solution for serving dimension tables. When combined with Apache Flink, it enables high-throughput lookup joins with exceptional efficiency.',
        Svg: require('@site/static/img/feature_lookup.svg').default
    },
    // {
    //     title: 'Interactive Queries',
    //     content:
    //         'Fluss is queryable with query engines like Flink, enabling direct data analytics. This reduces development complexity and simplifies debugging.',
    //     Svg: require('@site/static/img/feature_query.svg').default
    // },
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
