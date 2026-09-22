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

export type StoryCategory = 'End user' | 'Service provider' | 'Ecosystem integration';

export interface UserStory {
    id: string;
    organizationId: string;
    name: string;
    category: StoryCategory;
    region: string;
    about: string;
    website: string;
    title: string;
    summary: string;
    tags: string[];
    href: string;
    submission: string;
    sections: {heading: string; text: string}[];
    references?: {title: string; href: string}[];
}

export const userDiscussion = 'https://github.com/apache/fluss/discussions/4033';

// Keep one card per organization, linking to its dedicated overview.
// Organization overviews can collect multiple published stories as references.
export const userStories: UserStory[] = [
    {
        id: 'alibaba',
        organizationId: 'alibaba',
        name: 'Alibaba Group',
        category: 'End user',
        region: 'China',
        about: 'Alibaba Group is a technology company whose core businesses are e-commerce and cloud computing.',
        website: 'https://www.alibabagroup.com/en-US/faqs-business-operations',
        title: 'Unified real-time analytics across Alibaba Group',
        summary:
            'Alibaba uses Fluss as a shared ingestion and serving layer for behavioral analytics across Taobao, Tmall, Ele.me, Amap, and Alibaba Pictures.',
        tags: ['User-behavior analytics', 'Streaming Lakehouse'],
        href: '/user-stories/alibaba/',
        submission: `${userDiscussion}#discussioncomment-18067474`,
        sections: [
            {
                heading: 'Behavioral data collection at group scale',
                text: 'The A+ platform collects events from web, mobile apps, mini-programs, and server SDKs. Alibaba reports processing 4 PB daily during Double 11 2025, with peaks of 100 million TPS and 100 GiB/s, and over 40% lower pipeline costs.',
            },
            {
                heading: 'Real-time experiments at Taotian',
                text: 'Across more than 100 experimentation scenarios, Fluss feeds search, recommendations, content, and growth analytics. The team reports 59% lower Flink CPU usage, 73% lower memory usage, and near elimination of over 100 TB of operator state.',
            },
            {
                heading: 'Streaming into the Lakehouse',
                text: 'Fluss tiers data into Paimon/Alake, replacing separate Lambda pipelines. Streaming consumers receive updates within seconds; the lake becomes available within minutes. Partial-column updates also lower the cost of batch backfills.',
            },
        ],
        references: [
            {
                title: 'Taobao: Search and recommendations',
                href: '/blog/taobao-practice/',
            },
            {
                title: 'Taobao Instant Commerce: Real-time decisions',
                href: '/blog/taobao-instant-commerce-real-time-decision/',
            },
            {
                title: 'Alibaba Double 11: Trillion-scale production (Chinese)',
                href: 'https://zhuanlan.zhihu.com/p/2000632412443535295',
            },
        ],
    },
    {
        id: 'iqiyi',
        organizationId: 'iqiyi',
        name: 'iQIYI',
        category: 'End user',
        region: 'China',
        about: 'iQIYI is an online entertainment platform in China that produces and distributes video content through subscriptions, advertising, and content licensing.',
        website: 'https://ir.iqiyi.com/',
        title: 'Fast lookups and feature backfills at massive scale',
        summary:
            'iQIYI combines primary-key lookups, streaming feature backfills, and lake tiering to serve its machine-learning data pipelines.',
        tags: ['ML features', 'Primary-key lookups'],
        href: '/user-stories/iqiyi/',
        submission: `${userDiscussion}#discussioncomment-18074056`,
        sections: [
            {
                heading: 'Moving state out of Flink jobs',
                text: 'Large Flink state can slow recovery and make data difficult to inspect. iQIYI uses Fluss primary-key tables as a schema-enforced state store. In its reported production workload, primary-key lookups achieve P99 latency below one millisecond at 100,000 queries per second.',
            },
            {
                heading: 'Replaying historical features',
                text: 'Fluss log tables provide the streaming source for long-term ML feature backfills. Longer retention and column pruning let jobs replay the fields they need. Combined with the disaggregated state architecture, iQIYI reports that a ten-tablet-server cluster handles up to one million queries per second for backfill workloads and can backfill one day of data in about thirty minutes.',
            },
            {
                heading: 'Serving hot and cold feature data',
                text: 'For real-time feature enrichment, iQIYI combines Fluss primary-key tables with Paimon lake tiering and fallback lookups for cold data. The team reports ingestion of up to 20 GB/s and approximately 50% lower cost than its previous HBase-based solution.',
            },
        ],
    },
    {
        id: 'fresha',
        organizationId: 'fresha',
        name: 'Fresha',
        category: 'End user',
        region: 'United Kingdom',
        about: 'Fresha provides booking, payments, and business-management software for beauty and wellness businesses, alongside a consumer marketplace for appointments.',
        website: 'https://www.fresha.com/for-business',
        title: 'Bringing transactional data into a real-time serving layer',
        summary:
            'Fresha runs Fluss on Amazon EKS, uses Delta Join to denormalize commerce data, and contributes Kubernetes and multi-language client support.',
        tags: ['Delta Join', 'Kubernetes'],
        href: '/user-stories/fresha/',
        submission: `${userDiscussion}#discussioncomment-18081546`,
        sections: [
            {
                heading: 'Enriching transactional streams',
                text: 'Fresha uses Fluss as the real-time serving layer between its operational databases and analytical stack. Flink ingests database changes into primary-key tables, then uses lookup joins to enrich appointment events with dimensional data stored in Fluss.',
            },
            {
                heading: 'Denormalizing commerce data with Delta Join',
                text: 'Sales, sale items, and related records form a deep transactional hierarchy. By storing each level in Fluss, eligible Flink joins can use Delta Join to look up matching records instead of retaining both inputs in operator state. Fresha reports that this brings pipeline state close to zero and reduces recovery from tens of minutes to seconds.',
            },
            {
                heading: 'Running on Kubernetes and connecting applications',
                text: 'Fresha runs Fluss on Amazon EKS with S3 remote storage and the community Helm chart. Its engineers contribute Kubernetes deployment support, the Rust core client, and Elixir bindings so application services can access Fluss directly.',
            },
        ],
        references: [
            {
                title: 'Processing Patterns with Apache Fluss',
                href: 'https://medium.com/fresha-data-engineering/processing-patterns-with-apache-fluss-302449793c43',
            },
        ],
    },
    {
        id: 'rednote',
        organizationId: 'rednote',
        name: 'rednote',
        category: 'End user',
        region: 'China',
        about: 'rednote (Xiaohongshu) is a lifestyle community platform where people discover and share interests, experiences, and recommendations.',
        website: 'https://www.rednote.com/',
        title: 'Migrating a core real-time indexing pipeline from Kafka',
        summary:
            'rednote uses column pruning and tiered storage to reduce read amplification and isolate historical index builds from online workloads.',
        tags: ['Real-time indexing', 'Streaming lakehouse'],
        href: '/user-stories/rednote/',
        submission: `${userDiscussion}#discussioncomment-18089790`,
        sections: [
            {
                heading: 'Reading fewer columns for real-time indexes',
                text: 'Search, recommendation, and advertising services consume different fields from the same wide tables. rednote moved its core indexing pipeline toward Fluss so each consumer can request only the columns it needs. The team reports 30% to 90% lower bandwidth usage across online workloads and approximately three times the peak throughput.',
            },
            {
                heading: 'Separating historical builds from online traffic',
                text: 'Index builders read historical Fluss log files directly from object storage. This keeps large scans from competing with real-time consumers for broker bandwidth and cache. rednote reports that batch and incremental index construction became 50% to 80% faster.',
            },
            {
                heading: 'Connecting the pipeline to Paimon',
                text: 'Fluss also tiers the data into Apache Paimon for exploration, migration validation, and historical analytics. Load tests, extended dual runs, and a phased rollout helped the team validate correctness and stability before moving core production traffic.',
            },
        ],
        references: [
            {
                title: 'From Kafka to Fluss: How Rednote Migrated a Core Real-Time Indexing Pipeline',
                href: '/blog/rednote-kafka-to-fluss-real-time-indexing/',
            },
        ],
    },
    {
        id: 'ant-group',
        organizationId: 'ant-group',
        name: 'Ant Group',
        category: 'End user',
        region: 'China',
        about: 'Ant Group is a digital technology company and the operator of Alipay, which connects consumers with payments and everyday services.',
        website: 'https://www.antgroup.com/en',
        title: 'Unifying real-time marketing data at Alipay',
        summary:
            'Alipay uses partial updates and columnar streaming to bring marketing events together and serve fresh data across its marketing platform.',
        tags: ['Partial updates', 'Real-time marketing'],
        href: '/user-stories/ant-group/',
        submission: `${userDiscussion}#discussioncomment-18123921`,
        sections: [
            {
                heading: 'One attribution table, many event streams',
                text: 'Alipay’s marketing platform supports coupons, red packets, payment marketing, merchant offers, and personalized recommendations. Fluss primary-key tables merge events such as coupon issuance, redemption, rewards, and task participation through partial updates. Each upstream job writes the columns it owns, while the storage layer combines them into a shared attribution table.',
            },
            {
                heading: 'Fresh data with less processing state',
                text: 'Moving column-level merging into Fluss reduces chained streaming joins and the state held in Flink. The team describes an improvement from multi-minute pipeline latency to second-level data availability. Column pruning lets downstream jobs consume only the fields required for their real-time analytics and feature pipelines.',
            },
            {
                heading: 'Connecting streaming and lakehouse analytics',
                text: 'Alipay is building a unified streaming storage layer that tiers data into the lakehouse. Real-time applications read fresh data from Fluss, while batch analytics and next-day validation use the lakehouse representation. This simplifies the previous architecture of separate streaming and lakehouse storage, dual writes, and additional data-copy jobs.',
            },
        ],
    },
    {
        id: 'jd',
        organizationId: 'jd',
        name: 'JD',
        category: 'End user',
        region: 'China',
        about: 'JD is a supply-chain technology and services company with businesses in retail, logistics, and related services.',
        website: 'https://corporate.jd.com/',
        title: 'Connecting online and offline samples for model training',
        summary:
            'JD combines Fluss and Apache Hudi to produce recommendation-ranking samples for online serving and near-real-time and offline model training.',
        tags: ['Model training', 'Streaming lakehouse'],
        href: '/user-stories/jd/',
        submission: `${userDiscussion}#discussioncomment-18149379`,
        sections: [
            {
                heading: 'A shared foundation for training samples',
                text: 'JD builds its recommendation-ranking sample pipeline on Apache Fluss and Apache Hudi. The architecture connects streaming and batch sample generation with model training, supporting online sample serving alongside near-real-time and offline training data.',
            },
            {
                heading: 'Tiering samples into the lake',
                text: 'The pipeline uses the tiering service to synchronize data to Hudi lake storage in near real time. JD reports contributing the Fluss–Hudi integration back to the open-source community.',
            },
            {
                heading: 'Reading only what a training job needs',
                text: 'During online training, jobs use Fluss column pruning to read only the fields needed by the model. This improves reading efficiency and reduces bandwidth consumption. JD also collaborates with the community on message-queue capabilities such as offset management.',
            },
        ],
    },
    {
        id: 'cisco',
        organizationId: 'cisco',
        name: 'Cisco Webex',
        category: 'End user',
        region: 'Global',
        about: 'Webex is Cisco’s collaboration and customer-experience platform, offering meetings, calling, messaging, and contact-center services.',
        website: 'https://www.webex.com/',
        title: 'Streaming Lakehouse for high-throughput events',
        summary:
            'Cisco Webex’s streaming Lakehouse architecture combines Fluss, Flink, and Iceberg for event ingestion, primary-key lookups, and simpler data lifecycle management.',
        tags: ['Event ingestion', 'Primary-key lookups'],
        href: '/user-stories/cisco/',
        submission: `${userDiscussion}#discussioncomment-18173301`,
        sections: [
            {
                heading: 'A real-time layer for event data',
                text: 'Cisco Webex’s streaming Lakehouse architecture brings together Apache Fluss for high-throughput event ingestion, Apache Flink for processing, and Apache Iceberg for historical analytics.',
            },
            {
                heading: 'Reducing the state in Flink',
                text: 'The team uses low-latency primary-key lookups in Fluss to reduce the state maintained inside Flink jobs. Cisco Webex reports that this reduces job memory usage to approximately one-third of the previous level.',
            },
            {
                heading: 'Contributing enterprise integration',
                text: 'Cisco Webex contributes implicit time partitioning, SASL/OAUTHBEARER authentication, and group-based ACL authorization to the Fluss community. These capabilities support data lifecycle management and integration with enterprise identity and access-control infrastructure.',
            },
        ],
    },
    {
        id: 'alibaba-cloud',
        organizationId: 'alibaba-cloud',
        name: 'Alibaba Cloud',
        category: 'Service provider',
        region: 'China',
        about: 'Alibaba Cloud provides cloud infrastructure and services for computing, storage, databases, and data analytics.',
        website: 'https://www.alibabacloud.com/en/product',
        title: 'Fully-managed streaming storage for Apache Fluss',
        summary:
            'Alibaba Cloud provides managed Fluss instances with visual table management, access controls, monitoring, and hot and cold data tiering.',
        tags: ['Managed service', 'Flink integration'],
        href: '/user-stories/alibaba-cloud/',
        submission: `${userDiscussion}#discussioncomment-18064873`,
        sections: [
            {
                heading: 'Managed streaming storage',
                text: 'Alibaba Cloud offers a fully managed service built on Apache Fluss. Customers create isolated, highly available instances while the service handles deployment and operation of the underlying Fluss servers and ZooKeeper nodes.',
            },
            {
                heading: 'Operating through a shared console',
                text: 'The service includes visual database and table management, fine-grained access controls, monitoring and alerts, and hot and cold data tiering. These capabilities bring streaming storage administration into the cloud platform.',
            },
            {
                heading: 'Building with Flink and the Lakehouse',
                text: 'Integration with Alibaba Cloud Realtime Compute for Apache Flink supports real-time warehouses and Streaming Lakehouse workloads. Teams can use Fluss capabilities such as column pruning, partial updates, and Delta Join within their data pipelines.',
            },
        ],
        references: [
            {
                title: 'Streaming Storage for Apache Fluss — Alibaba Cloud',
                href: 'https://www.aliyun.com/product/flink/fluss',
            },
        ],
    },
    {
        id: 'ververica',
        organizationId: 'ververica',
        name: 'Ververica',
        category: 'Service provider',
        region: 'Europe',
        about: 'Ververica develops an enterprise streaming data platform built on Apache Flink for deploying and operating real-time data applications.',
        website: 'https://www.ververica.com/product',
        title: 'Self-managed streaming storage for Apache Fluss',
        summary:
            'Ververica integrates Fluss into its platform with cluster management, database and table operations, scaling, and lake tiering.',
        tags: ['Self-managed service', 'Flink integration'],
        href: '/user-stories/ververica/',
        submission: `${userDiscussion}#discussioncomment-18077747`,
        sections: [
            {
                heading: 'Streaming storage in the customer environment',
                text: 'Ververica integrates Apache Fluss into its self-managed platform, letting organizations run streaming storage in their own infrastructure alongside Flink processing.',
            },
            {
                heading: 'Managing clusters and data',
                text: 'The platform provides visual and programmatic management for Fluss clusters, databases, and tables. Teams can manage lake tiering, rebalance data, and scale clusters through the platform’s operational tooling.',
            },
            {
                heading: 'A shared layer for streaming and historical data',
                text: 'Fluss adds low-latency reads and writes, columnar streaming, and table updates to Ververica’s data platform. Flink integration and lake tiering connect real-time pipelines with historical data for warehouse and Streaming Lakehouse applications.',
            },
        ],
        references: [
            {
                title: 'Ververica Platform for Apache Fluss — Documentation',
                href: 'https://docs.ververica.com/docs/byoc/fluss',
            },
        ],
    },
    {
        id: 'doris',
        organizationId: 'doris',
        name: 'Apache Doris',
        category: 'Ecosystem integration',
        region: 'Global',
        about: 'Apache Doris is an open-source analytical database for real-time reporting, interactive SQL analytics, and data warehousing.',
        website: 'https://doris.apache.org/docs/dev/getting-started/what-is-apache-doris/',
        title: 'Querying Fluss tables directly with SQL',
        summary:
            'The Fluss Catalog in Doris 5.0 connects SQL analytics to Fluss log and primary-key tables, including union reads over Fluss and Paimon.',
        tags: ['SQL analytics', 'Lake and log union reads'],
        href: '/user-stories/doris/',
        submission: `${userDiscussion}#discussioncomment-18269669`,
        sections: [
            {
                heading: 'SQL access to streaming tables',
                text: 'The Fluss Catalog in Doris 5.0 exposes Fluss log and primary-key tables to SQL clients and BI tools without an intermediate ETL job. For primary-key tables, Doris combines snapshots with subsequent changes to return the latest state of each key.',
            },
            {
                heading: 'Combining lake history with fresh updates',
                text: 'For tables tiered into Paimon, a union read combines historical lake data with the newer records still in Fluss. Doris scans lake files through its native readers and reads the remaining log tail through the Fluss client, preserving a unified view of the table.',
            },
            {
                heading: 'Bringing Fluss into federated analytics',
                text: 'Queries can join Fluss tables with Doris internal tables or other catalogs, including Hive, Iceberg, Paimon, and JDBC sources. Column and partition pruning reduce unnecessary reads, while query profiles expose which read paths were used.',
            },
        ],
        references: [
            {
                title: 'Fluss Catalog — Apache Doris Documentation',
                href: 'https://doris.apache.org/docs/dev/lakehouse/catalogs/fluss-catalog',
            },
        ],
    },
];

// Homepage logos link to the same destinations as the organization cards.
export const endUsers = userStories.filter((story) => story.category === 'End user');

// Visible artwork bounds, in source-image pixels. Source files are unmodified.
export const logoArtwork: Record<string, {file?: string; width: number; height: number; viewBox: string}> = {
    'alibaba-cloud': {
        width: 960,
        height: 121,
        viewBox: '0 0 960 121',
    },
    alibaba: {
        width: 1000,
        height: 378,
        viewBox: '0 0 1000 378',
    },
    iqiyi: {
        file: 'iqiyi.svg',
        width: 1577,
        height: 492,
        viewBox: '0 0 1577 492',
    },
    ververica: {
        width: 2000,
        height: 357,
        viewBox: '0 0 2000 357',
    },
    fresha: {
        width: 1400,
        height: 438,
        viewBox: '26 20 1348 398',
    },
    rednote: {
        width: 468,
        height: 72,
        viewBox: '0 0 468 72',
    },
    'ant-group': {
        file: 'ant-group-english.png',
        width: 483,
        height: 207,
        viewBox: '0 0 483 207',
    },
    jd: {
        file: 'jd-english.jpg',
        width: 8000,
        height: 4500,
        viewBox: '1712 1936 4585 633',
    },
    cisco: {
        width: 738,
        height: 210,
        viewBox: '87 22 587 166',
    },
    doris: {
        width: 2400,
        height: 480,
        viewBox: '158 88 2084 305',
    },
};

// White artwork is separate from the original submission images.
// Bounds normalize transparent padding without modifying or recoloring the assets.
export const whiteLogoArtwork: Record<string, {
    file: string;
    width: number;
    height: number;
    viewBox: string;
}> = {
    alibaba: {file: 'alibaba.png', width: 435, height: 56, viewBox: '0 0 435 56'},
    iqiyi: {file: 'iqiyi.svg', width: 1577, height: 492, viewBox: '0 0 1577 492'},
    fresha: {file: 'fresha.svg', width: 819.5, height: 240.5, viewBox: '0 0 819.5 240.5'},
    rednote: {file: 'rednote.png', width: 468, height: 72, viewBox: '0 0 468 72'},
    'ant-group': {file: 'ant-group.png', width: 256, height: 80, viewBox: '0 2 179 76'},
    jd: {file: 'jd.png', width: 200, height: 39, viewBox: '1 3 199 27'},
    cisco: {file: 'cisco.svg', width: 24, height: 24, viewBox: '0 4.3 24 15.4'},
};
