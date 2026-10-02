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
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import {useColorMode} from '@docusaurus/theme-common';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import UserLogoBand from '@site/src/components/UserLogoBand';
import {useEffect, useRef, useState} from 'react';
import {Highlight} from 'prism-react-renderer';
import flussPrismDark from '@site/src/utils/prismDark';

import styles from './index.module.css';

/**
 * Canonical Fluss + Flink SQL snippet, sourced from
 * docs/engine-flink/getting-started.md.
 */
const HERO_FLINK_SQL = `-- Register Apache Fluss as a Flink catalog
CREATE CATALOG fluss_catalog WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
USE CATALOG fluss_catalog;

-- Create a primary-key table
CREATE TABLE pk_table (
  shop_id    BIGINT,
  user_id    BIGINT,
  num_orders INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH ('bucket.num' = '4');

INSERT INTO pk_table VALUES (1234, 1234, 1);
SELECT * FROM pk_table WHERE shop_id = 1234;
`;

/**
 * Canonical Fluss + Spark SQL snippet, sourced from
 * docs/engine-spark/getting-started.md. Demonstrates registering Fluss
 * as a Spark catalog and creating the equivalent primary-key table.
 */
const HERO_SPARK_SQL = `-- Register Apache Fluss as a Spark catalog (via spark-sql --conf):
--   spark.sql.catalog.fluss_catalog = org.apache.fluss.spark.SparkCatalog
--   spark.sql.catalog.fluss_catalog.bootstrap.servers = localhost:9123
USE fluss_catalog;

-- Create the same primary-key table
CREATE TABLE pk_table (
  shop_id    BIGINT,
  user_id    BIGINT,
  num_orders INT
) TBLPROPERTIES (
  'primary.key' = 'shop_id,user_id',
  'bucket.num'  = '4'
);

INSERT INTO pk_table VALUES (1234, 1234, 1);
SELECT * FROM pk_table ORDER BY shop_id;
`;

/**
 * Toggle a body-level class while the hero is in view, so we can drive the
 * navbar's transparent → solid transition entirely from CSS, without timing
 * tricks or pixel-based scroll thresholds. Works on any viewport size.
 */
function useHeroVisibilityClass(ref: React.RefObject<HTMLElement>) {
    useEffect(() => {
        const el = ref.current;
        if (!el || typeof window === 'undefined') return;
        // Mark as on-hero immediately on mount so the navbar starts transparent.
        document.body.classList.add('fluss-on-hero');

        const observer = new IntersectionObserver(
            ([entry]) => {
                if (entry.isIntersecting) {
                    document.body.classList.add('fluss-on-hero');
                } else {
                    document.body.classList.remove('fluss-on-hero');
                }
            },
            {
                // Trigger when the hero has scrolled out far enough that
                // ~64px (one navbar height) of it remains under the navbar.
                rootMargin: '-64px 0px 0px 0px',
                threshold: 0,
            },
        );
        observer.observe(el);
        return () => {
            observer.disconnect();
            document.body.classList.remove('fluss-on-hero');
        };
    }, [ref]);
}

const SLACK_INVITE =
    'https://join.slack.com/t/apache-fluss/shared_invite/zt-473vgmvjr-cmIma~_iAA4cN02o5u2pDQ';

function HeroDiagram() {
    // Keep the original Fluss diagram palette and cluster layout. The taller
    // canvas adds the gateway, direct reads, engine status, and access patterns.
    return (
        <svg
            viewBox="0 0 1200 820"
            xmlns="http://www.w3.org/2000/svg"
            role="img"
            aria-labelledby="heroDiagramTitle heroDiagramDesc">
            <title id="heroDiagramTitle">Apache Fluss architecture</title>
            <desc id="heroDiagramDesc">
                CDC streams, event streams, and AI workloads feed the Fluss hot
                tier through ingestion. The Fluss Gateway provides HTTP/REST and
                AI agent access. A Coordinator Server manages metadata, placement,
                and failover for Tablet Servers with Log Tables and PK Tables,
                exposing sub-second freshness, a columnar log, changelog streams,
                and latest state. Lakestream coordinates Fluss and lakehouse
                storage through shared metadata and a Flink tiering service.
                Lake formats include Apache Paimon, Apache Iceberg, Apache Hudi,
                and Lance. Read patterns include incremental streaming reads,
                batch snapshot and full scans, key-value and prefix lookups,
                and union reads across hot and cold data in a single query.
                Query engines can query Fluss directly through supported
                integrations. Engines shown are Apache Flink, Apache Spark,
                StarRocks (planned), Apache DataFusion (work in progress), Apache Doris
                (work in progress), DuckDB (experimental), and Trino (work in
                progress). Data access patterns include column pruning,
                partition pruning, and predicate pushdowns.
            </desc>

            <defs>
                <marker id="hgArrowLive" viewBox="0 0 10 10" refX="9" refY="5"
                        markerWidth="7" markerHeight="7"
                        orient="auto-start-reverse">
                    <path d="M0 0 L 10 5 L 0 10 Z" fill="#266D95" />
                </marker>
                <marker id="hgArrowMuted" viewBox="0 0 10 10" refX="9" refY="5"
                        markerWidth="7" markerHeight="7"
                        orient="auto-start-reverse">
                    <path d="M0 0 L 10 5 L 0 10 Z" fill="#7AAFCB" />
                </marker>
                <style
                    dangerouslySetInnerHTML={{
                        __html: `
                            .fluss-hero-live {
                                animation: flussHeroFlow 5s linear infinite;
                            }
                            @keyframes flussHeroFlow {
                                0%   { stroke-dashoffset: 32; }
                                100% { stroke-dashoffset: 0; }
                            }
                            @media (prefers-reduced-motion: reduce) {
                                .fluss-hero-live { animation: none; }
                            }
                        `,
                    }}
                />
            </defs>

            <g fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" fontSize="12">

                {/* ===== Column eyebrows ===== */}
                <text x="0" y="32" fill="#266D95" fontSize="11"
                      fontWeight="700" letterSpacing="1.6">
                    01 · SOURCES
                </text>
                <text x="310" y="32" fill="#266D95" fontSize="11"
                      fontWeight="700" letterSpacing="1.6">
                    02 · APACHE FLUSS · HOT TIER
                </text>
                <text x="940" y="32" fill="#266D95" fontSize="11"
                      fontWeight="700" letterSpacing="1.6">
                    03 · READ PATTERNS
                </text>

                {/* ===== 01 · SOURCES ===== */}
                {[
                    {y: 120, title: 'CDC Streams',   items: ['Postgres · MySQL', 'Oracle · MongoDB']},
                    {y: 188, title: 'Event Streams', items: ['Devices · Web', 'Mobile']},
                    {y: 256, title: 'AI Workloads',  items: ['Features · Embeddings', 'Multimodal · Agents']},
                ].map((s, i) => (
                    <g key={i}>
                        <text x="0" y={s.y}
                              fill="#E6ECFA" fontSize="13" fontWeight="700">
                            {s.title}
                        </text>
                        <text x="0" fill="#7AAFCB" fontSize="11">
                            {s.items.map((line, j) => (
                                <tspan key={j} x="0" y={s.y + 18 + j * 14}>{line}</tspan>
                            ))}
                        </text>
                    </g>
                ))}

                {/* Gateway access enters the same shared streaming tables. */}
                <rect x="0" y="330" width="176" height="90" rx="9"
                      fill="#102856" stroke="rgba(122,175,203,0.4)" />
                <text x="14" y="356" fill="#E6ECFA" fontSize="13" fontWeight="700">
                    Fluss Gateway
                </text>
                <text x="14" y="378" fill="#7AAFCB" fontSize="11">HTTP / REST</text>
                <text x="14" y="396" fill="#7AAFCB" fontSize="11">AI Agent Access</text>
                <path className="fluss-hero-live"
                      d="M176 375 L190 375 Q210 375 210 355 L210 265 Q210 245 230 245 L290 245"
                      fill="none" stroke="#266D95" strokeWidth="1.75"
                      strokeDasharray="6 6" markerEnd="url(#hgArrowLive)" />

                {/* Vertical separator between sources column and hot tier */}
                <line x1="150" y1="60" x2="150" y2="310"
                      stroke="rgba(122,175,203,0.18)" strokeWidth="1" />

                {/* Ingestion through engines and Fluss clients. */}
                <path
                    className="fluss-hero-live"
                    d="M150 200 L 290 200"
                    stroke="#266D95"
                    strokeWidth="1.75"
                    strokeDasharray="6 6"
                    fill="none"
                    markerEnd="url(#hgArrowLive)"
                />
                <text fill="#B1CEDF" fontSize="10" textAnchor="middle">
                    <tspan x="220" y="158" fontSize="12" fontWeight="700">Ingestion</tspan>
                    <tspan x="220" y="179">Apache Flink / Spark</tspan>
                    <tspan x="220" y="192">Apache Fluss Clients</tspan>
                </text>

                {/* ===== 02 · FLUSS · HOT TIER ===== */}
                <rect x="290" y="60" width="540" height="240" rx="14"
                      fill="#102856"
                      stroke="rgba(38,109,149,0.5)"
                      strokeWidth="1.25" />
                <text x="310" y="86"
                      fill="#B1CEDF" fontSize="11"
                      fontWeight="700" letterSpacing="1.2">
                    FLUSS CLUSTER
                </text>
                <text x="310" y="104"
                      fill="#7AAFCB" fontSize="11">
                    Sub-second freshness · Columnar log
                </text>
                <text x="310" y="119" fill="#7AAFCB" fontSize="11">
                    Changelog stream · Latest state
                </text>

                {/* Coordinator Server (centred, top) */}
                <rect x="420" y="134" width="280" height="50" rx="9"
                      fill="#0A1745"
                      stroke="rgba(38,109,149,0.55)"
                      strokeWidth="1" />
                <text x="560" y="154" textAnchor="middle"
                      fill="#B1CEDF" fontSize="13" fontWeight="700">
                    Coordinator Server
                </text>
                <text x="560" y="173" textAnchor="middle"
                      fill="#7AAFCB" fontSize="10">
                    Metadata · Placement · Failover
                </text>

                {/* Coordinator → Tablet Server fan-out (dashed muted lines) */}
                {[365, 495, 625, 755].map((cx, i) => (
                    <path key={i}
                          d={`M560 184 L 560 196 L ${cx} 196 L ${cx} 216`}
                          stroke="rgba(122,175,203,0.35)"
                          strokeWidth="1"
                          strokeDasharray="3 3"
                          fill="none" />
                ))}

                {/* Tablet Servers row (4 boxes; the last is dashed = "Node N").
                    Each tablet server contains two small pills — Log Table
                    and PK Table — to show the table types it serves. The
                    box height is bumped to 70 to fit the pills under the
                    title/label without crowding. */}
                {[
                    {x: 309, label: 'Node 01', dashed: false},
                    {x: 439, label: 'Node 02', dashed: false},
                    {x: 569, label: 'Node 03', dashed: false},
                    {x: 699, label: 'Node N',  dashed: true },
                ].map((t, i) => (
                    <g key={i}>
                        <rect x={t.x} y="216" width="112" height="70" rx="9"
                              fill="#0A1745"
                              stroke={t.dashed
                                  ? 'rgba(122,175,203,0.55)'
                                  : 'rgba(38,109,149,0.55)'}
                              strokeWidth="1"
                              strokeDasharray={t.dashed ? '4 4' : 'none'} />
                        <text x={t.x + 56} y="230" textAnchor="middle"
                              fill="#B1CEDF" fontSize="12" fontWeight="700">
                            Tablet Server
                        </text>
                        <text x={t.x + 56} y="244" textAnchor="middle"
                              fill="#7AAFCB" fontSize="10">
                            {t.label}
                        </text>
                        {/* Log Table pill (top, full width) */}
                        <rect x={t.x + 6} y="250" width="100" height="14" rx="4"
                              fill="#102856"
                              stroke="rgba(122,175,203,0.4)"
                              strokeWidth="0.75" />
                        <text x={t.x + 56} y="261" textAnchor="middle"
                              fill="#E6ECFA" fontSize="9" fontWeight="700">
                            Log Table
                        </text>
                        {/* PK Table pill (stacked below Log Table) */}
                        <rect x={t.x + 6} y="266" width="100" height="14" rx="4"
                              fill="#102856"
                              stroke="rgba(122,175,203,0.4)"
                              strokeWidth="0.75" />
                        <text x={t.x + 56} y="277" textAnchor="middle"
                              fill="#E6ECFA" fontSize="9" fontWeight="700">
                            PK Table
                        </text>
                    </g>
                ))}

                {/* Lakestream coordinates the streaming and lake representations. */}
                <path className="fluss-hero-live"
                      d="M560 300 L560 450"
                      stroke="#266D95" strokeWidth="1.75" strokeDasharray="4 4"
                      fill="none" markerEnd="url(#hgArrowLive)" />
                <text x="580" y="335" fill="#B1CEDF" fontSize="15" fontWeight="700">
                    LAKESTREAM
                </text>
                <text x="580" y="354" fill="#7AAFCB" fontSize="10">
                    One logical table · Two freshness layers
                </text>
                <text x="580" y="387" fill="#B1CEDF" fontSize="12" fontWeight="700">
                    Tiering Service
                </text>
                <text x="580" y="405" fill="#7AAFCB" fontSize="10">
                    Flink job · Compaction &amp; commits
                </text>
                <text x="580" y="423" fill="#7AAFCB" fontSize="10">
                    Shared metadata · Committed progress
                </text>

                {/* ===== 04 · LAKEHOUSE NATIVE ===== */}
                <text x="310" y="439" fill="#266D95" fontSize="11"
                      fontWeight="700" letterSpacing="1.6">
                    04 · LAKEHOUSE NATIVE
                </text>
                <rect x="290" y="460" width="540" height="120" rx="14"
                      fill="#102856" stroke="rgba(38,109,149,0.55)"
                      strokeWidth="1.25" strokeDasharray="5 4" />
                <text x="310" y="486" fill="#B1CEDF" fontSize="11"
                      fontWeight="700" letterSpacing="1.2">
                    LAKEHOUSE · COLD TIER
                </text>
                <text x="310" y="504" fill="#7AAFCB" fontSize="11">
                    Open formats · Long retention · Query-engine native
                </text>
                {[
                    {x: 312, label: 'Apache Paimon'},
                    {x: 438, label: 'Apache Iceberg'},
                    {x: 564, label: 'Apache Hudi'},
                    {x: 690, label: 'Lance'},
                ].map((l) => (
                    <g key={l.label}>
                        <rect x={l.x} y="518" width="118" height="46" rx="8"
                              fill="#0A1745" stroke="rgba(122,175,203,0.4)" />
                        <text x={l.x + 59} y="546" textAnchor="middle"
                              fill="#E6ECFA" fontSize="12" fontWeight="700">
                            {l.label}
                        </text>
                    </g>
                ))}

                {/* ===== 03 · READ PATTERNS (right column) ===== */}

                <text x="940" y="60" fill="#B1CEDF" fontSize="10">
                    Consume / Query
                </text>
                {[
                    {y: 91,  title: 'Streaming Reads', sub: 'Changelog stream · Incremental'},
                    {y: 150, title: 'Batch Reads',     sub: 'Snapshot scan · Full scan'},
                    {y: 209, title: 'Lookups',         sub: 'KV lookups · Prefix lookups'},
                ].map((r, i) => (
                    <g key={i}>
                        <path
                            className="fluss-hero-live"
                            d={`M830 ${r.y} L 930 ${r.y}`}
                            stroke="#266D95"
                            strokeWidth="1.75"
                            strokeDasharray="4 4"
                            fill="none"
                            markerEnd="url(#hgArrowLive)"
                        />
                        <text x="940" y={r.y - 4}
                              fill="#E6ECFA" fontSize="13" fontWeight="700">
                            {r.title}
                        </text>
                        <text x="940" y={r.y + 14}
                              fill="#7AAFCB" fontSize="11">
                            {r.sub}
                        </text>
                    </g>
                ))}

                {/* Union Read combines the coordinated hot and lake layers. */}
                <path
                    className="fluss-hero-live"
                    d="M830 240 L880 240 L880 410"
                    stroke="#266D95"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                />
                <path
                    className="fluss-hero-live"
                    d="M830 540 L880 540 L880 410 L930 410"
                    stroke="#266D95"
                    strokeWidth="1.75"
                    strokeDasharray="4 4"
                    fill="none"
                    markerEnd="url(#hgArrowLive)"
                />
                <text x="940" y="406"
                      fill="#E6ECFA" fontSize="13" fontWeight="700">
                    Union Read
                </text>
                <text x="940" y="424"
                      fill="#7AAFCB" fontSize="11">
                    Hot &amp; Cold Data · Single query
                </text>

                {/* ===== 05 · QUERY ENGINES ===== */}
                <path d="M560 580 L560 642" fill="none"
                      stroke="rgba(122,175,203,0.45)" strokeWidth="1"
                      strokeDasharray="4 4" />
                <rect x="290" y="642" width="540" height="146" rx="14"
                      fill="#102856" stroke="rgba(38,109,149,0.5)" />
                <path d="M290 713 L250 713 L250 280 L290 280"
                      fill="none" stroke="#7AAFCB" strokeWidth="1.25"
                      strokeDasharray="4 4" markerEnd="url(#hgArrowMuted)" />
                <text x="230" y="489" textAnchor="end"
                      fill="#B1CEDF" fontSize="11" fontWeight="700">
                    <tspan x="230">Query Fluss</tspan>
                    <tspan x="230" dy="17">directly</tspan>
                </text>
                <text x="310" y="625" fill="#266D95" fontSize="11"
                      fontWeight="700" letterSpacing="1.6">
                    05 · QUERY ENGINES
                </text>
                {[
                    {x: 302, y: 656, w: 122, label: 'Apache Flink', status: ''},
                    {x: 434, y: 656, w: 122, label: 'Apache Spark', status: ''},
                    {x: 566, y: 656, w: 122, label: 'Apache DataFusion', status: 'WIP'},
                    {x: 698, y: 656, w: 120, label: 'Apache Doris', status: 'WIP'},
                    {x: 330, y: 720, w: 144, label: 'StarRocks', status: 'Planned'},
                    {x: 486, y: 720, w: 144, label: 'DuckDB', status: 'Experimental'},
                    {x: 642, y: 720, w: 144, label: 'Trino', status: 'WIP'},
                ].map((e) => (
                    <g key={e.label}>
                        <rect x={e.x} y={e.y} width={e.w} height="52" rx="8"
                              fill={e.status ? '#17335B' : '#0A1745'}
                              stroke="rgba(122,175,203,0.4)"
                              strokeDasharray={e.status ? '3 3' : undefined} />
                        <text x={e.x + e.w / 2} y={e.y + (e.status ? 21 : 30)}
                              textAnchor="middle" fill="#E6ECFA"
                              fontSize="10.5" fontWeight="700">
                            {e.label}
                        </text>
                        {e.status && (
                            <text x={e.x + e.w / 2} y={e.y + 39}
                                  textAnchor="middle" fill="#B1CEDF" fontSize="10">
                                {e.status}
                            </text>
                        )}
                    </g>
                ))}

                {/* ===== 06 · DATA ACCESS PATTERNS ===== */}
                <rect x="915" y="495" width="280" height="293" rx="14"
                      fill="#102856" stroke="rgba(38,109,149,0.5)" />
                <text x="933" y="524" fill="#7AAFCB" fontSize="11"
                      fontWeight="700" letterSpacing="1">
                    06 · DATA ACCESS PATTERNS
                </text>
                {[
                    {y: 569, label: 'Column Pruning', kind: 'column'},
                    {y: 650, label: 'Partition Pruning', kind: 'partition'},
                    {y: 731, label: 'Predicate Pushdowns', kind: 'predicate'},
                ].map((pattern) => (
                    <g key={pattern.kind}>
                        <g transform={`translate(933 ${pattern.y - 19})`}
                           stroke="#7AAFCB" strokeWidth="1">
                            {pattern.kind === 'predicate' ? (
                                <path d="M0 0 H32 L20 16 V29 L12 33 V16 Z"
                                      fill="#266D95" />
                            ) : (
                                [0, 1, 2, 3].map((row) => (
                                    [0, 1, 2].map((col) => (
                                        <rect key={`${row}-${col}`}
                                              x={col * 11} y={row * 9} width="9" height="7"
                                              fill={(pattern.kind === 'column' ? col === 1 : row === 1)
                                                  ? '#7AAFCB' : '#0A1745'} />
                                    ))
                                ))
                            )}
                        </g>
                        <text x="981" y={pattern.y} fill="#E6ECFA"
                              fontSize="12" fontWeight="700">
                            {pattern.label}
                        </text>
                    </g>
                ))}

            </g>
        </svg>
    );
}

function HomepageHeader({heroRef}: {heroRef: React.RefObject<HTMLElement>}) {
    return (
        <header ref={heroRef} className={styles.heroBanner}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.heroInner}>
                    <div>
                        {/* Hero badge: previously "Apache Software Foundation ·
                             Apache 2.0" — three paperwork labels that
                            duplicate footer content (Jark feedback, PR #3226).
                            Replaced with a single value-oriented pill.*/}
                        <span className={styles.heroEyebrow}>
                            <span className={styles.dot} />
                            Open Source · Apache 2.0
                        </span>

                        <h1 className={styles.heroTitle}>
                            Streaming Storage for{' '}
                            <span className={styles.accent}>Real-Time Analytics &amp; AI</span>
                        </h1>

                        <p className={styles.heroSubtitle}>
                            Apache Fluss is an open-source,
                            lakehouse-native streaming storage system. It enables
                            Lakestream: a shared table foundation coordinating
                            fresh streaming data and historical lakehouse data
                            for the Streamhouse architecture.
                        </p>

                        <div className={styles.heroCtas}>
                            <Link
                                className={styles.btnPrimary}
                                to="/docs/quickstart/flink">
                                Get Started
                                <span aria-hidden="true">→</span>
                            </Link>

                            <Link
                                className={styles.btnSecondary}
                                to="https://github.com/apache/fluss">
                                <img
                                    src="img/github_icon.svg"
                                    alt=""
                                    aria-hidden="true"
                                    className={styles.btnIcon}
                                />
                                View on GitHub
                            </Link>
                        </div>
                    </div>

                    <div className={styles.heroCodeColumn}>
                        <HeroCodePanel />
                    </div>
                </div>
                <UserLogoBand />
            </div>
        </header>
    );
}

/**
 * Renders a SQL snippet with Prism syntax highlighting using the project's
 * shared dark Prism theme (so colours match the Fluss palette). The theme's
 * background is overridden because the surrounding code card already paints
 * its own background.
 */
function HeroSqlBlock({code}: {code: string}) {
    return (
        <Highlight code={code.trimEnd()} language="sql" theme={flussPrismDark}>
            {({className, tokens, getLineProps, getTokenProps}) => (
                <pre
                    role="tabpanel"
                    className={clsx(styles.codeBody, className)}
                    style={{background: 'transparent'}}>
                    {tokens.map((line, i) => (
                        <div key={i} {...getLineProps({line})}>
                            {line.map((token, key) => (
                                <span key={key} {...getTokenProps({token})} />
                            ))}
                        </div>
                    ))}
                </pre>
            )}
        </Highlight>
    );
}

function HeroCodePanel() {
    const [active, setActive] = useState<'flink' | 'spark'>('flink');
    return (
        <div className={styles.codeCard} aria-label="Apache Fluss code example">
            <div className={styles.heroCodeHeader}>
                <span className={styles.heroCodeDots} aria-hidden="true">
                    <span /><span /><span />
                </span>
                <div className={styles.heroCodeTabs} role="tablist" aria-label="Engine">
                    <button
                        type="button"
                        role="tab"
                        aria-selected={active === 'flink'}
                        className={clsx(
                            styles.heroCodeTab,
                            active === 'flink' && styles.heroCodeTabActive,
                        )}
                        onClick={() => setActive('flink')}>
                        Flink SQL
                    </button>
                    <button
                        type="button"
                        role="tab"
                        aria-selected={active === 'spark'}
                        className={clsx(
                            styles.heroCodeTab,
                            active === 'spark' && styles.heroCodeTabActive,
                        )}
                        onClick={() => setActive('spark')}>
                        Spark SQL
                    </button>
                </div>
            </div>

            <HeroSqlBlock code={active === 'flink' ? HERO_FLINK_SQL : HERO_SPARK_SQL} />
        </div>
    );
}

function ArchitectureSection() {
    return (
        <section className={clsx(styles.section, styles.sectionDark, styles.archSection)}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Architecture</span>
                    <h2 className={clsx(styles.sectionTitle, styles.archTitle)}>
                        Unlocking the Streamhouse Architecture
                    </h2>
                    <p className={clsx(styles.sectionLead, styles.archLead)}>
                        Streamhouse brings streaming, serving, and analytics
                        together through shared tables and independent compute
                        engines. Fluss enables its Lakestream storage foundation
                        by coordinating fresh streaming data with historical lakehouse data.
                    </p>
                    <Link className={styles.btnSecondary} to="/docs/next/concepts/streamhouse-and-lakestream">
                        Explore Streamhouse and Lakestream
                        <span aria-hidden="true">→</span>
                    </Link>
                </div>
                <div className={styles.archDiagram}
                     role="region" aria-label="Apache Fluss architecture diagram"
                     tabIndex={0}>
                    <HeroDiagram />
                </div>
            </div>
        </section>
    );
}

function SystemsTaxSection() {
    const beforeStack = [
        {
            label: 'Message broker',
            sub: 'Kafka, for event transport.',
        },
        {
            label: 'Stream processor',
            sub: 'Flink or Spark, for derived features and aggregations.',
        },
        {
            label: 'Online store',
            sub: 'Redis or DynamoDB, for sub-millisecond lookup.',
        },
        {
            label: 'Offline store',
            sub: 'Iceberg or Parquet on S3, for training and history.',
        },
        {
            label: 'Sync layer',
            sub: 'Bespoke pipelines and freshness monitors that drift silently.',
        },
    ];

    const afterRequirements: {label: string; sub: string}[] = [
        {
            label: 'Streaming Log',
            sub: 'Durable, replayable, offset-ordered streams',
        },
        {
            label: 'PK Lookup',
            sub: 'Sub-millisecond key/value serving',
        },
        {
            label: 'Lakestream',
            sub: 'Coordinated streaming and lakehouse layers of one logical table',
        },
        {
            label: 'Shared Tables',
            sub: 'Reusable results maintained by independent compute engines',
        },
        {
            label: 'Multi-Modal',
            sub: 'Lance integration for vectors and ML context',
        },
        {
            label: 'Audit Trail',
            sub: 'Change data feed, replayable by design',
        },
    ];

    return (
        <section className={styles.taxSection}>
            <div className={clsx('container', styles.container)}>
                {/* Left-aligned header (matches CompareSection). Previously used
                    sectionHeaderCenter, which forced the 3-sentence lead into
                    centred body copy — readable for a tagline, but awkward for
                    a paragraph this long. Left alignment also anchors the lead's
                    left edge to the taxGrid below it. */}
                <div className={styles.sectionHeader}>
                    <span className={styles.eyebrow}>The multiple-systems tax</span>
                    <h2 className={styles.sectionTitle}>
                        Shared tables reduce repeated data maintenance.
                    </h2>
                    <p className={styles.sectionLead}>
                        A common stream can feed several systems that each ingest,
                        reconstruct, and synchronize equivalent data. Streamhouse
                        lets compatible workloads reuse maintained tables on a
                        Lakestream foundation. Compute engines still run transformations;
                        specialized stores remain useful when a workload needs them.
                    </p>
                </div>

                <div className={styles.taxGrid}>
                    <div className={styles.taxColumn}>
                        <span className={clsx(styles.taxLabel, styles.taxLabelBefore)}>
                            Separate systems · repeated maintenance
                        </span>
                        <div className={styles.taxStack}>
                            {beforeStack.map((s, i) => (
                                <div key={i} className={styles.taxStackItem}>
                                    <span className={styles.taxStackIndex} aria-hidden="true">
                                        {String(i + 1).padStart(2, '0')}
                                    </span>
                                    <div>
                                        <div className={styles.taxStackTitle}>{s.label}</div>
                                        <div className={styles.taxStackSub}>{s.sub}</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                        <p className={styles.taxFootnote}>
                            Repeated ingestion · Equivalent state · Synchronization work
                        </p>
                    </div>

                    <div className={styles.taxArrow} aria-hidden="true">
                        <svg viewBox="0 0 60 24" xmlns="http://www.w3.org/2000/svg">
                            <defs>
                                <linearGradient id="taxArrowGrad" x1="0" x2="1">
                                    <stop offset="0" stopColor="#266D95" />
                                    <stop offset="1" stopColor="#1C5078" />
                                </linearGradient>
                            </defs>
                            <path
                                d="M2 12 L52 12 M44 4 L54 12 L44 20"
                                fill="none"
                                stroke="url(#taxArrowGrad)"
                                strokeWidth="2.5"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                            />
                        </svg>
                    </div>

                    <div className={styles.taxColumn}>
                        <span className={clsx(styles.taxLabel, styles.taxLabelAfter)}>
                            Streamhouse · shared table foundation
                        </span>
                        <div className={styles.taxAfterCard}>
                            <div className={styles.taxAfterHeader}>
                                <div className={styles.taxAfterTitle}>Apache Fluss</div>
                                <div className={styles.taxAfterSub}>
                                    Streaming tables and lakehouse integration
                                    enabling Lakestream beneath independent engines.
                                </div>
                            </div>
                            <ul className={styles.taxAfterList}>
                                {afterRequirements.map((r) => (
                                    <li key={r.label}>
                                        <span className={styles.taxCheck} aria-hidden="true">✓</span>
                                        <span>
                                            <strong>{r.label}</strong>
                                            {' · '}
                                            {r.sub}
                                        </span>
                                    </li>
                                ))}
                            </ul>
                        </div>
                        <p className={styles.taxFootnote}>
                            Shared tables · Managed tiering · Compatible access paths
                        </p>
                    </div>
                </div>
            </div>
        </section>
    );
}

function CompareSection() {
    /* Homepage teaser. The full comparison — when each is the right tool,
       common production patterns, per-scenario walkthroughs, and the full
       feature matrix — lives at /compare/kafka so this section stays
       short and the dedicated page can grow without diluting the
       homepage (Jark feedback, PR #3226). */
    return (
        <section className={styles.section}>
            <div className={clsx('container', styles.container)}>
                <div className={styles.sectionHeader}>
                    <span className={styles.eyebrow}>Apache Fluss vs Apache Kafka</span>
                    <h2 className={styles.sectionTitle}>
                        Where Streams Meet The Lakehouse
                    </h2>
                    <p className={styles.sectionLead}>
                        Kafka is the streaming transport. Fluss is the streaming
                        storage. If your need is large-scale stream processing with
                        Flink, real-time analytics, AI/ML, or a sub-second
                        lakehouse, Fluss is the shared streaming storage substrate
                        behind all of them. Read the full breakdown to see which
                        fits your stack.
                    </p>
                    <div className={styles.heroCtas} style={{marginTop: 0}}>
                        <Link
                            className={styles.btnPrimary}
                            to="/compare/kafka">
                            See the full comparison
                            <span aria-hidden="true">→</span>
                        </Link>
                    </div>
                </div>
            </div>
        </section>
    );
}

function CommunitySection() {
    return (
        <section className={clsx(styles.section, styles.sectionDark)}>
            <div className={clsx('container', styles.container)}>
                <div className={clsx(styles.sectionHeader, styles.sectionHeaderCenter)}>
                    <span className={styles.eyebrow}>Community</span>
                    <h2 className={styles.sectionTitle}>
                        Built in the open, governed by the ASF.
                    </h2>
                    <p className={styles.sectionLead}>
                        Apache Fluss is developed openly by a global community of
                        contributors. Join the discussion, file an issue, or send a patch.
                    </p>
                </div>

                <div className={styles.statsRow} aria-label="Project signals">
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>Apache 2.0</div>
                        <div className={styles.statLabel}>Open-source license</div>
                    </div>
                    <div className={styles.statCard}>
                        <div className={styles.statValue}>ASF</div>
                        <div className={styles.statLabel}>Apache Software Foundation governance</div>
                    </div>
                </div>

                <div className={styles.communityGrid}>
                    <Link className={styles.communityCard} to="https://github.com/apache/fluss">
                        <div className={styles.communityTitle}>GitHub</div>
                        <div>Source code, issues, and pull requests.</div>
                        <div className={styles.communityArrow}>Open repository →</div>
                    </Link>

                    <Link className={styles.communityCard} to={SLACK_INVITE}>
                        <div className={styles.communityTitle}>Slack</div>
                        <div>Real-time chat with users and committers.</div>
                        <div className={styles.communityArrow}>Join the workspace →</div>
                    </Link>

                    <Link className={styles.communityCard} to="/community/welcome">
                        <div className={styles.communityTitle}>Contribute</div>
                        <div>Welcome guide, mailing lists, and how to send your first patch.</div>
                        <div className={styles.communityArrow}>Get started →</div>
                    </Link>
                </div>
            </div>
        </section>
    );
}

/**
 * Tags <body> with `fluss-home-page` while the homepage is mounted, so
 * navbar-level CSS (which lives outside the Layout's wrapperClassName)
 * can scope homepage-only rules — e.g. hiding the Ask-AI / colour-mode
 * toggle on the landing page only.
 *
 * Body-class only — colour-mode pinning lives in <HomeColorModeLock>
 * below, which must be rendered inside <Layout> (where Docusaurus'
 * ColorModeProvider is in scope).
 */
function useHomeBodyClass() {
    useEffect(() => {
        if (typeof document === 'undefined') return;
        document.body.classList.add('fluss-home-page');
        return () => {
            document.body.classList.remove('fluss-home-page');
        };
    }, []);
}

/**
 * Pins the colour mode to light while the homepage is mounted: the
 * landing page is authored as a single (always-light) design with no
 * dark-mode variant. The user's saved preference is captured at mount
 * and restored on unmount, so docs/blog still honour their choice.
 *
 * IMPORTANT: we must go through Docusaurus' setColorMode (not raw
 * setAttribute on <html data-theme>). The navbar Logo is rendered via
 * ThemedComponent, which client-side renders ONLY the variant matching
 * the React `useColorMode()` state — not whatever's on the DOM. If we
 * force `data-theme=light` while React state is still `dark`, the Logo
 * renders only the `themedImage--dark` <img>, which `[data-theme=light]`
 * CSS doesn't match, so it stays `display: none` and the logo vanishes.
 * setColorMode keeps React state and the DOM attribute in sync.
 *
 * `{persist: false}` is a runtime-supported option on Docusaurus'
 * setColorMode (see @docusaurus/theme-common colorMode.js) — it skips
 * the localStorage write so we don't overwrite the user's preference.
 * The public TS signature omits the options arg, hence the cast.
 *
 * This component must be rendered as a child of <Layout> so that
 * ColorModeProvider (which Layout mounts) is in scope when
 * useColorMode is called.
 */
type SetColorModeWithPersist = (
    colorMode: 'light' | 'dark' | null,
    options?: {persist?: boolean},
) => void;

function HomeColorModeLock(): null {
    const {colorMode, setColorMode} = useColorMode();
    // Snapshot the user's preference at mount time. A ref keeps the
    // effect's deps array stable — otherwise the very setColorMode call
    // below would retrigger the effect and overwrite the snapshot.
    const previousColorModeRef = useRef(colorMode);

    useEffect(() => {
        previousColorModeRef.current = colorMode;

        const setMode = setColorMode as unknown as SetColorModeWithPersist;
        if (colorMode !== 'light') {
            setMode('light', {persist: false});
        }

        return () => {
            const previous = previousColorModeRef.current;
            if (previous !== 'light') {
                setMode(previous, {persist: false});
            }
        };
        // Run once per mount. colorMode is read via the closure at mount
        // time and stored in a ref so the cleanup restores the user's
        // *original* preference, not the 'light' we just forced.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return null;
}

export default function Home(): JSX.Element {
    const heroRef = useRef<HTMLElement>(null);
    useHeroVisibilityClass(heroRef);
    useHomeBodyClass();

    return (
        <Layout
            title=""
            description="Apache Fluss is an open-source columnar streaming storage system. Sub-second freshness, primary-key tables, first-class Apache Flink integration, and native tiering to Apache Iceberg and Apache Paimon."
            wrapperClassName={clsx(styles.homepageWrapper, 'fluss-home')}>
            {/* Must be inside <Layout> so ColorModeProvider is in scope. */}
            <HomeColorModeLock />
            <HomepageHeader heroRef={heroRef}/>
            <main>
                {/* Narrative arc:
                    Hero → How it's built (Architecture) → Why you need it
                    (SystemsTax) → What you get (HomepageFeatures) → How it
                    differs (Compare) → Who builds it (Community). The
                    "What is Fluss?" answer and the runnable quickstart
                    both live on the docs intro / quickstart pages now. */}
                <ArchitectureSection/>
                <SystemsTaxSection/>
                <HomepageFeatures/>
                <CompareSection/>
                <CommunitySection/>
            </main>
        </Layout>
    );
}
