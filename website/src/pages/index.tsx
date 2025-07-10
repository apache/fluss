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
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';
import HomepageIntroduce from '@site/src/components/HomepageIntroduce';
import Heading from '@theme/Heading';

import styles from './index.module.css';

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    return (
        <header className={clsx('hero hero--primary', styles.heroBanner)}>
            <div className="container">
                <Heading as="h1" className="hero__title">
                    {siteConfig.title}
                </Heading>
                <p className="hero__subtitle">{siteConfig.tagline}</p>
                <div className={styles.buttons}>
                    <Link
                        className={clsx("hero_button button button--primary button--lg", styles.buttonWidth)}
                        to="/docs/quickstart/flink">
                        Quick Start
                    </Link>

                    <Link
                        className={clsx("button button--secondary button--lg", styles.buttonWithIcon, styles.buttonWidth)}
                        to="https://github.com/alibaba/fluss">
                        <img
                            src="img/github_icon.svg"
                            alt="GitHub"
                            className={styles.buttonIcon}
                        />
                        GitHub
                    </Link>

                    <Link
                        className={clsx("button button--secondary button--lg", styles.buttonWithIcon, styles.buttonWidth)}
                        to="https://join.slack.com/t/fluss-hq/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw">
                        <img
                            src="img/slack_icon.svg"
                            alt="Slack"
                            className={styles.buttonIcon}
                        />
                        Slack
                    </Link>
                </div>
            </div>
        </header>
    );
}

export default function Home(): JSX.Element {
    const {siteConfig} = useDocusaurusContext();
    return (
        <Layout
            title="Apache Fluss™ (incubating)"
            description="Streaming Storage for Real-Time Analytics">
            <HomepageHeader/>
            <main>
                <HomepageIntroduce/>
                <HomepageFeatures/>
            </main>
        </Layout>
    );
}
