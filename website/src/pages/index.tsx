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
import {useEffect, useState} from 'react';

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
                        to="https://github.com/apache/fluss">
                        <img
                            src="img/github_icon.svg"
                            alt="GitHub"
                            className={styles.buttonIcon}
                        />
                        GitHub
                    </Link>

                    <Link
                        className={clsx("button button--secondary button--lg", styles.buttonWithIcon, styles.buttonWidth)}
                        to="https://join.slack.com/t/apache-fluss/shared_invite/zt-33wlna581-QAooAiCmnYboJS8D_JUcYw">
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
    const [isScrolled, setIsScrolled] = useState(false);

    useEffect(() => {
        const handleScroll = () => {
            // Change navbar style when scrolled past 500 (past most of the hero section)
            setIsScrolled(window.scrollY > 500);
        };

        window.addEventListener('scroll', handleScroll);
        return () => window.removeEventListener('scroll', handleScroll);
    }, []);

    return (
        <>
            {/* Inline styles for homepage navbar with scroll-based transitions */}
            <style dangerouslySetInnerHTML={{__html: `
                .navbar {
                    background-color: ${isScrolled ? 'var(--ifm-navbar-background-color)' : 'transparent'} !important;
                    backdrop-filter: ${isScrolled ? 'blur(8px)' : 'none'} !important;
                    box-shadow: ${isScrolled ? 'var(--ifm-navbar-shadow)' : 'none'} !important;
                    transition: background-color 0.3s ease, backdrop-filter 0.3s ease, box-shadow 0.3s ease !important;
                }
                .navbar__link,
                .navbar__brand,
                .navbar__brand b,
                .navbar__brand strong,
                .navbar__title {
                    color: ${isScrolled ? 'var(--ifm-navbar-link-color)' : 'white'} !important;
                    text-shadow: ${isScrolled ? 'none' : '0 1px 3px rgba(0, 0, 0, 0.3)'};
                    transition: color 0.3s ease, text-shadow 0.3s ease !important;
                }
                .navbar__brand * {
                    color: ${isScrolled ? 'var(--ifm-navbar-link-color)' : 'white'} !important;
                }
                .navbar__logo {
                    content: url('${isScrolled ? '/img/logo/svg/colored_logo.svg' : '/img/logo/svg/white_color_logo.svg'}') !important;
                    transition: content 0.3s ease !important;
                }
                .navbar__search-input {
                    background-color: rgba(255, 255, 255, 0.9) !important;
                    color: #1d1d1d !important;
                }
                .navbar__search-input::placeholder {
                    color: #666 !important;
                    opacity: 1 !important;
                }
                .DocSearch-Button {
                    background-color: rgba(255, 255, 255, 0.9) !important;
                }
                .DocSearch-Button-Placeholder {
                    color: #666 !important;
                }
                .DocSearch-Search-Icon {
                    color: #666 !important;
                }
                .navbar__toggle {
                    color: ${isScrolled ? 'var(--ifm-navbar-link-color)' : 'white'} !important;
                    transition: color 0.3s ease !important;
                }
                .navbar__toggle svg {
                    color: ${isScrolled ? 'var(--ifm-navbar-link-color)' : 'white'} !important;
                    fill: ${isScrolled ? 'var(--ifm-navbar-link-color)' : 'white'} !important;
                    transition: color 0.3s ease, fill 0.3s ease !important;
                }
                .header-github-link::before {
                    background: url("data:image/svg+xml,%3Csvg viewBox='0 0 24 24' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath fill='${isScrolled ? 'black' : 'white'}' d='M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12'/%3E%3C/svg%3E") no-repeat !important;
                    transition: background 0.3s ease !important;
                }
            `}} />
            <Layout
                // leave empty to just display "Apache Fluss (Incubating)" in tab on landing page
                title=""
                description="Streaming Storage for Real-Time Analytics"
                wrapperClassName={styles.homepageWrapper}>
                <HomepageHeader/>
                <main>
                    <HomepageIntroduce/>
                    <HomepageFeatures/>
                </main>
            </Layout>
        </>
    );
}
