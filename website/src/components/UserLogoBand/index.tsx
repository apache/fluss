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
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import BrandLogo from '@site/src/components/UserStory/BrandLogo';
import {endUsers} from '@site/src/data/userStories';
import styles from './styles.module.css';

/** A quiet, continuous strip of end-user stories below the homepage hero. */
export default function UserLogoBand(): JSX.Element {
    return (
        <section
            className={styles.band}
            aria-label="Organizations using Apache Fluss"
        >
            <div className={styles.viewport}>
                <div className={styles.track}>
                    {[false, true].map((duplicate) => (
                        <div
                            key={String(duplicate)}
                            className={clsx(styles.group, duplicate && styles.duplicate)}
                            aria-hidden={duplicate || undefined}
                        >
                            {endUsers.map((story) => (
                                <Link
                                    key={story.organizationId}
                                    to={story.href}
                                    className={styles.logoLink}
                                    tabIndex={duplicate ? -1 : undefined}
                                    aria-label={`Read ${story.name}'s story`}
                                    title={`${story.name}: ${story.title}`}
                                >
                                    <BrandLogo story={story} white className={styles.logo} />
                                </Link>
                            ))}
                        </div>
                    ))}
                </div>
            </div>
        </section>
    );
}
