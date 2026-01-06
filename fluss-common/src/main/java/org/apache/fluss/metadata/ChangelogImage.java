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

package org.apache.fluss.metadata;

/**
 * The changelog image mode for the primary key table.
 *
 * <p>This enum defines what information is included in the changelog for update operations. It is
 * inspired by similar configurations in database systems like MySQL's binlog_row_image and
 * PostgreSQL's replica identity.
 *
 * @since 0.9
 */
public enum ChangelogImage {

    /**
     * Full changelog with both UPDATE_BEFORE and UPDATE_AFTER records. This is the default behavior
     * that captures complete information about updates, allowing tracking of previous values.
     */
    FULL,

    /**
     * WAL mode does not produce UPDATE_BEFORE records. Only INSERT, UPDATE_AFTER (and DELETE if
     * allowed) records are emitted. When WAL mode is enabled, the default merge engine (no merge
     * engine configured) us used, updates are full row (not partial update), and there is no
     * auto-increment column, an optimization is applied to skip looking up old values, and in this
     * case INSERT operations are converted to UPDATE_AFTER events, similar to database WAL
     * (Write-Ahead Log) behavior. This mode reduces storage and transmission costs but loses the
     * ability to track previous values.
     */
    WAL;

    /** Creates a {@link ChangelogImage} from the given string. */
    public static ChangelogImage fromString(String image) {
        switch (image.toUpperCase()) {
            case "FULL":
                return FULL;
            case "WAL":
                return WAL;

            default:
                throw new IllegalArgumentException("Unsupported changelog image: " + image);
        }
    }
}
