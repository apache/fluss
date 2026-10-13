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

package org.apache.fluss.flink.action.orphan.rule;

import org.apache.fluss.annotation.Internal;

/** Decision returned by a {@link FileRule} for a given file. */
@Internal
public enum Decision {

    /** File is orphan and should be deleted. */
    DELETE,

    /** File is referenced by an active object (manifest, snapshot, etc.). */
    KEEP_ACTIVE,

    /**
     * File is not in the active set but its age is under the {@code --older-than} threshold; the
     * deletion verdict is deferred to a future cleanup round, by which time the file will either
     * have entered the active set (KEEP_ACTIVE) or aged past the threshold (DELETE). The grace
     * window prevents racing in-flight writes whose manifest entry has not yet been committed.
     */
    DEFER,

    /** File path or extension is not recognized; skip without deletion. */
    SKIP_UNKNOWN
}
