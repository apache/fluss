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

/** Rule that decides whether a single file is orphan. */
@Internal
public interface FileRule {

    /** Stable identifier used in audit logs. */
    RuleId id();

    /**
     * Decide what to do with the given file.
     *
     * @param cutoffMillis absolute epoch-ms cutoff: a file whose mtime is {@code < cutoffMillis} is
     *     age-eligible for deletion (a {@link Decision#DELETE}); a file whose mtime is {@code >=
     *     cutoffMillis} is {@link Decision#DEFER}red. Pre-frozen at action start; does not slide
     *     during a run.
     */
    Decision evaluate(FileMeta file, BucketActiveRefs activeRefs, long cutoffMillis);
}
