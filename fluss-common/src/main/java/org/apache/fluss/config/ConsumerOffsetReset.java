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

package org.apache.fluss.config;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * The starting position for a consumer group bucket when no committed offset exists.
 *
 * <p>Configured by {@link ConfigOptions#CLIENT_CONSUMER_AUTO_OFFSET_RESET}.
 */
@PublicEvolving
public enum ConsumerOffsetReset {
    /** Start from the earliest available offset. */
    EARLIEST,
    /** Start from the latest offset. */
    LATEST,
    /** Fail resolution instead of choosing a position. */
    NONE
}
