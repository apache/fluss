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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Server returns this error if a request could not be processed because the server is not the
 * leader or follower for a table bucket. This could be a transient exception during leader
 * elections and reassignments. For `Produce` and other requests which are intended only for the
 * leader, this exception indicates that the server is not the current leader. For client `Fetch`
 * requests which may be satisfied by a leader or follower, this exception indicates that the server
 * is not a replica of the table bucket.
 *
 * @since 0.1
 */
@PublicEvolving
public class NotLeaderOrFollowerException extends InvalidMetadataException {
    private static final long serialVersionUID = 1L;

    public NotLeaderOrFollowerException(String message) {
        super(message);
    }

    public NotLeaderOrFollowerException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotLeaderOrFollowerException(Throwable cause) {
        super(cause);
    }
}
