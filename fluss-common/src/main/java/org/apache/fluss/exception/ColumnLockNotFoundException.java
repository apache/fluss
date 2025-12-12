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
 * Exception thrown when a column lock is not found or has expired.
 *
 * <p>This is a retriable exception (extends {@link RetriableException}) because:
 *
 * <ul>
 *   <li>The client's background renewal thread will automatically attempt to re-acquire the lock if
 *       renewal fails
 *   <li>Lock may be temporarily unavailable due to TabletServer restart (server loses in-memory
 *       lock state)
 *   <li>After successful lock re-acquisition, subsequent write operations can proceed normally
 * </ul>
 *
 * <p>The client should retry write operations when this exception occurs. The background lock
 * manager will handle lock recovery automatically.
 *
 * @since 0.9
 */
@PublicEvolving
public class ColumnLockNotFoundException extends RetriableException {
    private static final long serialVersionUID = 1L;

    public ColumnLockNotFoundException(String message) {
        super(message);
    }

    public ColumnLockNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
