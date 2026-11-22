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

/**
 * Exception thrown when a request is sent to a standby coordinator server.
 *
 * <p>The server rejects the request before executing it. Clients should refresh cluster metadata to
 * discover the current coordinator leader before retrying.
 *
 * @since 1.0
 */
public class NotCoordinatorLeaderException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public NotCoordinatorLeaderException(String message) {
        super(message);
    }

    public NotCoordinatorLeaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
