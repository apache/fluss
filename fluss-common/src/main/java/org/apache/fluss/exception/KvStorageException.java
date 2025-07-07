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
 * Storage Exception for Kv.
 *
 * <p>Miscellaneous disk-related IOException occurred when handling a request. Client should request
 * metadata update and retry if the response shows {@link KvStorageException}.
 *
 * <p>Here are the guidelines on how to handle {@link KvStorageException} and IOException:
 * <li>1) If the server has not finished loading kvs, IOException does not need to be converted to
 *     {@link KvStorageException}
 * <li>2) After the server has finished loading kvs, IOException should be converted and re-thrown
 *     as {@link KvStorageException}
 * <li>3) It is preferred for IOException to be caught in KvTablet rather than in ReplicaManager
 *
 * @since 0.1
 */
@PublicEvolving
public class KvStorageException extends StorageException {

    private static final long serialVersionUID = 1L;

    public KvStorageException(String message) {
        super(message);
    }

    public KvStorageException(Throwable cause) {
        super(cause);
    }

    public KvStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
