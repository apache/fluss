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

package org.apache.fluss.connector.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Objects;
import java.util.UUID;

/**
 * Transaction handle for Fluss connector.
 * 
 * <p>Currently supports read-only transactions.
 */
public class FlussTransactionHandle implements ConnectorTransactionHandle {

    private final String transactionId;

    @JsonCreator
    public FlussTransactionHandle(@JsonProperty("transactionId") String transactionId) {
        this.transactionId = transactionId != null ? transactionId : UUID.randomUUID().toString();
    }

    public FlussTransactionHandle() {
        this(UUID.randomUUID().toString());
    }

    @JsonProperty
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlussTransactionHandle that = (FlussTransactionHandle) obj;
        return Objects.equals(transactionId, that.transactionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId);
    }

    @Override
    public String toString() {
        return "FlussTransactionHandle{" +
                "transactionId='" + transactionId + '\'' +
                '}';
    }
}
