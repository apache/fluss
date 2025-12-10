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

package org.apache.fluss.connector.trino.split;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Split for Fluss connector representing a table bucket.
 * 
 * <p>A split corresponds to a single bucket in Fluss which is the unit of parallelism
 * for data reading.
 */
public class FlussSplit implements ConnectorSplit {

    private final TablePath tablePath;
    private final TableBucket tableBucket;
    private final List<HostAddress> addresses;

    @JsonCreator
    public FlussSplit(
            @JsonProperty("tablePath") TablePath tablePath,
            @JsonProperty("tableBucket") TableBucket tableBucket,
            @JsonProperty("addresses") List<HostAddress> addresses) {
        this.tablePath = requireNonNull(tablePath, "tablePath is null");
        this.tableBucket = requireNonNull(tableBucket, "tableBucket is null");
        this.addresses = requireNonNull(addresses, "addresses is null");
    }

    public FlussSplit(TablePath tablePath, TableBucket tableBucket) {
        this(tablePath, tableBucket, List.of());
    }

    @JsonProperty
    public TablePath getTablePath() {
        return tablePath;
    }

    @JsonProperty
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public List<HostAddress> getPreferredHosts() {
        // Return the addresses as preferred hosts for locality-aware scheduling
        // If addresses are empty, return empty list to let Trino scheduler decide
        return addresses;
    }

    @Override
    public boolean isRemotelyAccessible() {
        // Fluss splits can be accessed from any worker
        return true;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FlussSplit that = (FlussSplit) obj;
        return Objects.equals(tablePath, that.tablePath) &&
                Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket);
    }

    @Override
    public String toString() {
        return "FlussSplit{" +
                "tablePath=" + tablePath +
                ", tableBucket=" + tableBucket +
                ", addresses=" + addresses +
                '}';
    }
}