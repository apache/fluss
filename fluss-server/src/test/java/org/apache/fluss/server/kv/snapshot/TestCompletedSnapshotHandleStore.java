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

package org.apache.fluss.server.kv.snapshot;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.function.FunctionWithException;
import org.apache.fluss.utils.function.SupplierWithException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** An implementation of {@link CompletedSnapshotStore} for test purpose. */
public class TestCompletedSnapshotHandleStore implements CompletedSnapshotHandleStore {

    private final SupplierWithException<Optional<CompletedSnapshotHandle>, Exception>
            getLatestSupplier;

    private final FunctionWithException<CompletedSnapshotHandle, Void, Exception> addFunction;

    public TestCompletedSnapshotHandleStore(
            FunctionWithException<CompletedSnapshotHandle, Void, Exception> addFunction,
            SupplierWithException<Optional<CompletedSnapshotHandle>, Exception> getLatestSupplier) {
        this.addFunction = addFunction;
        this.getLatestSupplier = getLatestSupplier;
    }

    @Override
    public void add(
            TableBucket tableBucket, long snapshotId, CompletedSnapshotHandle snapshotHandle)
            throws Exception {
        addFunction.apply(snapshotHandle);
    }

    @Override
    public void remove(TableBucket tableBucket, long snapshotId) throws Exception {}

    @Override
    public Optional<CompletedSnapshotHandle> get(TableBucket tableBucket, long snapshotId)
            throws Exception {
        return Optional.empty();
    }

    @Override
    public List<CompletedSnapshotHandle> getAllCompletedSnapshotHandles(TableBucket tableBucket)
            throws Exception {
        return Collections.emptyList();
    }

    @Override
    public Optional<CompletedSnapshotHandle> getLatestCompletedSnapshotHandle(
            TableBucket tableBucket) throws Exception {
        return getLatestSupplier.get();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder class for TestCompletedSnapshotHandleStore. */
    public static class Builder {

        private Builder() {}

        private FunctionWithException<CompletedSnapshotHandle, Void, Exception> addFunction =
                (ignore) -> null;

        private SupplierWithException<Optional<CompletedSnapshotHandle>, Exception>
                getLatestSupplier = Optional::empty;

        public Builder setAddFunction(
                FunctionWithException<CompletedSnapshotHandle, Void, Exception> addFunction) {
            this.addFunction = addFunction;
            return this;
        }

        public Builder setGetLatestSupplier(
                SupplierWithException<Optional<CompletedSnapshotHandle>, Exception>
                        getLatestSupplier) {
            this.getLatestSupplier = getLatestSupplier;
            return this;
        }

        public TestCompletedSnapshotHandleStore build() {
            return new TestCompletedSnapshotHandleStore(addFunction, getLatestSupplier);
        }
    }
}
