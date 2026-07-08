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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.TablePath;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * The LakeStorage interface defines how to implement lakehouse storage system such as Paimon and
 * Iceberg. It provides a method to create a lake tiering factory.
 *
 * @since 0.7
 */
@PublicEvolving
public interface LakeStorage {

    /**
     * Creates a lake tiering factory to create lake writers and committers.
     *
     * @return the lake tiering factory
     */
    LakeTieringFactory<?, ?> createLakeTieringFactory();

    /** Create lake catalog. */
    LakeCatalog createLakeCatalog();

    /**
     * Creates a lake source instance for reading lakehouse data from the specified table path. The
     * lake source provides capabilities for split planning and record reading, enabling efficient
     * distributed processing of lakehouse data.
     *
     * @param tablePath the logical path identifying the table in the lakehouse storage
     * @return a configured lake source instance for the specified table
     */
    LakeSource<?> createLakeSource(TablePath tablePath);

    /**
     * Creates a table-level point lookuper for the specified lake table.
     *
     * @param tablePath the logical path identifying the table in the lakehouse storage
     * @param context runtime context for creating the lookuper
     * @return a table-level point lookuper
     */
    default LakeTableLookuper createLakeTableLookuper(
            TablePath tablePath, LookuperContext context) {
        throw new UnsupportedOperationException(
                "Point lookup is not supported for this lake storage.");
    }

    /** Runtime context for creating a lake table lookuper. */
    final class LookuperContext {
        private final String ioTmpDir;

        /**
         * Creates a lookuper context.
         *
         * @param ioTmpDir local directory for temporary files used by the lookuper
         */
        public LookuperContext(String ioTmpDir) {
            this.ioTmpDir = checkNotNull(ioTmpDir, "ioTmpDir must not be null.");
        }

        /** Returns the local directory for temporary files used by the lookuper. */
        public String ioTmpDir() {
            return ioTmpDir;
        }
    }
}
