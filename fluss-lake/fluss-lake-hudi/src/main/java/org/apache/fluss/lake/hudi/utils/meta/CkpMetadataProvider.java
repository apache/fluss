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

package org.apache.fluss.lake.hudi.utils.meta;

import org.apache.fluss.lake.hudi.tiering.HudiWriteTableInfo;
import org.apache.fluss.metadata.TablePath;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Provides table-scoped Hudi checkpoint metadata. */
public class CkpMetadataProvider implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient volatile Map<TablePath, CkpMetadata> ckpMetadatas;

    public CkpMetadata get(TablePath tablePath, HudiWriteTableInfo hudiTableInfo)
            throws IOException {
        Map<TablePath, CkpMetadata> metadataCache = getMetadataCache();
        CkpMetadata ckpMetadata = metadataCache.get(tablePath);
        if (ckpMetadata != null) {
            return ckpMetadata;
        }
        synchronized (this) {
            metadataCache = getMetadataCache();
            ckpMetadata = metadataCache.get(tablePath);
            if (ckpMetadata == null) {
                ckpMetadata = CkpMetadataFactory.getCkpMetadata(hudiTableInfo.getFlinkConfig());
                metadataCache.put(tablePath, ckpMetadata);
            }
            return ckpMetadata;
        }
    }

    private Map<TablePath, CkpMetadata> getMetadataCache() {
        Map<TablePath, CkpMetadata> metadataCache = ckpMetadatas;
        if (metadataCache != null) {
            return metadataCache;
        }
        synchronized (this) {
            if (ckpMetadatas == null) {
                ckpMetadatas = new ConcurrentHashMap<>();
            }
            return ckpMetadatas;
        }
    }
}
