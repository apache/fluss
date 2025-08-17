/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.iceberg.actions;

import org.apache.iceberg.DataFile;

import java.util.Collections;
import java.util.List;

/** compaction result holder. */
public class CompactionResult {
    final List<DataFile> addedFiles;
    final List<DataFile> deletedFiles;
    final int bucket;

    CompactionResult(List<DataFile> addedFiles, List<DataFile> deletedFiles, int bucket) {
        this.addedFiles = addedFiles != null ? addedFiles : Collections.emptyList();
        this.deletedFiles = deletedFiles != null ? deletedFiles : Collections.emptyList();
        this.bucket = bucket;
    }
}
