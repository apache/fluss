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

package org.apache.fluss.lake.iceberg.tiering;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.io.WriteResult;

import java.io.Serializable;

/** The write result of Iceberg lake writer to pass to commiter to commit. */
public class IcebergWriteResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final WriteResult writeResult;

    @Nullable private final RewriteDataFilesActionResult rewriteDataFilesActionResult;

    public IcebergWriteResult(
            WriteResult writeResult,
            @Nullable RewriteDataFilesActionResult rewriteDataFilesActionResult) {
        this.writeResult = writeResult;
        this.rewriteDataFilesActionResult = rewriteDataFilesActionResult;
    }

    public WriteResult getWriteResult() {
        return writeResult;
    }

    @Nullable
    public RewriteDataFilesActionResult getRewriteDataFilesActionResult() {
        return rewriteDataFilesActionResult;
    }

    @Override
    public String toString() {
        return "IcebergWriteResult{"
                + "dataFiles="
                + writeResult.dataFiles().length
                + ", deleteFiles="
                + writeResult.deleteFiles().length
                + '}';
    }
}
