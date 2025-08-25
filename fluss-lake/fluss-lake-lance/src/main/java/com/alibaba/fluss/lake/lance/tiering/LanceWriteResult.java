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

<<<<<<<< HEAD:fluss-lake/fluss-lake-lance/src/main/java/com/alibaba/fluss/lake/lance/tiering/LanceWriteResult.java
package com.alibaba.fluss.lake.lance.tiering;
========
package org.apache.fluss.flink.lakehouse.paimon.reader;
>>>>>>>> c4d07399 ([INFRA] The project package name updated to org.apache.fluss.):fluss-flink/fluss-flink-common/src/main/java/org/apache/fluss/flink/lakehouse/paimon/reader/KeyValueRow.java

import com.lancedb.lance.FragmentMetadata;

import java.io.Serializable;
import java.util.List;

/** The write result of Lance lake writer to pass to commiter to commit. */
public class LanceWriteResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<FragmentMetadata> commitMessage;

    public LanceWriteResult(List<FragmentMetadata> commitMessage) {
        this.commitMessage = commitMessage;
    }

    public List<FragmentMetadata> commitMessage() {
        return commitMessage;
    }
}
