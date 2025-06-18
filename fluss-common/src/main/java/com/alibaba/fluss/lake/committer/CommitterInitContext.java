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

package com.alibaba.fluss.lake.committer;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.metadata.TablePath;

/**
 * The CommitterInitContext interface provides the context needed to create a LakeCommitter. It
 * includes methods to obtain the table path.
 *
 * @since 0.7
 */
@PublicEvolving
public interface CommitterInitContext {

    /**
     * Returns the table path.
     *
     * @return the table path
     */
    TablePath tablePath();
}
