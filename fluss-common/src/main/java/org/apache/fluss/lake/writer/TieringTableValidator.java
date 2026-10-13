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

package org.apache.fluss.lake.writer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableInfo;

import java.io.IOException;

/**
 * An optional capability that a {@link LakeTieringFactory} may implement to validate whether a
 * Fluss table can be tiered to the configured lake storage.
 *
 * <p>The validation runs before tiering splits are generated, so an incompatible table only fails
 * that table instead of the whole tiering job. Lake formats that do not implement this interface
 * are tiered without extra validation.
 */
@Internal
public interface TieringTableValidator {

    /**
     * Validates whether the given Fluss table can be tiered to the configured lake storage.
     *
     * @param tableInfo the Fluss table metadata
     * @throws IOException if the validation fails or cannot be completed
     */
    void validateTable(TableInfo tableInfo) throws IOException;
}
