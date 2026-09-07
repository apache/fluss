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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;

/**
 * Thrown at {@code CREATE TABLE} time when a column-group declaration is structurally invalid — for
 * example, when a partition-key column is included in a {@code column-groups.<g>} property, when a
 * group references an unknown column, when membership is duplicated across groups, or when a group
 * is paired with a primary key. Detected before the table is created; the error message identifies
 * the offending column or group so the user can correct the DDL.
 *
 * @since 0.10
 */
@PublicEvolving
public class InvalidColumnGroupConfigException extends ApiException {
    public InvalidColumnGroupConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidColumnGroupConfigException(String message) {
        super(message);
    }

    public InvalidColumnGroupConfigException(Throwable cause) {
        super(cause);
    }
}
