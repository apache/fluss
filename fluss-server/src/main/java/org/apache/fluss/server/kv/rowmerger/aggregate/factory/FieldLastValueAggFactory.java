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

package org.apache.fluss.server.kv.rowmerger.aggregate.factory;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastValueAgg;
import org.apache.fluss.types.DataType;

/** Factory for {@link FieldLastValueAgg}. */
public class FieldLastValueAggFactory implements FieldAggregatorFactory {

    @Override
    public FieldLastValueAgg create(DataType fieldType, AggFunction aggFunction) {
        return new FieldLastValueAgg(fieldType);
    }

    @Override
    public String identifier() {
        return AggFunctionType.LAST_VALUE.toString();
    }
}
