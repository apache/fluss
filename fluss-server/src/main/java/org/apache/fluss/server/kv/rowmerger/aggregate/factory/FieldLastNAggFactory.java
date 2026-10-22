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

import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastNAgg;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/** Factory for {@link FieldLastNAgg}. */
public class FieldLastNAggFactory implements FieldAggregatorFactory {

    @Override
    public FieldLastNAgg create(DataType fieldType, AggFunction aggFunction) {
        checkArgument(
                fieldType instanceof ArrayType,
                "Data type for last_n column must be 'ArrayType' but was '%s'.",
                fieldType);
        String size = aggFunction.getParameter(AggFunctions.PARAM_SIZE);
        checkArgument(size != null, "Aggregation function last_n requires the 'size' parameter.");
        return new FieldLastNAgg((ArrayType) fieldType, Integer.parseInt(size));
    }

    @Override
    public String identifier() {
        return AggFunctionType.LAST_N.toString();
    }
}
