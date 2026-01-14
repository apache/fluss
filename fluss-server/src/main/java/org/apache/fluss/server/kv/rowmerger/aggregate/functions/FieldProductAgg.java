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

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

import org.apache.fluss.row.Decimal;
import org.apache.fluss.types.DataType;

import java.math.BigDecimal;

import static org.apache.fluss.row.Decimal.fromBigDecimal;

/** Product aggregator - computes the product of numeric values. */
public class FieldProductAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldProductAgg(DataType dataType) {
        super(dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        Object product;

        // ordered by type root definition
        switch (typeRoot) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                BigDecimal bigDecimal = mergeFieldDD.toBigDecimal();
                BigDecimal bigDecimal1 = inFieldDD.toBigDecimal();
                BigDecimal mul = bigDecimal.multiply(bigDecimal1);
                product = fromBigDecimal(mul, mergeFieldDD.precision(), mergeFieldDD.scale());
                break;
            case TINYINT:
                product = (byte) ((byte) accumulator * (byte) inputField);
                break;
            case SMALLINT:
                product = (short) ((short) accumulator * (short) inputField);
                break;
            case INTEGER:
                product = (int) accumulator * (int) inputField;
                break;
            case BIGINT:
                product = (long) accumulator * (long) inputField;
                break;
            case FLOAT:
                product = (float) accumulator * (float) inputField;
                break;
            case DOUBLE:
                product = (double) accumulator * (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s", typeRoot, this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return product;
    }
}
