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
import org.apache.fluss.utils.DecimalUtils;

/** Sum aggregator - computes the sum of numeric values. */
public class FieldSumAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    public FieldSumAgg(DataType dataType) {
        super(dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        Object sum;

        // ordered by type root definition
        switch (typeRoot) {
            case DECIMAL:
                Decimal mergeFieldDD = (Decimal) accumulator;
                Decimal inFieldDD = (Decimal) inputField;
                assert mergeFieldDD.scale() == inFieldDD.scale()
                        : "Inconsistent scale of aggregate Decimal!";
                assert mergeFieldDD.precision() == inFieldDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                sum =
                        DecimalUtils.add(
                                mergeFieldDD,
                                inFieldDD,
                                mergeFieldDD.precision(),
                                mergeFieldDD.scale());
                break;
            case TINYINT:
                sum = (byte) ((byte) accumulator + (byte) inputField);
                break;
            case SMALLINT:
                sum = (short) ((short) accumulator + (short) inputField);
                break;
            case INTEGER:
                sum = (int) accumulator + (int) inputField;
                break;
            case BIGINT:
                sum = (long) accumulator + (long) inputField;
                break;
            case FLOAT:
                sum = (float) accumulator + (float) inputField;
                break;
            case DOUBLE:
                sum = (double) accumulator + (double) inputField;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s", typeRoot, this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
        return sum;
    }

    @Override
    public boolean supportsRetract() {
        return true;
    }

    @Override
    public Object retract(Object accumulator, Object retractField) {
        if (retractField == null) {
            return accumulator;
        }
        if (accumulator == null) {
            // A missing contribution starts at zero; it does not cancel the retract input.
            return negative(retractField);
        }

        return subtractByType(accumulator, retractField);
    }

    private Object subtractByType(Object accumulator, Object retractField) {
        switch (typeRoot) {
            case DECIMAL:
                Decimal accDD = (Decimal) accumulator;
                Decimal retDD = (Decimal) retractField;
                assert accDD.scale() == retDD.scale() : "Inconsistent scale of aggregate Decimal!";
                assert accDD.precision() == retDD.precision()
                        : "Inconsistent precision of aggregate Decimal!";
                return DecimalUtils.subtract(accDD, retDD, accDD.precision(), accDD.scale());
            case TINYINT:
                return (byte) ((byte) accumulator - (byte) retractField);
            case SMALLINT:
                return (short) ((short) accumulator - (short) retractField);
            case INTEGER:
                return (int) accumulator - (int) retractField;
            case BIGINT:
                return (long) accumulator - (long) retractField;
            case FLOAT:
                return (float) accumulator - (float) retractField;
            case DOUBLE:
                return (double) accumulator - (double) retractField;
            default:
                String msg =
                        String.format(
                                "Type %s is not supported in %s",
                                typeRoot, this.getClass().getName());
                throw new IllegalArgumentException(msg);
        }
    }

    private Object negative(Object value) {
        switch (typeRoot) {
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return Decimal.fromBigDecimal(
                        decimal.toBigDecimal().negate(), decimal.precision(), decimal.scale());
            case TINYINT:
                return (byte) -((byte) value);
            case SMALLINT:
                return (short) -((short) value);
            case INTEGER:
                return -((int) value);
            case BIGINT:
                return -((long) value);
            case FLOAT:
                return -((float) value);
            case DOUBLE:
                return -((double) value);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Type %s is not supported in %s", typeRoot, getClass().getName()));
        }
    }
}
