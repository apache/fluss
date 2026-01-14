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
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldAggregator;
import org.apache.fluss.types.DataType;

import java.util.EnumMap;
import java.util.ServiceLoader;

/** Factory interface for creating {@link FieldAggregator} instances. */
public interface FieldAggregatorFactory {

    /**
     * Creates a field aggregator with parameters from AggFunction.
     *
     * @param fieldType the data type of the field
     * @param aggFunction the aggregation function with parameters
     * @return the field aggregator
     */
    FieldAggregator create(DataType fieldType, AggFunction aggFunction);

    /**
     * Returns the unique identifier for this factory.
     *
     * @return the identifier string
     */
    String identifier();

    /**
     * Gets a factory by its aggregation function type.
     *
     * @param type the aggregation function type
     * @return the factory, or null if not found
     */
    static FieldAggregatorFactory getFactory(AggFunctionType type) {
        return FactoryRegistry.INSTANCE.getFactory(type);
    }

    /** Registry for field aggregator factories using Java SPI. */
    class FactoryRegistry {
        private static final FactoryRegistry INSTANCE = new FactoryRegistry();
        private final EnumMap<AggFunctionType, FieldAggregatorFactory> factories;

        private FactoryRegistry() {
            this.factories = new EnumMap<>(AggFunctionType.class);
            loadFactories();
        }

        private void loadFactories() {
            ServiceLoader<FieldAggregatorFactory> loader =
                    ServiceLoader.load(
                            FieldAggregatorFactory.class,
                            FieldAggregatorFactory.class.getClassLoader());
            for (FieldAggregatorFactory factory : loader) {
                // Map factory identifier to AggFunctionType
                AggFunctionType type = AggFunctionType.fromString(factory.identifier());
                if (type != null) {
                    factories.put(type, factory);
                }
            }
        }

        FieldAggregatorFactory getFactory(AggFunctionType type) {
            return factories.get(type);
        }
    }
}
