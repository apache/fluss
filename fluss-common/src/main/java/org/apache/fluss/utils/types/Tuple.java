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

package org.apache.fluss.utils.types;

import org.apache.fluss.annotation.PublicStable;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The base class of all tuples. Tuples have a fix length and contain a set of fields, which may all
 * be of different types.
 *
 * <p>The fields in the tuples may be accessed directly a public fields.
 *
 * <p>Tuples are in principle serializable. However, they may contain non-serializable fields, in
 * which case serialization will fail.
 *
 * @since 0.1
 */
@PublicStable
public abstract class Tuple implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    public abstract <T extends Tuple> T copy();
}
