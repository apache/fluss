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

package org.apache.fluss.flink.tiering;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.plugin.Plugin;

/**
 * A Plugin to create instances of {@link LakeTieringDecorator}.
 *
 * <p>This plugin mechanism allows different environments (e.g., internal vs. public cloud) to
 * provide their own decorator implementations for customizing Flink execution environment.
 *
 * <p>Multiple different plugin implementations can be loaded and applied simultaneously. However,
 * the loading order of plugins is not guaranteed and may vary between runs. Plugin implementations
 * should not depend on the loading order and should be designed to work correctly regardless of
 * when they are applied relative to other plugins.
 *
 * @since 0.9
 */
@PublicEvolving
public interface LakeTieringDecoratorPlugin extends Plugin {

    /**
     * Returns a unique identifier among {@link LakeTieringDecoratorPlugin} implementations.
     *
     * @return the identifier
     */
    String identifier();

    /**
     * Creates a new instance of {@link LakeTieringDecorator}.
     *
     * @return the lake tiering decorator instance
     */
    LakeTieringDecorator createLakeTieringDecorator();
}
