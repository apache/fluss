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

package org.apache.fluss.connector.trino.module;

import org.apache.fluss.connector.trino.FlussConnector;
import org.apache.fluss.connector.trino.FlussMetadata;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * Guice module for Fluss metadata components.
 */
public class FlussMetadataModule implements Module {

    @Override
    public void configure(Binder binder) {
        // Bind core connector
        binder.bind(FlussConnector.class).in(Scopes.SINGLETON);
        
        // Bind metadata components
        binder.bind(FlussMetadata.class).in(Scopes.SINGLETON);
    }
}
