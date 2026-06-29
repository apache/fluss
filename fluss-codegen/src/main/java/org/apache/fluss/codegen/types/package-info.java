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

/**
 * API interfaces for code generation.
 *
 * <p>This package contains stable interfaces that define the contract for runtime-generated
 * classes. These interfaces are implemented by dynamically generated code at runtime.
 *
 * <ul>
 *   <li>{@link org.apache.fluss.codegen.types.RecordEqualiser} - Interface for comparing two
 *       InternalRow instances
 * </ul>
 *
 * <p>These interfaces are stable and should not change frequently. New generated class types should
 * add their interfaces here.
 */
package org.apache.fluss.codegen.types;
