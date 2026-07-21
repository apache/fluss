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
 * Concrete code generators for specific use cases.
 *
 * <p>This package contains the actual code generator implementations that produce generated classes
 * at runtime. Each generator is responsible for generating code for a specific purpose.
 *
 * <ul>
 *   <li>{@link org.apache.fluss.codegen.generator.EqualiserCodeGenerator} - Generates {@link
 *       org.apache.fluss.codegen.types.RecordEqualiser} implementations for comparing InternalRow
 *       instances
 * </ul>
 *
 * <p>To add a new code generator:
 *
 * <ol>
 *   <li>Define the interface in {@code org.apache.fluss.codegen.types}
 *   <li>Create the generator class in this package
 *   <li>Use {@link org.apache.fluss.codegen.CodeGeneratorContext} for managing reusable code
 *   <li>Use {@link org.apache.fluss.codegen.JavaCodeBuilder} for building Java source code
 *   <li>Return a {@link org.apache.fluss.codegen.GeneratedClass} from the generator
 * </ol>
 */
package org.apache.fluss.codegen.generator;
