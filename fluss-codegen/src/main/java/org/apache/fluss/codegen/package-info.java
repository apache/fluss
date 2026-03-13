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
 * Code generation framework for Fluss.
 *
 * <p>This package provides the core infrastructure for runtime code generation using Janino
 * compiler. It enables generating optimized Java code at runtime for performance-critical
 * operations.
 *
 * <h2>Core Components</h2>
 *
 * <ul>
 *   <li>{@link org.apache.fluss.codegen.CodeGeneratorContext} - Manages reusable code fragments and
 *       member variables
 *   <li>{@link org.apache.fluss.codegen.JavaCodeBuilder} - Type-safe builder for constructing Java
 *       source code
 *   <li>{@link org.apache.fluss.codegen.CompileUtils} - Compiles generated source code using Janino
 *   <li>{@link org.apache.fluss.codegen.GeneratedClass} - Wrapper for generated class with source
 *       code and compiled class
 *   <li>{@link org.apache.fluss.codegen.CodeGenException} - Exception for code generation failures
 * </ul>
 *
 * <h2>Sub-packages</h2>
 *
 * <ul>
 *   <li>{@code org.apache.fluss.codegen.types} - API interfaces for generated classes
 *   <li>{@code org.apache.fluss.codegen.generator} - Concrete code generator implementations
 * </ul>
 *
 * <h2>Usage Example</h2>
 *
 * <pre>{@code
 * // Use a specific generator
 * EqualiserCodeGenerator generator = new EqualiserCodeGenerator(rowType);
 * GeneratedClass<RecordEqualiser> generated = generator.generate();
 *
 * // Get the compiled instance
 * RecordEqualiser equaliser = generated.newInstance(classLoader);
 * boolean equal = equaliser.equals(row1, row2);
 * }</pre>
 *
 * @see org.apache.fluss.codegen.generator.EqualiserCodeGenerator
 * @see org.apache.fluss.codegen.types.RecordEqualiser
 */
package org.apache.fluss.codegen;
