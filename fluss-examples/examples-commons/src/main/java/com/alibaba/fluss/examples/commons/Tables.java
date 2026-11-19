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

package com.alibaba.fluss.examples.commons;

import com.alibaba.fluss.metadata.TablePath;

/**
 * This class serves as a placeholder for the tables used in the e-commerce application. It
 * currently does not contain any fields or methods, but can be extended in the future to include
 * table definitions or other related functionality.
 */
public class Tables {
    public static final String TABLE_BASIC = "basic";

    public static final TablePath BASIC_TABLE_PATH = TablePath.of("examples_db", TABLE_BASIC);
}
