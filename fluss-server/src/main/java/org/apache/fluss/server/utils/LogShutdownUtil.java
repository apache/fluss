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

package org.apache.fluss.server.utils;

/** Utility class to shut down log system. */
public class LogShutdownUtil {

    public static void shutdownLogIfPossible() {
        // To avoid binding to a specific logging implementation, we use reflection here.
        try {
            // To ensure that logs within the JVM shutdown hook can be printed when using Log4j2,
            // Fluss has disabled Log4j2's shutdown hook; therefore, manual shutdown is required
            // here.
            Class<?> logManager = Class.forName("org.apache.logging.log4j.LogManager");
            logManager.getMethod("shutdown").invoke(null);
        } catch (ClassNotFoundException e) {
            System.err.println("Class org.apache.logging.log4j.LogManager not found");
        } catch (Exception e) {
            System.err.println(
                    "Error to invoke shutdown method of org.apache.logging.log4j.LogManager");
        }
    }
}
