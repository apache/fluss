/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.cli.format;

/** Utility class that contains all strings for CLI commands and messages. */
public final class CliStrings {

    private CliStrings() {
        // private
    }

    public static final String MESSAGE_WELCOME;

    // make findbugs happy
    static {
        MESSAGE_WELCOME =
                "______ _                 _____ _ _            _   BETA\n"
                        + "|  ___| |               /  __ \\ (_)          | |  \n"
                        + "| |_  | |_   _ ___ ___  | /  \\/ |_  ___ _ __ | |_ \n"
                        + "|  _| | | | | / __/ __| | |   | | |/ _ \\ '_ \\| __|\n"
                        + "| |   | | |_| \\__ \\__ \\ | \\__/\\ | |  __/ | | | |_ \n"
                        + "\\_|   |_|\\__,_|___/___/  \\____/_|_|\\___|_| |_|\\__|\n"
                        + "          \n"
                        + "        Welcome to Fluss Client! \nFluss CLI: v0.1.0 Runtime Version: Fluss Core 0.7.0\n";
    }

    public static void print() {
        System.out.println(MESSAGE_WELCOME);
    }

    public static void printVersion() {
        System.out.println("Fluss CLI: v0.1.0\nRuntime Version: Fluss Core 0.7.0\n");
    }
}
