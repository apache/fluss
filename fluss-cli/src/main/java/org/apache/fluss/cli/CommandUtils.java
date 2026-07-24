/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.cli;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/** Shared utility methods for CLI commands. */
@Internal
public final class CommandUtils {

    public static final long DEFAULT_TIMEOUT_SECS = 30;

    private CommandUtils() {}

    /** Create a {@link Connection} from common CLI options. */
    public static Connection createConnection(CommandDefaultOptions opts) {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", opts.bootstrapServer());
        Properties cmdConfig = opts.commandConfig();
        cmdConfig.forEach((k, v) -> conf.setString((String) k, (String) v));
        return ConnectionFactory.createConnection(conf);
    }

    /** Validate exactly one action flag is specified. */
    public static void validateActions(CommandLine cmd, Option... actions) {
        long count = Arrays.stream(actions).filter(cmd::hasOption).count();
        if (count != 1) {
            String names =
                    Arrays.stream(actions)
                            .map(o -> "--" + o.getLongOpt())
                            .collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Exactly one of " + names + " must be specified.");
        }
    }

    /** Print help and exit. */
    public static void printUsageAndExit(Options options, String cmdName) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(100);
        formatter.printHelp(cmdName, options, true);
        System.exit(0);
    }

    /** Print version and exit. */
    public static void printVersionAndExit() {
        String version = CommandUtils.class.getPackage().getImplementationVersion();
        if (version == null) {
            version = "(version unknown)";
        }
        System.out.println("Fluss CLI version " + version);
        System.exit(0);
    }

    /** Parse repeatable {@code --property key=value} options into a Map. */
    public static Map<String, String> parseProperties(String[] values) {
        Map<String, String> props = new HashMap<>();
        if (values == null) {
            return props;
        }
        for (String prop : values) {
            String[] kv = prop.split("=", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid property format: '" + prop + "'. Expected key=value.");
            }
            props.put(kv[0].trim(), kv[1].trim());
        }
        return props;
    }

    /** Unwrap {@link ExecutionException} to get the root cause message. */
    public static String unwrapExceptionMessage(Throwable e) {
        if (e instanceof ExecutionException && e.getCause() != null) {
            return e.getCause().getMessage();
        }
        return e.getMessage();
    }
}
