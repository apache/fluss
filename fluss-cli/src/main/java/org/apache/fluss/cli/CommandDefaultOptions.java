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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Base class for CLI command option parsers. Provides common options shared by all CLI tools:
 * {@code --help}, {@code --version}, {@code --bootstrap-server}, {@code --command-config}.
 */
@Internal
public class CommandDefaultOptions {

    private static final Option HELP_OPT =
            Option.builder().longOpt("help").desc("Print usage information.").build();

    private static final Option VERSION_OPT =
            Option.builder().longOpt("version").desc("Display Fluss version.").build();

    private static final Option BOOTSTRAP_SERVER_OPT =
            Option.builder()
                    .longOpt("bootstrap-server")
                    .hasArg()
                    .argName("server")
                    .desc("REQUIRED: The Fluss server to connect to.")
                    .build();

    private static final Option COMMAND_CONFIG_OPT =
            Option.builder()
                    .longOpt("command-config")
                    .hasArg()
                    .argName("file")
                    .desc("Property file containing configs to be passed to the client.")
                    .build();

    protected final Options options;
    protected CommandLine commandLine;

    protected CommandDefaultOptions() {
        this.options = new Options();
        options.addOption(HELP_OPT);
        options.addOption(VERSION_OPT);
        options.addOption(BOOTSTRAP_SERVER_OPT);
        options.addOption(COMMAND_CONFIG_OPT);
    }

    protected void parse(String[] args) throws ParseException {
        this.commandLine = new DefaultParser().parse(options, args);
        if (!hasHelpOption() && !hasVersionOption() && bootstrapServer() == null) {
            throw new ParseException("Missing required option: --bootstrap-server");
        }
    }

    /** Returns {@code true} if the {@code --help} flag was specified. */
    public boolean hasHelpOption() {
        return commandLine.hasOption(HELP_OPT);
    }

    /** Returns {@code true} if the {@code --version} flag was specified. */
    public boolean hasVersionOption() {
        return commandLine.hasOption(VERSION_OPT);
    }

    /** Returns the value of {@code --bootstrap-server}, or {@code null} if not specified. */
    public String bootstrapServer() {
        return commandLine.getOptionValue(BOOTSTRAP_SERVER_OPT);
    }

    /**
     * Loads the properties file specified by {@code --command-config}. Returns an empty {@link
     * Properties} instance if the option was not specified.
     */
    public Properties commandConfig() {
        Properties props = new Properties();
        String file = commandLine.getOptionValue(COMMAND_CONFIG_OPT);
        if (file == null) {
            return props;
        }
        try (FileInputStream fis = new FileInputStream(file)) {
            props.load(fis);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load command config file: " + file, e);
        }
        return props;
    }

    /** Returns the configured {@link Options} for generating help text. */
    public Options options() {
        return options;
    }
}
