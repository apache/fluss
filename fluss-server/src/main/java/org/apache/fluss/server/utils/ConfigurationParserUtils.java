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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.ConfigurationUtils;
import org.apache.fluss.config.GlobalConfiguration;
import org.apache.fluss.server.cli.CommandLineParser;
import org.apache.fluss.server.cli.ServerConfiguration;
import org.apache.fluss.server.cli.ServerConfigurationParserFactory;
import org.apache.fluss.server.exception.FlussParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to extract related parameters from {@link Configuration} and to sanity check them.
 */
public class ConfigurationParserUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationParserUtils.class);

    /**
     * Generate configuration from only the config file and dynamic properties.
     *
     * @param args the commandline arguments
     * @param cmdLineSyntax the syntax for this application
     * @return generated configuration
     * @throws FlussParseException if the configuration cannot be generated
     */
    public static Configuration loadCommonConfiguration(String[] args, String cmdLineSyntax)
            throws FlussParseException {
        final CommandLineParser<ServerConfiguration> commandLineParser =
                new CommandLineParser<>(new ServerConfigurationParserFactory());

        final ServerConfiguration serverConfiguration;

        try {
            serverConfiguration = commandLineParser.parse(args);
        } catch (FlussParseException e) {
            LOG.error("Could not parse the command line options.", e);
            commandLineParser.printHelp(cmdLineSyntax);
            throw e;
        }

        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(serverConfiguration.getDynamicProperties());
        return GlobalConfiguration.loadConfiguration(
                serverConfiguration.getConfigDir(), dynamicProperties);
    }
}
