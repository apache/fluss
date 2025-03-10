/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.cli;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.config.ConfigurationUtils;
import com.alibaba.fluss.utils.types.Tuple2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Properties;

/**
 * Utility class to extract related parameters from {@link Configuration} and to sanity check them.
 */
public class ConfigurationParserUtils {

    /**
     * Generate configuration from only the config file and dynamic properties.
     *
     * @param args the commandline arguments
     * @return confDir and generated configuration
     */
    public static Tuple2<String, Configuration> getConfigDirAndDynamicConfig(String[] args) {
        // load dynamic properties
        final DefaultParser parser = new DefaultParser();
        Option configOption =
                Option.builder("c")
                        .longOpt("configDir")
                        .required(true)
                        .hasArg(true)
                        .argName("configuration directory")
                        .desc("Directory which contains the configuration file server.yaml.")
                        .build();
        Option dynamicOption =
                Option.builder("D")
                        .argName("property=value")
                        .numberOfArgs(2)
                        .valueSeparator('=')
                        .desc("use value for given property")
                        .build();
        final Options options = new Options().addOption(dynamicOption).addOption(configOption);
        final CommandLine commandLine;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse the command line arguments.", e);
        }
        String confDir = commandLine.getOptionValue(configOption);
        Properties dynamicProperties = commandLine.getOptionProperties(dynamicOption);
        final Configuration dynamicConfiguration =
                ConfigurationUtils.createConfiguration(dynamicProperties);
        return Tuple2.of(confDir, dynamicConfiguration);
    }
}
