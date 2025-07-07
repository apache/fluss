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

package org.apache.fluss.config;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.exception.IllegalConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Global configuration object for Fluss. Similar to Java properties configuration objects it
 * includes key-value pairs which represent the framework's configuration.
 */
@Internal
public class GlobalConfiguration {

    static final ConfigOption<String> SERVER_CONFIG_FILE =
            ConfigBuilder.key("configFile").stringType().noDefaultValue();

    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

    public static final String FLUSS_CONF_FILENAME = "server.yaml";

    // --------------------------------------------------------------------------------------------

    private GlobalConfiguration() {}

    /**
     * Load the configuration files from the config file specified by key {@link
     * #SERVER_CONFIG_FILE} in {@code dynamicProperties}. If no config file is specified, it'll load
     * the configuration from the specified {@code defaultConfigDir}.
     *
     * <p>If the {@code dynamicProperties} is not null, then it is added to the loaded
     * configuration.
     *
     * @param defaultConfigDir directory to load the configuration from when no config file is
     *     specified in the dynamic properties
     * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
     * @return The configuration loaded from the given configuration directory
     */
    public static Configuration loadConfiguration(
            final String defaultConfigDir, @Nullable final Configuration dynamicProperties) {

        File yamlConfigFile = null;

        // first, try to get the config file name from the dynamic properties
        // user passed
        if (dynamicProperties != null && dynamicProperties.contains(SERVER_CONFIG_FILE)) {
            // get the config file name passed by user
            String configFileName = dynamicProperties.getString(SERVER_CONFIG_FILE);
            dynamicProperties.removeConfig(SERVER_CONFIG_FILE);
            yamlConfigFile = new File(configFileName);
            if (!yamlConfigFile.exists() && !yamlConfigFile.isFile()) {
                throw new IllegalConfigurationException(
                        "The given configuration file name '"
                                + configFileName
                                + "' ("
                                + yamlConfigFile.getAbsolutePath()
                                + ") does not describe an existing file.");
            }
        }

        if (yamlConfigFile == null) {
            // try to load from the default conf dir
            if (defaultConfigDir == null) {
                throw new IllegalArgumentException(
                        "Given configuration directory is null, cannot load configuration");
            }
            final File confDirFile = new File(defaultConfigDir);
            if (!(confDirFile.exists())) {
                throw new IllegalConfigurationException(
                        "The given configuration directory name '"
                                + defaultConfigDir
                                + "' ("
                                + confDirFile.getAbsolutePath()
                                + ") does not describe an existing directory.");
            }
            // get Fluss yaml configuration file from dir
            yamlConfigFile = new File(confDirFile, FLUSS_CONF_FILENAME);
        }

        Configuration configuration = loadYAMLResource(yamlConfigFile);

        logConfiguration("Loading", configuration);

        if (dynamicProperties != null) {
            logConfiguration("Loading dynamic", dynamicProperties);
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }

    private static void logConfiguration(String prefix, Configuration config) {
        config.confData.forEach(
                (key, value) ->
                        LOG.info(
                                "{} configuration property: {}={}",
                                prefix,
                                key,
                                value instanceof Password ? Password.HIDDEN_CONTENT : value));
    }

    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a
     * single-line comment.
     *
     * <p>Example:
     *
     * <pre>
     * coordinator.rpc.address: localhost # network address for communication with the coordinator server
     * coordinator.rpc.port   : 6123      # network port to connect to for communication with the coordinator server
     * </pre>
     *
     * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML
     * key-value pairs (see issue #113 on GitHub). If at any point in time, there is a need to go
     * beyond simple key-value pairs syntax compatibility will allow to introduce a YAML parser
     * library.
     *
     * @param file the YAML file to read from
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    private static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Line is not a key-value pair (missing space after ':'?)");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": Key or value was empty");
                        continue;
                    }

                    config.setString(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }
}
