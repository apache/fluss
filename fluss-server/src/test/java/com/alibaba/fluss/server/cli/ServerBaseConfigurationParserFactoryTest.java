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

package com.alibaba.fluss.server.cli;

import com.alibaba.fluss.server.exception.FlussParseException;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link com.alibaba.fluss.server.cli.ServerConfigurationParserFactory}. */
class ServerBaseConfigurationParserFactoryTest {

    private static final CommandLineParser<ServerConfiguration> commandLineParser =
            new CommandLineParser<>(new ServerConfigurationParserFactory());

    @Test
    void testEntrypointClusterConfigurationParsing() throws FlussParseException {
        final String configDir = "/foo/bar";
        final String key = "key";
        final String value = "value";
        final String arg1 = "arg1";
        final String arg2 = "arg2";
        final String[] args = {
            "--configDir", configDir, String.format("-D%s=%s", key, value), arg1, arg2
        };

        final ServerConfiguration serverConfiguration = commandLineParser.parse(args);

        assertThat(serverConfiguration.getConfigDir()).isEqualTo(configDir);
        final Properties dynamicProperties = serverConfiguration.getDynamicProperties();

        assertThat(dynamicProperties).containsEntry(key, value);

        assertThat(serverConfiguration.getArgs()).containsExactly(arg1, arg2);
    }

    @Test
    void testOnlyRequiredArguments() throws FlussParseException {
        final String configDir = "/foo/bar";
        final String[] args = {"--configDir", configDir};

        final ServerConfiguration clusterConfiguration = commandLineParser.parse(args);

        assertThat(clusterConfiguration.getConfigDir()).isEqualTo(configDir);
    }

    @Test
    void testMissingRequiredArgument() {
        final String[] args = {};

        assertThatThrownBy(() -> commandLineParser.parse(args))
                .isInstanceOf(FlussParseException.class);
    }
}
