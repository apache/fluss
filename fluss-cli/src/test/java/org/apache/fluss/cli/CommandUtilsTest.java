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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CommandUtils}. */
class CommandUtilsTest {

    @Test
    void testParsePropertiesValid() {
        Map<String, String> props =
                CommandUtils.parseProperties(new String[] {"key1=value1", "key2=value2"});

        assertThat(props).hasSize(2);
        assertThat(props.get("key1")).isEqualTo("value1");
        assertThat(props.get("key2")).isEqualTo("value2");
    }

    @Test
    void testParsePropertiesWithEqualsInValue() {
        Map<String, String> props = CommandUtils.parseProperties(new String[] {"key=val=ue"});

        assertThat(props.get("key")).isEqualTo("val=ue");
    }

    @Test
    void testParsePropertiesNull() {
        Map<String, String> props = CommandUtils.parseProperties(null);

        assertThat(props).isEmpty();
    }

    @Test
    void testParsePropertiesInvalidFormat() {
        assertThatThrownBy(() -> CommandUtils.parseProperties(new String[] {"noequalssign"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid property format");
    }

    @Test
    void testValidateActionsExactlyOne() throws Exception {
        Option createOpt = Option.builder().longOpt("create").build();
        Option listOpt = Option.builder().longOpt("list").build();
        Options opts = new Options();
        opts.addOption(createOpt);
        opts.addOption(listOpt);
        CommandLine cmd = new DefaultParser().parse(opts, new String[] {"--create"});

        // should not throw
        CommandUtils.validateActions(cmd, createOpt, listOpt);
    }

    @Test
    void testValidateActionsNoneThrows() throws Exception {
        Option createOpt = Option.builder().longOpt("create").build();
        Option listOpt = Option.builder().longOpt("list").build();
        Options opts = new Options();
        opts.addOption(createOpt);
        opts.addOption(listOpt);
        CommandLine cmd = new DefaultParser().parse(opts, new String[] {});

        assertThatThrownBy(() -> CommandUtils.validateActions(cmd, createOpt, listOpt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Exactly one of");
    }

    @Test
    void testValidateActionsMultipleThrows() throws Exception {
        Option createOpt = Option.builder().longOpt("create").build();
        Option listOpt = Option.builder().longOpt("list").build();
        Options opts = new Options();
        opts.addOption(createOpt);
        opts.addOption(listOpt);
        CommandLine cmd = new DefaultParser().parse(opts, new String[] {"--create", "--list"});

        assertThatThrownBy(() -> CommandUtils.validateActions(cmd, createOpt, listOpt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Exactly one of");
    }

    @Test
    void testUnwrapExecutionException() {
        ExecutionException e = new ExecutionException(new RuntimeException("root cause"));

        assertThat(CommandUtils.unwrapExceptionMessage(e)).isEqualTo("root cause");
    }

    @Test
    void testUnwrapRegularException() {
        RuntimeException e = new RuntimeException("direct message");

        assertThat(CommandUtils.unwrapExceptionMessage(e)).isEqualTo("direct message");
    }
}
