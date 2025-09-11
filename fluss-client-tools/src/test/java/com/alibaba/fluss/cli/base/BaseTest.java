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

package com.alibaba.fluss.cli.base;

import com.alibaba.fluss.cli.cmd.base.BaseCliCmd;
import com.alibaba.fluss.cli.cmd.base.BaseParentCmd;
import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class BaseTest {

    protected final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    protected final PrintStream originalOut = System.out;

    @BeforeEach
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    @Parameters(commandNames = "test-parent")
    public static class TestParentCmd extends BaseParentCmd {
        @Parameter(
                names = {"--test-param1", "-b"},
                description = "Test params.")
        private String testParam1;

        @Parameter(names = {"--test-enum"})
        private TestType type = TestType.T1;

        @DynamicParameter(names = "-D", description = "Dynamic parameters")
        private Map<String, String> params = new HashMap<>();
    }

    @Parameters(commandNames = "test-sub")
    public static class TestSubCmd extends BaseCliCmd<BaseCmdTest.TestParentCmd> {

        @Override
        protected int execute() throws Exception {
            return 0;
        }
    }

    public enum TestType {
        T1,
        T2
    }
}
