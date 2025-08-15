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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

public class CommanderFactoryTest {

    @Test
    public void testCreateCommander() {
        Object cmdObj = new Object();
        JCommander jc1 = CommanderFactory.createCommander("test", cmdObj);
        assertEquals("test", jc1.getProgramName());
        JCommander jc2 = CommanderFactory.createCommander("test", "parent", cmdObj);
        assertEquals("parent test", jc2.getProgramName());
    }
}
