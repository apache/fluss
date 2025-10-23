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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.alibaba.fluss.client.admin.Admin;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

public class BaseCmdTest extends BaseTest {

    @Test
    public void testCommandExecution() throws Exception {
        TestParentCmd parentCmd = new TestParentCmd();
        Supplier<Admin> mockAdminSupplier = mock(Supplier.class);
        parentCmd.init(mockAdminSupplier);
        parentCmd.setArgs(new String[] {"test-sub"});
        int result = parentCmd.execute();
        assertEquals(0, result);
    }

    @Test
    public void testInvalidCmdException() throws Exception {
        TestParentCmd parentCmd = new TestParentCmd();
        parentCmd.setArgs(new String[] {"invalid-cmd"});
        Supplier<Admin> mockAdminSupplier = mock(Supplier.class);
        parentCmd.init(mockAdminSupplier);
    }

    @Test
    public void testMissingSubCommand() throws Exception {
        TestParentCmd parentCmd = new TestParentCmd();
        parentCmd.setArgs(new String[] {});
        Supplier<Admin> mockAdminSupplier = mock(Supplier.class);
        parentCmd.init(mockAdminSupplier);
        assertEquals(1, parentCmd.execute());
    }
}
