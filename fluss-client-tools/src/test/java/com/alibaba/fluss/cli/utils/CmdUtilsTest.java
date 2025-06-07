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

package com.alibaba.fluss.cli.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.fluss.cli.base.BaseTest;
import com.alibaba.fluss.client.admin.Admin;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

public class CmdUtilsTest extends BaseTest {
    @Test
    public void testRegisterCommand() {
        Supplier<Admin> mockAdminSupplier = mock(Supplier.class);
        JCommander mockJCommander = mock(JCommander.class);
        JCommander subCommander = mock(JCommander.class);
        when(mockJCommander.findCommandByAlias("test-sub")).thenReturn(subCommander);
        Predicate<Class<?>> cmdPredicate =
                clazz -> clazz.getAnnotation(Parameters.class).commandNames()[0].equals("test-sub");
        CmdUtils.registerCommand(mockAdminSupplier, mockJCommander, cmdPredicate);
        verify(mockJCommander, times(1)).addCommand(eq("test-sub"), any(TestSubCmd.class));
    }
}
