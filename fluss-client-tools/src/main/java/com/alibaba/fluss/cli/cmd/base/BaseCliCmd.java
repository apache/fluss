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

package com.alibaba.fluss.cli.cmd.base;

import com.alibaba.fluss.cli.FlussCliMain;
import com.beust.jcommander.Strings;
import java.util.Arrays;

/**
 * This is for child sub-command implementation, e.g. subcommands under parent [table] can be
 * ListTableCommand, CreateTableCommand, DropTableCommand.
 */
public abstract class BaseCliCmd<T extends BaseParentCmd> extends BaseCmd {

    @Override
    protected String fullNamePrefix() {
        return Strings.join(" ", Arrays.asList(FlussCliMain.MAIN_CMD, super.fullNamePrefix()));
    }
}
