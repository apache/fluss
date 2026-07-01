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

import static com.beust.jcommander.DefaultUsageFormatter.s;

import com.alibaba.fluss.annotation.Internal;
import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;

/** Commander Factory to create a {@link JCommander}. */
@Internal
public class CommanderFactory {

    public static JCommander createCommander(String name, Object cmdObj) {
        return createCommander(name, StringUtils.EMPTY, cmdObj);
    }

    public static JCommander createCommander(String name, String parentCmdName, Object cmdObj) {
        String fullName =
                StringUtils.isNotEmpty(parentCmdName) ? parentCmdName + s(1) + name : name;
        JCommander jc = JCommander.newBuilder().addObject(cmdObj).programName(fullName).build();
        jc.setUsageFormatter(new CmdUsageFormat(jc));
        return jc;
    }
}
