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

import com.alibaba.fluss.cli.format.CommanderFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;

/** This is base cmd, both parent cmd and sub command should extend this class. */
public abstract class BaseCmd {

    protected Supplier<Admin> adminSupplier;

    protected JCommander cmdJc;

    public BaseCmd() {
        this.cmdJc = CommanderFactory.createCommander(getCmdName(), fullNamePrefix(), this);
    }

    protected abstract int execute() throws Exception;

    public void printUsage() {
        cmdJc.usage();
    }

    public Admin getAdmin() {
        return adminSupplier.get();
    }

    protected String getCmdName() {
        return getCmdName(this.getClass());
    }

    protected String getCmdName(Class<?> clazz) {
        return clazz.getAnnotation(Parameters.class).commandNames()[0];
    }

    protected String fullNamePrefix() {
        Type type = this.getClass().getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type[] actualTypeArgs = pt.getActualTypeArguments();
            if (actualTypeArgs.length > 0) {
                Class<?> actualClazz = (Class<?>) actualTypeArgs[0];
                String[] commandNames = actualClazz.getAnnotation(Parameters.class).commandNames();
                return commandNames.length > 0 ? commandNames[0] : StringUtils.EMPTY;
            }
        }
        return StringUtils.EMPTY;
    }

    public void setArgs(String[] args) {}

    public void init(Supplier<Admin> adminSupplier) {
        this.adminSupplier = adminSupplier;
    }
}
