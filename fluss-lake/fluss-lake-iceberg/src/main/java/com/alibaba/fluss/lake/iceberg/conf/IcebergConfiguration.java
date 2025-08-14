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

package com.alibaba.fluss.lake.iceberg.conf;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.Configuration;

import org.apache.iceberg.common.DynClasses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wraps the hadoop configuration used to configure {@link org.apache.iceberg.catalog.Catalog} if
 * hadoop related classes is available.
 *
 * <p>It don't declare Hadoop configuration explicitly for some catalogs won't need hadoop
 * configuration. For these catalogs, it won't throw class not found exception. It set the conf to
 * null if no hadoop dependencies are found. It's fine to use null for the catalogs don't require
 * Hadoop configuration.
 *
 * <p>For the catalogs require Hadoop configuration, hadoop related class not found exception will
 * be thrown which guides users to add hadoop related classes.
 */
public class IcebergConfiguration implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConfiguration.class);

    private transient Object conf;

    @VisibleForTesting
    protected IcebergConfiguration(Object conf) {
        this.conf = conf;
    }

    public static IcebergConfiguration from(Configuration flussConfig) {
        return new IcebergConfiguration(loadHadoopConfig(flussConfig));
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        if (conf == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            HadoopConfSerde.writeObject(out, conf);
        }
    }

    private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
        in.defaultReadObject();
        boolean configIsNotNull = in.readBoolean();
        if (configIsNotNull) {
            conf = HadoopConfSerde.readObject(in);
        } else {
            conf = null;
        }
    }

    private static Object loadHadoopConfig(Configuration flussConfig) {
        Class<?> configClass =
                DynClasses.builder()
                        .impl("org.apache.hadoop.hdfs.HdfsConfiguration")
                        .impl("org.apache.hadoop.conf.Configuration")
                        .orNull()
                        .build();

        if (configClass == null) {
            LOG.info("Hadoop not found on classpath, not creating Hadoop config");
            return null;
        }

        return HadoopUtils.getHadoopConfiguration(flussConfig);
    }

    public Object get() {
        return conf;
    }
}
