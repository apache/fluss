/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.conf;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** UT for {@link IcebergConfiguration}. */
class IcebergConfigurationTest {

    @Test
    void testSerde() throws Exception {
        // test when conf is null
        IcebergConfiguration conf = new IcebergConfiguration(null);

        byte[] data = serialize(conf);
        IcebergConfiguration gotConf = deserialize(data);
        assertThat(gotConf.get()).isNull();

        // test when conf is not null
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("k1", "v1");
        hadoopConf.set("k2", "v2");
        conf = new IcebergConfiguration(hadoopConf);
        data = serialize(conf);
        gotConf = deserialize(data);
        Configuration gotHadoopConf = (Configuration) gotConf.get();
        assertThat(gotHadoopConf.get("k1")).isEqualTo("v1");
        assertThat(gotHadoopConf.get("k2")).isEqualTo("v2");
    }

    private byte[] serialize(IcebergConfiguration conf) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(conf);
            return bos.toByteArray();
        }
    }

    private IcebergConfiguration deserialize(byte[] data) throws Exception {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (IcebergConfiguration) ois.readObject();
        }
    }
}
