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

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Serde of {@link Configuration} . */
public class HadoopConfSerde {

    public static void writeObject(ObjectOutputStream out, Object hadoopConf) throws IOException {
        try {
            Configuration conf = (Configuration) hadoopConf;
            conf.write(out);
        } catch (IOException e) {
            throw new IOException("Failed to serialize Hadoop Configuration: " + e.getMessage(), e);
        }
    }

    public static Configuration readObject(ObjectInputStream in) throws IOException {
        try {
            Configuration hadoopConf = new Configuration();
            hadoopConf.readFields(in);
            return hadoopConf;
        } catch (IOException e) {
            throw new IOException(
                    "Failed to deserialize Hadoop Configuration: " + e.getMessage(), e);
        }
    }
}
