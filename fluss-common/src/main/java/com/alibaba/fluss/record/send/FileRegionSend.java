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

package com.alibaba.fluss.record.send;

import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;
import com.alibaba.fluss.shaded.netty4.io.netty.channel.FileRegion;

import java.io.IOException;

/**
 * A {@link Send} that backends on a Netty {@link FileRegion}. This leverages the zero-copy of Netty
 * framework that use send_file system call to send a local file to network without copying to user
 * memory.
 */
public class FileRegionSend implements Send {

    private final FileRegion fileRegion;

    public FileRegionSend(FileRegion fileRegion) {
        this.fileRegion = fileRegion;
    }

    @Override
    public void writeTo(ChannelOutboundInvoker out) throws IOException {
        out.write(fileRegion);
    }
}
