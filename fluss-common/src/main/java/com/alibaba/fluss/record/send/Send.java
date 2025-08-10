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

import org.apache.fluss.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;

import java.io.IOException;

/** Represents the bytes data for sending to network. */
public interface Send {

    /**
     * Writes the bytes data from this send to the provided channel.
     *
     * @param out The output channel to write out
     * @throws IOException If the write fails
     */
    void writeTo(ChannelOutboundInvoker out) throws IOException;
}
