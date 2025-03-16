/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record.send;

import com.alibaba.fluss.shaded.netty4.io.netty.channel.ChannelOutboundInvoker;

import java.io.IOException;
import java.util.Queue;

/** A {@link Send} that backends on multiple {@link Send}s, sent one after another. */
public class MultiSend implements Send {

    private final Queue<Send> sends;

    public MultiSend(Queue<Send> sends) {
        this.sends = sends;
    }

    @Override
    public void writeTo(ChannelOutboundInvoker out) throws IOException {
        Send current = sends.poll();
        while (current != null) {
            current.writeTo(out);
            current = sends.poll();
        }
    }
}
