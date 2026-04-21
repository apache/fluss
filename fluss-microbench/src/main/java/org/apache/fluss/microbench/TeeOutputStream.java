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

package org.apache.fluss.microbench;

import java.io.IOException;
import java.io.OutputStream;

/** An output stream that writes to two underlying streams simultaneously. */
public class TeeOutputStream extends OutputStream {
    private final OutputStream primary;
    private final OutputStream secondary;

    public TeeOutputStream(OutputStream primary, OutputStream secondary) {
        this.primary = primary;
        this.secondary = secondary;
    }

    @Override
    public void write(int b) throws IOException {
        primary.write(b);
        secondary.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        primary.write(b, off, len);
        secondary.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        primary.flush();
        secondary.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            primary.close();
        } finally {
            secondary.close();
        }
    }
}
