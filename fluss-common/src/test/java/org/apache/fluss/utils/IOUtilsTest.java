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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.utils.IOUtils}. */
class IOUtilsTest {

    @Test
    void testCloseQuietly() {
        AutoCloseable withException = new CloserWithException();
        AutoCloseable withoutException = new CloserWithoutException();
        // test close one instance
        IOUtils.closeQuietly(withException);
        IOUtils.closeQuietly(withoutException);
        // test close with multi-instances
        IOUtils.closeAllQuietly(Arrays.asList(withException, withoutException));
    }

    @Test
    void testCopyBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("hello".getBytes());
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        ByteArrayOutputStream copyOut = new ByteArrayOutputStream();
        // copy bytes to a output stream
        IOUtils.copyBytes(in, copyOut);
        // then, read bytes from the out out stream
        byte[] buf = new byte[5];
        IOUtils.readFully(new ByteArrayInputStream(out.toByteArray()), buf, 0, 5);

        assertThat(buf).isEqualTo(new byte[] {'h', 'e', 'l', 'l', 'o'});
    }

    @Test
    void testReadFully() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write("hello".getBytes());
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        byte[] buf = new byte[5];
        IOUtils.readFully(in, buf);
        assertThat(buf).isEqualTo(new byte[] {'h', 'e', 'l', 'l', 'o'});

        out = new ByteArrayOutputStream();
        out.write("hello2".getBytes());
        in = new ByteArrayInputStream(out.toByteArray());
        buf = new byte[5];
        IOUtils.readFully(in, buf, 0, 5);
        assertThat(buf).isEqualTo(new byte[] {'h', 'e', 'l', 'l', 'o'});
    }

    private static class CloserWithException implements AutoCloseable {
        @Override
        public void close() throws Exception {
            throw new Exception("Fail to close");
        }
    }

    private static class CloserWithoutException implements AutoCloseable {
        @Override
        public void close() throws Exception {
            // don't throw exception
        }
    }
}
