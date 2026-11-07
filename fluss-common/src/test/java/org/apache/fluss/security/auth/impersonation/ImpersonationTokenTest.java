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

package org.apache.fluss.security.auth.impersonation;

import org.apache.fluss.exception.AuthenticationException;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the independent impersonation token format and malformed input handling. */
class ImpersonationTokenTest {

    @Test
    void testRoundTripUnicodeAndOptionalTarget() {
        ImpersonationToken token =
                ImpersonationToken.decode(ImpersonationToken.encode("代理", "密碼\u0000secret", "目标"));
        assertThat(token.username()).isEqualTo("代理");
        assertThat(token.password()).isEqualTo("密碼\u0000secret");
        assertThat(token.targetUser()).isEqualTo("目标");
        assertThat(
                        ImpersonationToken.decode(ImpersonationToken.encode("proxy", "secret", ""))
                                .targetUser())
                .isEmpty();
    }

    @Test
    void testRejectsTruncatedTokens() {
        assertInvalid(null);
        byte[] valid = ImpersonationToken.encode("proxy", "secret", "alice");
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertInvalid(truncated);
        }
    }

    @Test
    void testRejectsUnknownVersionMagicAndTrailingBytes() {
        byte[] valid = ImpersonationToken.encode("proxy", "secret", "alice");
        byte[] unknownVersion = valid.clone();
        unknownVersion[4] = 2;
        assertInvalid(unknownVersion);
        byte[] wrongMagic = valid.clone();
        wrongMagic[0] = 0;
        assertInvalid(wrongMagic);
        assertInvalid(Arrays.copyOf(valid, valid.length + 1));
    }

    @Test
    void testRejectsInvalidLengthsAndUtf8() {
        for (int length : new int[] {-1, 65537, Integer.MAX_VALUE}) {
            byte[] token = ImpersonationToken.encode("proxy", "secret", "alice");
            ByteBuffer.wrap(token).putInt(5, length);
            assertInvalid(token);
        }
        byte[] invalidUtf8 = ImpersonationToken.encode("proxy", "secret", "alice");
        invalidUtf8[9] = (byte) 0xc0;
        assertInvalid(invalidUtf8);
    }

    @Test
    void testRejectsOversizedFieldsAndSaslMessages() {
        char[] oversized = new char[65537];
        Arrays.fill(oversized, 'a');
        assertThatThrownBy(() -> ImpersonationToken.encode("proxy", new String(oversized), "alice"))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Invalid impersonation authentication token");
        assertInvalid("alice\u0000proxy\u0000secret".getBytes(StandardCharsets.UTF_8));
    }

    private static void assertInvalid(byte[] token) {
        assertThatThrownBy(() -> ImpersonationToken.decode(token))
                .isInstanceOf(AuthenticationException.class)
                .hasMessage("Invalid impersonation authentication token");
    }
}
