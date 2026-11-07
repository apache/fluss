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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/**
 * The impersonation protocol's authentication token: a magic number, version, and three
 * length-prefixed UTF-8 strings containing the account, password, and optional target user.
 */
final class ImpersonationToken {

    private static final int MAGIC = 0x46494d50;
    private static final byte VERSION = 1;
    private static final int MAX_FIELD_LENGTH = 65536;

    private final String username;
    private final String password;
    private final String targetUser;

    private ImpersonationToken(String username, String password, String targetUser) {
        this.username = username;
        this.password = password;
        this.targetUser = targetUser;
    }

    static byte[] encode(String username, String password, String targetUser) {
        byte[] usernameBytes = encodeString(username);
        byte[] passwordBytes = encodeString(password);
        byte[] targetBytes = encodeString(targetUser);
        ByteBuffer buffer =
                ByteBuffer.allocate(
                        17 + usernameBytes.length + passwordBytes.length + targetBytes.length);
        buffer.putInt(MAGIC).put(VERSION);
        putString(buffer, usernameBytes);
        putString(buffer, passwordBytes);
        putString(buffer, targetBytes);
        return buffer.array();
    }

    static ImpersonationToken decode(byte[] token) {
        if (token == null) {
            throw invalidToken();
        }
        ByteBuffer buffer = ByteBuffer.wrap(token);
        if (buffer.remaining() < 5 || buffer.getInt() != MAGIC || buffer.get() != VERSION) {
            throw invalidToken();
        }
        ImpersonationToken decoded =
                new ImpersonationToken(readString(buffer), readString(buffer), readString(buffer));
        if (buffer.hasRemaining()) {
            throw invalidToken();
        }
        return decoded;
    }

    String username() {
        return username;
    }

    String password() {
        return password;
    }

    String targetUser() {
        return targetUser;
    }

    private static byte[] encodeString(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_FIELD_LENGTH) {
            throw invalidToken();
        }
        return bytes;
    }

    private static void putString(ByteBuffer buffer, byte[] bytes) {
        buffer.putInt(bytes.length).put(bytes);
    }

    private static String readString(ByteBuffer buffer) {
        if (buffer.remaining() < 4) {
            throw invalidToken();
        }
        int length = buffer.getInt();
        if (length < 0 || length > MAX_FIELD_LENGTH || length > buffer.remaining()) {
            throw invalidToken();
        }
        ByteBuffer field = buffer.slice();
        field.limit(length);
        buffer.position(buffer.position() + length);
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(field).toString();
        } catch (CharacterCodingException e) {
            throw invalidToken();
        }
    }

    private static AuthenticationException invalidToken() {
        return new AuthenticationException("Invalid impersonation authentication token");
    }
}
