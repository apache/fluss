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

package org.apache.fluss.server.utils;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Utility methods for serializing roaring bitmaps. */
public final class RoaringBitmapUtils {

    private RoaringBitmapUtils() {
        // Utility class, no instantiation
    }

    /**
     * Serializes a 32-bit RoaringBitmap to a byte array using ByteBuffer.
     *
     * <p>Uses ByteBuffer as recommended by the RoaringBitmap Javadoc: "This is the preferred method
     * to serialize to a byte array (byte[])".
     */
    public static byte[] serializeRoaringBitmap32(RoaringBitmap bitmap) throws IOException {
        bitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(bitmap.serializedSizeInBytes());
        bitmap.serialize(buffer);
        return buffer.array();
    }

    public static void deserializeRoaringBitmap32(RoaringBitmap bitmap, byte[] bytes)
            throws IOException {
        bitmap.deserialize(ByteBuffer.wrap(bytes));
    }

    /**
     * Serializes a 64-bit Roaring64Bitmap to a byte array using DataOutputStream.
     *
     * <p>Note: Unlike RoaringBitmap (32-bit), Roaring64Bitmap does not provide a
     * serialize(ByteBuffer) method. It only supports serialize(DataOutput), hence the different
     * serialization strategy.
     */
    public static byte[] serializeRoaringBitmap64(Roaring64Bitmap bitmap) throws IOException {
        bitmap.runOptimize();
        try (ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataOutputStream dataOutput = new DataOutputStream(output)) {
            bitmap.serialize(dataOutput);
            return output.toByteArray();
        }
    }

    public static void deserializeRoaringBitmap64(Roaring64Bitmap bitmap, byte[] bytes)
            throws IOException {
        bitmap.deserialize(ByteBuffer.wrap(bytes));
    }
}
