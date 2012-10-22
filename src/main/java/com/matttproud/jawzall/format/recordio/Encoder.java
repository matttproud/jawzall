/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.matttproud.jawzall.format.recordio;

import net.jcip.annotations.ThreadSafe;

import java.nio.ByteBuffer;

/**
 * A means for serializing input into RecordIO-encoded values.
 *
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
@ThreadSafe
public class Encoder {
  public static final int MAX_UNSIGNED_VARINT64_LENGTH = 10;
  private static final long MASK = 128;

  /**
   * Encode an unsigned 32-bit integer for RecordIO.
   *
   * @param value The value to encode.
   * @return The encoded value.
   */
  public ByteBuffer encodeUnsignedVarint32(final long value) {
    final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_UNSIGNED_VARINT64_LENGTH);

    if (value < (1 << 7)) {
      byteBuffer.put(Long.valueOf(value).byteValue());
    } else if (value < (1 << 14)) {
      byteBuffer.put(Long.valueOf(value | MASK).byteValue());
      byteBuffer.put(Long.valueOf(value >> 7).byteValue());
    } else if (value < (1 << 21)) {
      byteBuffer.put(Long.valueOf(value | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 7) | MASK).byteValue());
      byteBuffer.put(Long.valueOf(value >> 14).byteValue());
    } else if (value < (1 << 28)) {
      byteBuffer.put(Long.valueOf(value | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 7) | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 14) | MASK).byteValue());
      byteBuffer.put(Long.valueOf(value >> 21).byteValue());
    } else {
      byteBuffer.put(Long.valueOf(value | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 7) | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 14) | MASK).byteValue());
      byteBuffer.put(Long.valueOf((value >> 21) | MASK).byteValue());
      byteBuffer.put(Long.valueOf(value >> 28).byteValue());
    }

    return byteBuffer;
  }

  /**
   * Encode an unsigned 64-bit integer for RecordIO.
   *
   * @param value The value to encode.
   * @return The encoded value.
   */
  public ByteBuffer encodeUnsignedVarint64(final long value) {
    if (value < (1 << 28)) {
      return encodeUnsignedVarint32(value);
    }

    final ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_UNSIGNED_VARINT64_LENGTH);

    byteBuffer.put(Long.valueOf(value | MASK).byteValue());
    byteBuffer.put(Long.valueOf((value >> 7) | MASK).byteValue());
    byteBuffer.put(Long.valueOf((value >> 14) | MASK).byteValue());
    byteBuffer.put(Long.valueOf((value >> 21) | MASK).byteValue());

    if (value < (1 << 35)) {
      byteBuffer.put(Long.valueOf(value >> 28).byteValue());
      return byteBuffer;
    }

    byteBuffer.put(Long.valueOf((value >> 28) | MASK).byteValue());

    return byteBuffer;
  }
}
