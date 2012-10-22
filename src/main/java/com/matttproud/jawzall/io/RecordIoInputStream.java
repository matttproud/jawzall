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
package com.matttproud.jawzall.io;

import com.matttproud.jawzall.format.recordio.Decoder;
import com.matttproud.jawzall.format.recordio.Encoder;
import net.jcip.annotations.NotThreadSafe;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * A means for decoding RecordIO-encoded items from a {@link InputStream}.
 *
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
@NotThreadSafe
public class RecordIoInputStream extends InputStream {
  private final InputStream stream;
  private final Decoder decoder;

  /**
   * Create a decoding stream.
   *
   * @param stream  The stream to decode from.
   * @param decoder The decoder.
   */
  public RecordIoInputStream(final InputStream stream, final Decoder decoder) {
    this.stream = stream;
    this.decoder = decoder;
  }

  /**
   * Read a single entity from the stream.
   *
   * @param b The target for the encoded data.
   * @return The number of bytes read.
   * @throws IOException if an underlying storage or decoding anomaly occurs.
   */
  @Override
  public int read(final byte[] b) throws IOException {
    final ByteBuffer prefix = ByteBuffer.allocate(Encoder.MAX_UNSIGNED_VARINT64_LENGTH);
    do {
      if (prefix.capacity() == 0) {
        throw new IOException("Corrupt record length.");
      }

      final int next = stream.read();

      if (next == -1) {
        if (prefix.capacity() != Encoder.MAX_UNSIGNED_VARINT64_LENGTH) {
          throw new IOException("Corrupt record length at end of stream.");
        }
      }

      prefix.put(Integer.valueOf(next).byteValue());

      if (next < 128) {
        break;
      }
    } while (true);

    prefix.position(0);

    final int size = (int) decoder.decodeUnsignedVarint64(prefix);
    final int readQuantity = stream.read(b, 0, size);

    if (readQuantity < size) {
      throw new IOException("Unknown error.");
    } else if (readQuantity == -1) {
      throw new IOException("Reached the end of stream.");
    }

    return readQuantity;
  }


  /**
   * @throws NotImplementedException if called, because this is only useful for
   *                                 decoding large blocks of data.
   */
  @Override
  public int read() {
    throw new NotImplementedException();
  }
}
