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

import com.matttproud.jawzall.format.recordio.Encoder;
import net.jcip.annotations.NotThreadSafe;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A means for encoding items into a RecordIO stream.
 *
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
@NotThreadSafe
public class RecordIoOutputStream extends OutputStream {
  private final OutputStream stream;
  private final Encoder encoder;

  /**
   * Create the encoding stream.
   *
   * @param stream  The stream into which to dump values.
   * @param encoder The encoder.
   */
  public RecordIoOutputStream(final OutputStream stream, final Encoder encoder) {
    this.stream = stream;
    this.encoder = encoder;
  }

  /**
   * @throws NotImplementedException if called, because this is only useful for
   *                                 encoding large blocks of data.
   */
  @Override
  public void write(int b) throws IOException {
    throw new NotImplementedException();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    final int recordLength = b.length;
    final ByteBuffer encodedLength = encoder.encodeUnsignedVarint64(recordLength);
    stream.write(encodedLength.array(), 0, encodedLength.position());
    stream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
