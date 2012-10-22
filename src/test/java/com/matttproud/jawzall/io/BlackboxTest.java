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
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
public class BlackboxTest {
  private static final int MAXIMUM_SINGLE_ENTRY_SIZE = 1 * 1024 * 1024;
  private static final int MAXIMUM_NUMBER_OF_ENTRIES = 10;

  @Test
  public void testSingleElement() throws IOException {
    for (final Payload expected : Iterables.toIterable(new PayloadGenerator())) {
      final ByteArrayOutputStream out = new ByteArrayOutputStream(MAXIMUM_SINGLE_ENTRY_SIZE);
      final RecordIoOutputStream wrappedOut = new RecordIoOutputStream(out, new Encoder());
      wrappedOut.write(expected.content);

      final ByteArrayInputStream wrappedIn = new ByteArrayInputStream(out.toByteArray());
      final RecordIoInputStream in = new RecordIoInputStream(wrappedIn, new Decoder());
      final byte[] actual = new byte[expected.content.length];
      in.read(actual);

      assertArrayEquals(expected.content, actual);
    }
  }

  @Test
  public void testPluralElements() throws IOException {
    PayloadGenerator payloadGenerator = new PayloadGenerator();

    for (final Integer payloadCount : Iterables.toIterable(PrimitiveGenerators.positiveIntegers(MAXIMUM_NUMBER_OF_ENTRIES))) {
      final Payload[] payloads = new Payload[payloadCount];

      final ByteArrayOutputStream out = new ByteArrayOutputStream(MAXIMUM_SINGLE_ENTRY_SIZE * MAXIMUM_NUMBER_OF_ENTRIES + 1024);
      final RecordIoOutputStream wrappedOut = new RecordIoOutputStream(out, new Encoder());

      for (int i = 0; i < payloadCount; i++) {
        payloads[i] = payloadGenerator.next();
        wrappedOut.write(payloads[i].content);
      }

      final ByteArrayInputStream wrappedIn = new ByteArrayInputStream(out.toByteArray());
      final RecordIoInputStream in = new RecordIoInputStream(wrappedIn, new Decoder());
      final List<Payload> actual = new ArrayList<Payload>();

      for (int i = 0; i < payloadCount; i++) {
        final byte[] incoming = new byte[payloads[i].content.length];
        final int readQuantity = in.read(incoming);

        assertArrayEquals(payloads[i].content, incoming);
      }
    }
  }

  private static class Payload {
    byte[] content;
  }

  private static class PayloadGenerator implements Generator<Payload> {
    Generator<Integer> lengths = PrimitiveGenerators.positiveIntegers(MAXIMUM_SINGLE_ENTRY_SIZE);
    Generator<Byte> contents = PrimitiveGenerators.bytes();

    @Override
    public Payload next() {
      final Payload emission = new Payload();
      final int number = lengths.next();
      emission.content = new byte[number];

      for (int i = 0; i < number; i++) {
        emission.content[i] = contents.next();
      }

      return emission;
    }
  }
}
