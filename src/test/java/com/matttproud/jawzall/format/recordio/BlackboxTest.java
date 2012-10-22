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

import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
public class BlackboxTest {

  @Test
  public void testEncoderDecoder() {
    for (Integer i : Iterables.toIterable(PrimitiveGenerators.positiveIntegers())) {
      final ByteBuffer encoded = new Encoder().encodeUnsignedVarint64(i);
      encoded.position(0);
      final Long decoded = new Decoder().decodeUnsignedVarint64(encoded);


      assertEquals(Long.valueOf(i), decoded);
    }
  }
}
