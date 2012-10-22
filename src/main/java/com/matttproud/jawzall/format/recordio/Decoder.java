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
import java.util.HashMap;
import java.util.Map;

/**
 * A means for deserializing RecordIO-encoded values.
 *
 * @author matt.proud@gmail.com (Matt T. Proud)
 */
@ThreadSafe
public class Decoder {
  private static final int MASK = 127;

  /**
   * Derive an unsigned 64-bit integer from RecordIO encoding.
   *
   * @param input The unprocessed input.
   * @return The output.
   */
  public long decodeUnsignedVarint64(final ByteBuffer input) {
    int fragmentCount = 0;
    long fragmentLedger = 0;
    int fragmentSequence = 0;

    Map<Integer, Long> fragmentLedgers = new HashMap<Integer, Long>();
    long result = 0;

    do {
      /*
       * N.B.(matt): Java byte primitives are signed, thusly along interval
       * from -127 to 127.
       */
      final int current = (input.get() & 0xFF);

      if (fragmentSequence == 0) {
        fragmentLedger = current & MASK;
      } else if (fragmentSequence == 1) {
        fragmentLedger |= (current & MASK) << 7;
      } else if (fragmentSequence == 2) {
        fragmentLedger |= (current & MASK) << 14;
      } else if (fragmentSequence == 3) {
        fragmentLedger |= (current & MASK) << 21;
      }

      fragmentLedgers.put(fragmentCount, fragmentLedger);

      if (fragmentSequence == 3) {
        fragmentLedger = 0;
        fragmentSequence = 0;
        fragmentCount++;

        continue;
      }

      if (current < 128) {
        break;
      }

      fragmentSequence++;
    } while (input.position() < input.capacity());

    if (fragmentCount == 0) {
      return fragmentLedgers.get(fragmentCount);
    } else if (fragmentCount == 1) {
      return fragmentLedgers.get(0) | (fragmentLedgers.get(1) << 28);
    }

    return fragmentLedgers.get(0) | (fragmentLedgers.get(1) << 28) | (fragmentLedgers.get(2) << 56);
  }
}
