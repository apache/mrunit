/**
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
package org.apache.hadoop.mrunit.testutil;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

public final class ExtendedAssert {

  private ExtendedAssert() {
  }

  /**
   * Asserts that all the elements of the list are equivalent under equals()
   * 
   * @param expected
   *          a list full of expected values
   * @param actual
   *          a list full of actual test values
   */
  public static void assertListEquals(final List<?> expected,
      final List<?> actual) {
    if (expected.size() != actual.size()) {
      fail("Expected list of size " + expected.size() + "; actual size is "
          + actual.size());
    }

    for (int i = 0; i < expected.size(); i++) {
      final Object t1 = expected.get(i);
      final Object t2 = actual.get(i);

      final boolean same = (t1 == t2) || (t1 != null && t1.equals(t2))
          || (t2 != null && t2.equals(t1));
      if (!same) {
        fail("Expected element " + t1 + " ("
            + ((t2 == null) ? "unknown type" : t2.getClass().getName())
            + ") at index " + i + " != actual element " + t2 + " ("
            + ((t2 == null) ? "unknown type" : t2.getClass().getName()) + ")");
      }
    }
  }

  /**
   * asserts x &gt; y
   */
  public static void assertGreater(final int x, final int y) {
    assertTrue("Expected " + x + " > " + y, x > y);
  }

  /** asserts x &gt;= y) */
  public static void assertGreaterOrEqual(final int x, final int y) {
    assertTrue("Expected " + x + " >= " + y, x >= y);
  }

  /**
   * asserts x &lt; y
   */
  public static void assertLess(final int x, final int y) {
    assertTrue("Expected " + x + " < " + y, x < y);
  }

  /** asserts x &gt;= y) */
  public static void assertLessOrEqual(final int x, final int y) {
    assertTrue("Expected " + x + " <= " + y, x <= y);
  }
}
