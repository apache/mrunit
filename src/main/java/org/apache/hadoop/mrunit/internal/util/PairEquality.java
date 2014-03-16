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
package org.apache.hadoop.mrunit.internal.util;

import java.util.Comparator;

import org.apache.hadoop.mrunit.types.Pair;

public class PairEquality<K, V> {

  private final Comparator<K> keyComparator;
  private final Comparator<V> valueComparator;

  public PairEquality(final Comparator<K> keyComparator,
      final Comparator<V> valueComparator) {
    this.keyComparator = keyComparator;
    this.valueComparator = valueComparator;
  }

  public boolean isTrueFor(final Pair<K, V> o1, Pair<K, V> o2) {
    return equalityOf(o1.getFirst(), o2.getFirst(), keyComparator)
        && equalityOf(o1.getSecond(), o2.getSecond(), valueComparator);
  }

  private <T> boolean equalityOf(final T t1, final T t2, final Comparator<T> c) {
    if (c != null) {
      return c.compare(t1, t2) == 0;
    }
    if (t1 == null && t2 == null) {
      return true;
    }
    if (t1 != null && t2 == null) {
      return false;
    }
    if (t1 == null && t2 != null) {
      return false;
    }
    return t1.equals(t2);
  }
}
