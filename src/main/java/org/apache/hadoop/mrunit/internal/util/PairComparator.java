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

public class PairComparator<K, V> implements Comparator<Pair<K,V>> {

  private final Comparator<K> keyComparator;
  private final Comparator<V> valueComparator;

  public PairComparator(final Comparator<K> keyComparator,
      final Comparator<V> valueComparator) {
    this.keyComparator = keyComparator;
    this.valueComparator = valueComparator;
  }

  @Override
  public int compare(final Pair<K, V> o1, Pair<K, V> o2) {
    int comparison;
    if (keyComparator != null) {
      comparison = keyComparator.compare(o1.getFirst(), o2.getFirst());
    } else if (o1.getFirst().getClass() != o2.getFirst().getClass()) {
      /* This case needs to be here in order to handle the type unsafety
       * introduced by withInputFromString and withOutputFromString (which are
       * currently marked as deprecated). Once these functions are removed,
       * this case can also be removed.
       */
      return -1;
    } else {
      comparison = ((Comparable<K>) o1.getFirst()).compareTo(o2.getFirst());
    }
    if (comparison != 0) {
      return comparison;
    }
    if (valueComparator != null) {
      return this.valueComparator.compare(o1.getSecond(), o2.getSecond());
    } else if (o1.getSecond().getClass() != o2.getSecond().getClass()) {
      /* This case needs to be here in order to handle the type unsafety
       * introduced by withInputFromString and withOutputFromString (which are
       * currently marked as deprecated). Once these functions are removed,
       * this case can also be removed.
       */
      return -1;
    } else {
      return ((Comparable<V>) o1.getSecond()).compareTo(o2.getSecond());
    }
  }
}
