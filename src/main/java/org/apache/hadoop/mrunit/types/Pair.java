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
package org.apache.hadoop.mrunit.types;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.util.Comparator;

/**
 * A very basic pair type that does not allow null values.
 *
 * @param <S>
 * @param <T>
 */
@SuppressWarnings("unchecked")
public class Pair<S, T> implements Comparable<Pair<S, T>> {

  private final S first;
  private final T second;

  public Pair(final S car, final T cdr) {
    first = returnNonNull(car);
    second = returnNonNull(cdr);
  }

  public S getFirst() {
    return first;
  }

  public T getSecond() {
    return second;
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof Pair) {
      final Pair<S, T> p = (Pair<S, T>) o;
      return first.equals(p.first) && second.equals(p.second);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return first.hashCode() + (second.hashCode() << 1);
  }

  @Override
  public int compareTo(final Pair<S, T> p) {
    final int firstResult = ((Comparable<S>) first).compareTo(p.first);
    if (firstResult == 0) {
      return ((Comparable<T>) second).compareTo(p.second);
    }
    return firstResult;
  }

  public static class FirstElemComparator<S, T> implements Comparator<Pair<S, T>> {
    public FirstElemComparator() {
    }

    @Override
    public int compare(final Pair<S, T> p1, final Pair<S, T> p2) {
      return ((Comparable<S>) p1.first).compareTo(p2.first);
    }
  }

  public static class SecondElemComparator<S, T> implements Comparator<Pair<S, T>> {
    public SecondElemComparator() {
    }

    @Override
    public int compare(final Pair<S, T> p1, final Pair<S, T> p2) {
      return ((Comparable<T>) p1.second).compareTo(p2.second);
    }
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ")";
  }
}
