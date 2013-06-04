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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.internal.io.Serialization;

/**
 * A List of Pair<K, V> to store a reducer input (k, v)* with keys equivalent for a grouping comparator.
 *
 * @param <K> The type of keys
 * @param <V> The type of Values
 */
public class KeyValueReuseList<K, V> extends ArrayList<Pair<K,V>>{
  private static final long serialVersionUID = 1192028416166671770L;
  private final K keyContainer;
  private final V valueContainer;
  private final Serialization serialization;

  public KeyValueReuseList(K keyContainer, V valueContainer, Configuration conf){
    super();
    serialization = new Serialization(conf);
    this.keyContainer = keyContainer;
    this.valueContainer = valueContainer;
  }

  public K getCurrentKey() {
    return keyContainer;
  }

  public KeyValueReuseList<K, V> clone(Configuration conf){
    K key = serialization.copy(keyContainer);
    V value = serialization.copy(valueContainer);
    KeyValueReuseList<K, V> clone = new KeyValueReuseList<K, V>(key, value, conf);
    for (Pair<K, V> pair : this) {
      clone.add(new Pair<K, V>(serialization.copy(pair.getFirst()), serialization.copy(pair.getSecond())));
    }
    return clone;
  }

  public Iterator<V> valueIterator() {
    return new Iterator<V>() {
      private K key = keyContainer;
      private V value = valueContainer;
      private final Iterator<Pair<K, V>> delegate = iterator();

      @Override
      public boolean hasNext() {
        return delegate.hasNext();
      }

      @Override
      public V next() {
        Pair<K, V> p = delegate.next();
        key = serialization.copy(p.getFirst(), key);
        value = serialization.copy(p.getSecond(), value);
        return value;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
