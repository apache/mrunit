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
package org.apache.hadoop.mrunit.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mrunit.internal.io.Serialization;
import org.apache.hadoop.mrunit.types.KeyValueReuseList;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * This Class provides some methods to get inputs compatible with new API reducer.
 *
 * @param <K> The type of the keys.
 * @param <V> The type of the values.
 */
public class ReduceFeeder<K, V> {
  private final Serialization serialization;
  private final Configuration conf;

  /**
   * Constructor
   *
   * @param conf The driver's configuration.
   */
  public ReduceFeeder(Configuration conf){
    this.conf = conf;
    serialization = new Serialization(conf);
  }

  /**
   * This method takes the outputs from a mapper and return the
   * corresponding reducer input where keys have been sorted and
   * grouped according to given comparators.
   *
   * If at least one comparator is null, Keys have to implements Comparable<K>.
   *
   * @param mapOutputs The outputs from mapper
   * @param keyValueOrderComparator The comparator for ordering keys.
   * @param keyGroupComparator The comparator for grouping keys
   * @return key values sorted and grouped for the reducer
   */
  public List<KeyValueReuseList<K,V>> sortAndGroup(final List<Pair<K, V>> mapOutputs,
      final Comparator<K> keyValueOrderComparator,
      final Comparator<K> keyGroupComparator){
    if(mapOutputs.isEmpty()) {
      return Collections.emptyList();
    }
    if (keyValueOrderComparator != null){
      Collections.sort(mapOutputs, new Comparator<Pair<K,V>>(){
        @Override
        public int compare(Pair<K, V> o1, Pair<K, V> o2) {
          return keyValueOrderComparator.compare(o1.getFirst(), o2.getFirst());
        }
      });
    } else {
      Collections.sort(mapOutputs);
    }

    List<KeyValueReuseList<K, V>> groupedInputs = new ArrayList<KeyValueReuseList<K, V>>();

    K currentKey = null;
    KeyValueReuseList<K, V> currentEntries = null;
    for(Pair<K,V> p : mapOutputs){
      if (currentKey == null
                || (keyGroupComparator != null && keyGroupComparator.compare(currentKey, p.getFirst()) != 0)
                || (keyGroupComparator == null && ((Comparable<K>)currentKey).compareTo(p.getFirst()) != 0)) {
        currentKey = p.getFirst();
        currentEntries = new KeyValueReuseList<K, V>(serialization.copy(p.getFirst()), serialization.copy(p.getSecond()), conf);
        groupedInputs.add(currentEntries);
      }
      currentEntries.add(p);
    }
    return groupedInputs;
  }

  /**
   * This method takes the outputs from a mapper and return the
   * corresponding reducer input where keys have been sorted and
   * grouped according to given comparator.
   *
   * If the comparator is null, Keys have to implements Comparable<K>.
   *
   * @param mapOutputs The outputs from mapper
   * @param keyOrderAndGroupComparator The comparator for grouping and ordering keys
   * @return key values sorted and grouped for the reducer
   */
  public List<KeyValueReuseList<K,V>> sortAndGroup(final List<Pair<K, V>> mapOutputs,
	      final Comparator<K> keyOrderAndGroupComparator){
	  return sortAndGroup(mapOutputs, keyOrderAndGroupComparator, keyOrderAndGroupComparator);
  }

  /**
   * This method takes the outputs from a mapper and return the
   * corresponding reducer input where keys have been sorted and
   * grouped according to their natural order.
   *
   * Keys have to implements Comparable<K>.
   *
   * @param mapOutputs The outputs from mapper
   * @return key values sorted and grouped for the reducer
   */
  public List<KeyValueReuseList<K,V>> sortAndGroup(final List<Pair<K, V>> mapOutputs){
	  return sortAndGroup(mapOutputs, null, null);
  }

  /**
   * This method takes a list of (k, v*) and return a list of (k*, v*) where k have been duplicated.
   *
   * @param inputs The list of key values pair with one key and multiples values
   * @return Reducer inputs compatible with the new API
   */
  public List<KeyValueReuseList<K, V>> updateAll(final List<Pair<K, List<V>>> inputs) {
    List<KeyValueReuseList<K, V>> transformedInputs = new ArrayList<KeyValueReuseList<K, V>>();

    for(Pair<K, List<V>> keyValues : inputs){
      if (!keyValues.getSecond().isEmpty()){
        transformedInputs.add(updateInput(keyValues));
      }
    }
    return transformedInputs;
  }

  /**
   * This method takes a (k, v*) input and return a (k*, v*) where k have been duplicated.
   *
   * @param input The key values pair with one key for multiple values
   * @return Reducer input compatible with the new API
   */
  public KeyValueReuseList<K, V> updateInput(final Pair<K, List<V>> input){
	  return updateInput(input.getFirst(), input.getSecond());
  }

  /**
   * This method takes a (k, v*) input and return a (k*, v*) where k have been duplicated.
   *
   * @param input The key values pair with one key for multiple values
   * @return Reducer input compatible with the new API
   */
  public KeyValueReuseList<K, V> updateInput(final K key, List<V> values){
    if (values.isEmpty()){
      return new KeyValueReuseList<K, V>(serialization.copy(key), null, conf);
    }
    KeyValueReuseList<K, V> entry =
      new KeyValueReuseList<K, V>(serialization.copy(key),serialization.copy(values.get(0)), conf);
    for(V value : values){
      entry.add(new Pair<K, V>(serialization.copy(key), serialization.copy(value)));
    }
    return entry;
  }
}
