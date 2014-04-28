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

package org.apache.hadoop.mrunit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.types.Pair;

import java.util.*;

public class MapOutputShuffler<K, V> {
  private final Configuration configuration;
  private final Comparator<K> outputKeyComparator;
  private final Comparator<K> outputValueGroupingComparator;

  public MapOutputShuffler(final Configuration configuration,
      final Comparator<K> outputKeyComparator,
      final Comparator<K> outputValueGroupingComparator) {
    this.configuration = configuration;
    this.outputKeyComparator = outputKeyComparator;
    this.outputValueGroupingComparator = outputValueGroupingComparator;
  }

  public List<Pair<K, List<V>>> shuffle(final List<Pair<K, V>> mapOutputs) {

    final Comparator<K> keyOrderComparator;
    final Comparator<K> keyGroupComparator;

    if (mapOutputs.isEmpty()) {
      return Collections.emptyList();
    }

    // JobConf needs the map output key class to work out the
    // comparator to use

    JobConf conf = new JobConf(configuration != null ? configuration
        : new Configuration());
    K firstKey = mapOutputs.get(0).getFirst();
    conf.setMapOutputKeyClass(firstKey.getClass());

    // get the ordering comparator or work out from conf
    if (outputKeyComparator == null) {
      keyOrderComparator = conf.getOutputKeyComparator();
    } else {
      keyOrderComparator = outputKeyComparator;
    }

    // get the grouping comparator or work out from conf
    if (outputValueGroupingComparator == null) {
      keyGroupComparator = conf.getOutputValueGroupingComparator();
    } else {
      keyGroupComparator = outputValueGroupingComparator;
    }

    // sort the map outputs according to their keys
    Collections.sort(mapOutputs, new Comparator<Pair<K, V>>() {
      public int compare(final Pair<K, V> o1, final Pair<K, V> o2) {
        return keyOrderComparator.compare(o1.getFirst(), o2.getFirst());
      }
    });

    // apply grouping comparator to create groups
    final Map<K, List<Pair<K, V>>> groupedByKey = new LinkedHashMap<K, List<Pair<K, V>>>();

    List<Pair<K, V>> groupedKeyList = null;
    Pair<K, V> previous = null;

    for (final Pair<K, V> mapOutput : mapOutputs) {
      if (previous == null
          || keyGroupComparator.compare(previous.getFirst(),
              mapOutput.getFirst()) != 0) {
        groupedKeyList = new ArrayList<Pair<K, V>>();
        groupedByKey.put(mapOutput.getFirst(), groupedKeyList);
      }
      groupedKeyList.add(mapOutput);
      previous = mapOutput;
    }

    // populate output list
    final List<Pair<K, List<V>>> outputKeyValuesList = new ArrayList<Pair<K, List<V>>>();
    for (final Map.Entry<K, List<Pair<K, V>>> groupedByKeyEntry : groupedByKey
        .entrySet()) {

      // create list to hold values for the grouped key
      final List<V> valuesList = new ArrayList<V>();
      for (final Pair<K, V> pair : groupedByKeyEntry.getValue()) {
        valuesList.add(pair.getSecond());
      }

      // add key and values to output list
      outputKeyValuesList.add(new Pair<K, List<V>>(groupedByKeyEntry.getKey(),
          valuesList));
    }

    return outputKeyValuesList;
  }
}
