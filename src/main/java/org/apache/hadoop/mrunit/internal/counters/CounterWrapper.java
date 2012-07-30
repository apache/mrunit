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
package org.apache.hadoop.mrunit.internal.counters;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Wrapper around Counters from both mapred and mapreduce packages so that we
 * can work with both counter classes in the same way.
 */
public class CounterWrapper {

  /**
   * Wrap old mapred counter class
   */
  private org.apache.hadoop.mapred.Counters mapred;

  /**
   * Wrap new mapreduce counter class
   */
  private org.apache.hadoop.mapreduce.Counters mapreduce;

  /**
   * Wrap old counter object
   * 
   * @param counters
   */
  public CounterWrapper(final org.apache.hadoop.mapred.Counters counters) {
    mapred = returnNonNull(counters);
  }

  /**
   * Wrap new counter object
   * 
   * @param counters
   */
  public CounterWrapper(final org.apache.hadoop.mapreduce.Counters counters) {
    mapreduce = returnNonNull(counters);
  }

  /**
   * Get counter value based on Enumeration
   * 
   * @param e
   * @return
   */
  public long findCounterValue(final Enum<?> e) {
    if (mapred != null) {
      return mapred.findCounter(e).getValue();
    } else {
      return mapreduce.findCounter(e).getValue();
    }
  }

  /**
   * Get counter value based on name
   * 
   * @param group
   * @param name
   * @return
   */
  public long findCounterValue(final String group, final String name) {
    if (mapred != null) {
      return mapred.findCounter(group, name).getValue();
    } else {
      return mapreduce.findCounter(group, name).getValue();
    }
  }

  public Collection<String> getGroupNames() {
    if (mapred != null) {
      return mapred.getGroupNames();
    } else {
      return mapreduce.getGroupNames();
    }
  }

  /**
   * @return the name of all counters
   */
  public Collection<Pair<String, String>> findCounterValues() {
    final Collection<Pair<String, String>> counters = new LinkedList<Pair<String, String>>();
    final Collection<String> groupNames = getGroupNames();
    if (mapred != null) {
      for (String groupName : groupNames) {
        final Group group = mapred.getGroup(groupName);
        collectCounters(counters, groupName, group.iterator());
      }
    } else {
      for (String groupName : groupNames) {
        final CounterGroup group = mapreduce.getGroup(groupName);
        collectCounters(counters, groupName, group.iterator());
      }
    }
    return counters;
  }

  /** Collect counters, same for both APIs **/
  private void collectCounters(final Collection<Pair<String, String>> counters,
      String groupName, Iterator<? extends Counter> iterator) {
    while (iterator.hasNext()) {
      String counter = iterator.next().getName();
      counters.add(new Pair<String, String>(groupName, counter));
    }
  }
}
