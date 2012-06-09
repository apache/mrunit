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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Harness that allows you to test a Mapper and a Reducer instance together You
 * provide the input key and value that should be sent to the Mapper, and
 * outputs you expect to be sent by the Reducer to the collector for those
 * inputs. By calling runTest(), the harness will deliver the input to the
 * Mapper, feed the intermediate results to the Reducer (without checking them),
 * and will check the Reducer's outputs against the expected results. This is
 * designed to handle a single (k, v)* -> (k, v)* case from the Mapper/Reducer
 * pair, representing a single unit test.
 */

public abstract class MapReduceDriverBase<K1, V1, K2, V2, K3, V3> extends
    TestDriver<K1, V1, K3, V3> {

  public static final Log LOG = LogFactory.getLog(MapReduceDriverBase.class);

  protected List<Pair<K1, V1>> inputList = new ArrayList<Pair<K1, V1>>();

  /** Key group comparator */
  protected Comparator<K2> keyGroupComparator;

  /** Key value order comparator */
  protected Comparator<K2> keyValueOrderComparator;

  /**
   * Adds an input to send to the mapper
   * 
   * @param key
   * @param val
   */
  public void addInput(final K1 key, final V1 val) {
    inputList.add(copyPair(key, val));
  }

  /**
   * Adds an input to send to the Mapper
   * 
   * @param input
   *          The (k, v) pair to add to the input list.
   */
  public void addInput(final Pair<K1, V1> input) {
    addInput(input.getFirst(), input.getSecond());
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * 
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(final Pair<K3, V3> outputRecord) {
    addOutput(outputRecord.getFirst(), outputRecord.getSecond());
  }

  /**
   * Adds a (k, v) pair we expect as output from the Reducer
   * 
   * @param key
   * @param val
   */
  public void addOutput(final K3 key, final V3 val) {
    expectedOutputs.add(copyPair(key, val));
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper input types to
   * Text.
   * 
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addInputFromString(final String input) {
    addInput((Pair<K1, V1>) parseTabbedPair(input));
  }

  /**
   * Expects an input of the form "key \t val" Forces the Reducer output types
   * to Text.
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addOutputFromString(final String output) {
    addOutput((Pair<K3, V3>) parseTabbedPair(output));
  }

  @Override
  public abstract List<Pair<K3, V3>> run() throws IOException;

  @Override
  public void runTest(final boolean orderMatters) {
    try {
      final List<Pair<K3, V3>> reduceOutputs = run();
      validate(reduceOutputs, orderMatters);
      validate(counterWrapper);
    } catch (final IOException ioe) {
      LOG.error(ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Take the outputs from the Mapper, combine all values for the same key, and
   * sort them by key.
   * 
   * @param mapOutputs
   *          An unordered list of (key, val) pairs from the mapper
   * @return the sorted list of (key, list(val))'s to present to the reducer
   */
  public List<Pair<K2, List<V2>>> shuffle(final List<Pair<K2, V2>> mapOutputs) {
    // step 1 - use the key group comparator to organise map outputs
    final Comparator<K2> keyGroupComparator;
    if (this.keyGroupComparator == null) {
      keyGroupComparator = new JobConf(getConfiguration())
          .getOutputValueGroupingComparator();
    } else {
      keyGroupComparator = this.keyGroupComparator;
    }
    final TreeMap<K2, List<Pair<K2, V2>>> groupedByKey = new TreeMap<K2, List<Pair<K2, V2>>>(
        keyGroupComparator);

    List<Pair<K2, V2>> groupedKeyList;
    for (final Pair<K2, V2> mapOutput : mapOutputs) {
      groupedKeyList = groupedByKey.get(mapOutput.getFirst());

      if (groupedKeyList == null) {
        groupedKeyList = new ArrayList<Pair<K2, V2>>();
        groupedByKey.put(mapOutput.getFirst(), groupedKeyList);
      }

      groupedKeyList.add(mapOutput);
    }

    // step 2 - sort each key group using the key order comparator (if set)
    final Comparator<Pair<K2, V2>> pairKeyComparator = new Comparator<Pair<K2, V2>>() {
      @Override
      public int compare(final Pair<K2, V2> o1, final Pair<K2, V2> o2) {
        return keyValueOrderComparator.compare(o1.getFirst(), o2.getFirst());
      }
    };

    // create shuffle stage output list
    final List<Pair<K2, List<V2>>> outputKeyValuesList = new ArrayList<Pair<K2, List<V2>>>();

    // populate output list
    for (final Entry<K2, List<Pair<K2, V2>>> groupedByKeyEntry : groupedByKey
        .entrySet()) {
      if (keyValueOrderComparator != null) {
        // sort the key/value pairs using the key order comparator (if set)
        Collections.sort(groupedByKeyEntry.getValue(), pairKeyComparator);
      }

      // create list to hold values for the grouped key
      final List<V2> valuesList = new ArrayList<V2>();
      for (final Pair<K2, V2> pair : groupedByKeyEntry.getValue()) {
        valuesList.add(pair.getSecond());
      }

      // add key and values to output list
      outputKeyValuesList.add(new Pair<K2, List<V2>>(
          groupedByKeyEntry.getKey(), valuesList));
    }

    // return output list
    return outputKeyValuesList;
  }

  /**
   * Set the key grouping comparator, similar to calling the following API calls
   * but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API: {@link JobConf#setOutputValueGroupingComparator(Class)}
   * <LI>0.20.1+ API: {@link Job#setGroupingComparatorClass(Class)}
   * </UL>
   * 
   * @param groupingComparator
   */
  public void setKeyGroupingComparator(
      final RawComparator<K2> groupingComparator) {
    keyGroupComparator = ReflectionUtils.newInstance(
        groupingComparator.getClass(), getConfiguration());
  }

  /**
   * Set the key value order comparator, similar to calling the following API
   * calls but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API: {@link JobConf#setOutputKeyComparatorClass(Class)}
   * <LI>0.20.1+ API: {@link Job#setSortComparatorClass(Class)}
   * </UL>
   * 
   * @param orderComparator
   */
  public void setKeyOrderComparator(final RawComparator<K2> orderComparator) {
    keyValueOrderComparator = ReflectionUtils.newInstance(
        orderComparator.getClass(), getConfiguration());
  }
}
