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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public abstract class MapReduceDriverBase<K1, V1, K2, V2, K3, V3, T extends MapReduceDriverBase<K1, V1, K2, V2, K3, V3, T>>
    extends TestDriver<K1, V1, K3, V3, T> {

  public static final Log LOG = LogFactory.getLog(MapReduceDriverBase.class);

  protected List<Pair<K1, V1>> inputList = new ArrayList<Pair<K1, V1>>();
  
  protected Path mapInputPath = new Path("somefile");

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
   * Adds input to send to the mapper
   * 
   * @param inputs
   *          List of (k, v) pairs to add to the input list
   */
  public void addAll(final List<Pair<K1, V1>> inputs) {
    for (Pair<K1, V1> input : inputs) {
      addInput(input);
    }
  }
  
  /**
   * Adds output (k, v)* pairs we expect from the Reducer
   * 
   * @param outputRecords
   *          List of (k, v) pairs to add
   */
  public void addAllOutput(final List<Pair<K3, V3>> outputRecords) {
    for (Pair<K3, V3> output : outputRecords) {
      addOutput(output);
    }
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
  
  @SuppressWarnings("unchecked")
  private T thisAsMapReduceDriver() {
    return (T) this;
  }
  
  /**
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param key
   * @param val
   * @return this
   */
  public T withInput(final K1 key,
      final V1 val) {
    addInput(key, val);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param input
   *          The (k, v) pair to add
   * @return this
   */
  public T withInput(
      final Pair<K1, V1> input) {
    addInput(input);
    return thisAsMapReduceDriver();
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public T withOutput(
      final Pair<K3, V3> outputRecord) {
    addOutput(outputRecord);
    return thisAsMapReduceDriver();
  }

  /**
   * Functions like addOutput() but returns self for fluent programming style
   * 
   * @param key
   * @param val
   * @return this
   */
  public T withOutput(final K3 key,
      final V3 val) {
    addOutput(key, val);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addInputFromString, but with a fluent programming style
   * 
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public T withInputFromString(
      final String input) {
    addInputFromString(input);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addOutputFromString, but with a fluent programming style
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public T withOutputFromString(
      final String output) {
    addOutputFromString(output);
    return thisAsMapReduceDriver();
  }

  public T withOutputCopyingOrInputFormatConfiguration(
      Configuration configuration) {
    setOutputCopyingOrInputFormatConfiguration(configuration);
    return thisAsMapReduceDriver();
  }
  
  /**
   * Identical to addAll() but returns self for fluent programming style
   * 
   * @param inputs
   *          List of (k, v) pairs to add
   * @return this
   */
  public T withAll(
      final List<Pair<K1, V1>> inputs) {
    addAll(inputs);
    return thisAsMapReduceDriver();
  }
  
  /**
   * Works like addAllOutput(), but returns self for fluent style
   * 
   * @param outputRecords
   * @return this
   */
  public T withAllOutput(
      final List<Pair<K3, V3>> outputRecords) {
    addAllOutput(outputRecords);
    return thisAsMapReduceDriver();
  }
  
  /**
   * @return the path passed to the mapper InputSplit
   */
  public Path getMapInputPath() {
    return mapInputPath;
  }

  /**
   * @param mapInputPath Path which is to be passed to the mappers InputSplit
   */
  public void setMapInputPath(Path mapInputPath) {
    this.mapInputPath = mapInputPath;
  }
  
  /**
   * @param mapInputPath
   *       The Path object which will be given to the mapper
   * @return
   */
  public final T withMapInputPath(Path mapInputPath) {
    setMapInputPath(mapInputPath);
    return thisAsTestDriver();
  }

  protected void preRunChecks(Object mapper, Object reducer) {
    if (inputList.isEmpty()) {
      throw new IllegalStateException("No input was provided");
    }
    if (mapper == null) {
      throw new IllegalStateException("No Mapper class was provided");
    }
    if (reducer == null) {
      throw new IllegalStateException("No Reducer class was provided");
    }
  }
  
  @Override
  public abstract List<Pair<K3, V3>> run() throws IOException;

  /**
   * Take the outputs from the Mapper, combine all values for the same key, and
   * sort them by key.
   * 
   * @param mapOutputs
   *          An unordered list of (key, val) pairs from the mapper
   * @return the sorted list of (key, list(val))'s to present to the reducer
   */
  public List<Pair<K2, List<V2>>> shuffle(final List<Pair<K2, V2>> mapOutputs) {
    
    final Comparator<K2> keyOrderComparator;
    final Comparator<K2> keyGroupComparator;
    
    if (mapOutputs.isEmpty()) {
      return Collections.emptyList();
    }

    // JobConf needs the map output key class to work out the
    // comparator to use
    JobConf conf = new JobConf(getConfiguration());
    K2 firstKey = mapOutputs.get(0).getFirst();
    conf.setMapOutputKeyClass(firstKey.getClass());

    // get the ordering comparator or work out from conf
    if (keyValueOrderComparator == null) {
      keyOrderComparator = conf.getOutputKeyComparator();
    } else {
      keyOrderComparator = this.keyValueOrderComparator;
    }
    
    // get the grouping comparator or work out from conf
    if (this.keyGroupComparator == null) {
      keyGroupComparator = conf.getOutputValueGroupingComparator();
    } else {
      keyGroupComparator = this.keyGroupComparator;
    }

    // sort the map outputs according to their keys
    Collections.sort(mapOutputs, new Comparator<Pair<K2, V2>>() {
      public int compare(final Pair<K2, V2> o1, final Pair<K2, V2> o2) {
        return keyOrderComparator.compare(o1.getFirst(), o2.getFirst());
      }
    });

    // apply grouping comparator to create groups
    final Map<K2, List<Pair<K2, V2>>> groupedByKey = 
        new LinkedHashMap<K2, List<Pair<K2, V2>>>();
    
    List<Pair<K2, V2>> groupedKeyList = null;
    Pair<K2,V2> previous = null;
    
    for (final Pair<K2, V2> mapOutput : mapOutputs) {
      if (previous == null || keyGroupComparator
          .compare(previous.getFirst(), mapOutput.getFirst()) != 0) {
        groupedKeyList = new ArrayList<Pair<K2, V2>>();
        groupedByKey.put(mapOutput.getFirst(), groupedKeyList);
      }
      groupedKeyList.add(mapOutput);
      previous = mapOutput;
    }

    // populate output list
    final List<Pair<K2, List<V2>>> outputKeyValuesList = new ArrayList<Pair<K2, List<V2>>>();
    for (final Entry<K2, List<Pair<K2, V2>>> groupedByKeyEntry : 
            groupedByKey.entrySet()) {

      // create list to hold values for the grouped key
      final List<V2> valuesList = new ArrayList<V2>();
      for (final Pair<K2, V2> pair : groupedByKeyEntry.getValue()) {
        valuesList.add(pair.getSecond());
      }

      // add key and values to output list
      outputKeyValuesList.add(new Pair<K2, List<V2>>(
          groupedByKeyEntry.getKey(), valuesList));
    }

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
