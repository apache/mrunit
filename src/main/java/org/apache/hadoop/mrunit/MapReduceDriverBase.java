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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
    extends TestDriver<K3, V3, T> {

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
  public T withInput(final K1 key, final V1 val) {
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
  public T withInput(final Pair<K1, V1> input) {
    addInput(input);
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
  public T withInputFromString(final String input) {
    addInputFromString(input);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addAll() but returns self for fluent programming style
   * 
   * @param inputs
   *          List of (k, v) pairs to add
   * @return this
   */
  public T withAll(final List<Pair<K1, V1>> inputs) {
    addAll(inputs);
    return thisAsMapReduceDriver();
  }

  /**
   * @return the path passed to the mapper InputSplit
   */
  public Path getMapInputPath() {
    return mapInputPath;
  }

  /**
   * @param mapInputPath
   *          Path which is to be passed to the mappers InputSplit
   */
  public void setMapInputPath(Path mapInputPath) {
    this.mapInputPath = mapInputPath;
  }

  /**
   * @param mapInputPath
   *          The Path object which will be given to the mapper
   * @return this
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
    if (driverReused()) {
      throw new IllegalStateException("Driver reuse not allowed");
    } else {
      setUsedOnceStatus();
    }
  }

  @Override
  public abstract List<Pair<K3, V3>> run() throws IOException;

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

  /**
   * Identical to {@link #setKeyGroupingComparator(RawComparator)}, but with a
   * fluent programming style
   * 
   * @param groupingComparator
   *          Comparator to use in the shuffle stage for key grouping
   * @return this
   */
  public T withKeyGroupingComparator(final RawComparator<K2> groupingComparator) {
    setKeyGroupingComparator(groupingComparator);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to {@link #setKeyOrderComparator(RawComparator)}, but with a
   * fluent programming style
   * 
   * @param orderComparator
   *          Comparator to use in the shuffle stage for key value ordering
   * @return this
   */
  public T withKeyOrderComparator(final RawComparator<K2> orderComparator) {
    setKeyOrderComparator(orderComparator);
    return thisAsMapReduceDriver();
  }

}
