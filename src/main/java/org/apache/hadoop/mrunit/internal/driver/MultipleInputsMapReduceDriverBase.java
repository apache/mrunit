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

package org.apache.hadoop.mrunit.internal.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mrunit.TestDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.*;

/**
 * Harness that allows you to test multiple Mappers and a Reducer instance
 * together You provide the input keys and values that should be sent to each
 * Mapper, and outputs you expect to be sent by the Reducer to the collector for
 * those inputs. By calling runTest(), the harness will deliver the inputs to
 * the respective Mappers, feed the intermediate results to the Reducer (without
 * checking them), and will check the Reducer's outputs against the expected
 * results.
 * 
 * @param <M>
 *          The type of the Mapper (to support mapred and mapreduce API)
 * @param <K1>
 *          The common map output key type
 * @param <V1>
 *          The common map output value type
 * @param <K2>
 *          The reduce output key type
 * @param <V2>
 *          The reduce output value type
 * @param <T>
 *          The type of the MultipleInputMapReduceDriver implementation
 */
public abstract class MultipleInputsMapReduceDriverBase<M, K1, V1, K2, V2, T extends MultipleInputsMapReduceDriverBase<M, K1, V1, K2, V2, T>>
    extends TestDriver<K2, V2, T> {
  public static final Log LOG = LogFactory
      .getLog(MultipleInputsMapReduceDriverBase.class);

  protected Map<M, Path> mapInputPaths = new HashMap<M, Path>();

  /**
   * The path passed to the specifed mapper InputSplit
   * 
   * @param mapper
   *          The mapper to get the input path for
   * @return The path
   */
  public Path getMapInputPath(final M mapper) {
    return mapInputPaths.get(mapper);
  }

  /**
   * Path that is passed to the mapper InputSplit
   * 
   * @param mapper
   *          The mapper to set the input path for
   * @param mapInputPath
   *          The path
   */
  public void setMapInputPath(final M mapper, Path mapInputPath) {
    mapInputPaths.put(mapper, mapInputPath);
  }

  /**
   * Identical to setMapInputPath but supports a fluent programming style
   * 
   * @param mapper
   *          The mapper to set the input path for
   * @param mapInputPath
   *          The path
   * @return this
   */
  public final T withMapInputPath(final M mapper, Path mapInputPath) {
    this.setMapInputPath(mapper, mapInputPath);
    return thisAsMapReduceDriver();
  }

  /**
   * Key group comparator
   */
  protected Comparator<K1> keyGroupComparator;

  /**
   * Set the key grouping comparator, similar to calling the following API calls
   * but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API:
   * {@link org.apache.hadoop.mapred.JobConf#setOutputValueGroupingComparator(Class)}
   * <LI>0.20.1+ API:
   * {@link org.apache.hadoop.mapreduce.Job#setGroupingComparatorClass(Class)}
   * </UL>
   * 
   * @param groupingComparator
   */
  @SuppressWarnings("unchecked")
  public void setKeyGroupingComparator(
      final RawComparator<K2> groupingComparator) {
    keyGroupComparator = ReflectionUtils.newInstance(
        groupingComparator.getClass(), getConfiguration());
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
   * Key value order comparator
   */
  protected Comparator<K1> keyValueOrderComparator;

  /**
   * Set the key value order comparator, similar to calling the following API
   * calls but passing a real instance rather than just the class:
   * <UL>
   * <LI>pre 0.20.1 API:
   * {@link org.apache.hadoop.mapred.JobConf#setOutputKeyComparatorClass(Class)}
   * <LI>0.20.1+ API:
   * {@link org.apache.hadoop.mapreduce.Job#setSortComparatorClass(Class)}
   * </UL>
   * 
   * @param orderComparator
   */
  @SuppressWarnings("unchecked")
  public void setKeyOrderComparator(final RawComparator<K2> orderComparator) {
    keyValueOrderComparator = ReflectionUtils.newInstance(
        orderComparator.getClass(), getConfiguration());
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

  @SuppressWarnings("rawtypes")
  protected Map<M, List<Pair>> inputs = new HashMap<M, List<Pair>>();

  /**
   * Add an input to send to the specified mapper
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param key
   *          The key to add
   * @param val
   *          The value to add
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   */
  protected <K, V> void addInput(final M mapper, final K key, final V val) {
    if (!inputs.containsKey(mapper)) {
      inputs.put(mapper, new ArrayList<Pair>());
    }
    inputs.get(mapper).add(copyPair(key, val));
  }

  /**
   * Add an input to the specified mappper
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param input
   *          The (k, v) pair
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   */
  protected <K, V> void addInput(final M mapper, final Pair<K, V> input) {
    addInput(mapper, input.getFirst(), input.getSecond());
  }

  /**
   * Add inputs to the specified mapper
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param inputs
   *          The (k, v) pairs
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   */
  protected <K, V> void addAll(final M mapper, final List<Pair<K, V>> inputs) {
    for (Pair<K, V> input : inputs) {
      addInput(mapper, input);
    }
  }

  /**
   * Identical to addInput but supports a fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param key
   *          The key to add
   * @param val
   *          The value to add
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   * @return this
   */
  protected <K, V> T withInput(final M mapper, final K key, final V val) {
    addInput(mapper, key, val);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addInput but supports a fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param input
   *          The (k, v) pair to add
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   * @return this
   */
  protected <K, V> T withInput(final M mapper, final Pair<K, V> input) {
    addInput(mapper, input);
    return thisAsMapReduceDriver();
  }

  /**
   * Identical to addAll but supports a fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input to
   * @param inputs
   *          The (k, v) pairs to add
   * @param <K>
   *          The key type
   * @param <V>
   *          The value type
   * @return this
   */
  protected <K, V> T withAll(final M mapper, final List<Pair<K, V>> inputs) {
    addAll(mapper, inputs);
    return thisAsMapReduceDriver();
  }

  protected void preRunChecks(Set<M> mappers, Object reducer) {
    for (M mapper : mappers) {
      if (inputs.get(mapper) == null || inputs.get(mapper).isEmpty()) {
        throw new IllegalStateException(String.format(
            "No input was provided for mapper %s", mapper));
      }
    }

    if (reducer == null) {
      throw new IllegalStateException("No reducer class was provided");
    }
    if (driverReused()) {
      throw new IllegalStateException("Driver reuse not allowed");
    } else {
      setUsedOnceStatus();
    }
  }

  @SuppressWarnings("unchecked")
  private T thisAsMapReduceDriver() {
    return (T) this;
  }

}
