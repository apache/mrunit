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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.driver.MultipleInputsMapReduceDriverBase;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

/**
 * Harness that allows you to test multiple Mappers and a Reducer instance
 * together (along with an optional combiner). You provide the input keys and
 * values that should be sent to each Mapper, and outputs you expect to be sent
 * by the Reducer to the collector for those inputs. By calling runTest(), the
 * harness will deliver the inputs to the respective Mappers, feed the
 * intermediate results to the Reducer (without checking them), and will check
 * the Reducer's outputs against the expected results.
 * 
 * If a combiner is specified, it will run exactly once after all the Mappers
 * and before the Reducer
 * 
 * @param <K1>
 *          The common map output key type
 * @param <V1>
 *          The common map output value type
 * @param <K2>
 *          The reduce output key type
 * @param <V2>
 *          The reduce output value type
 */
public class MultipleInputsMapReduceDriver<K1, V1, K2, V2>
    extends
        MultipleInputsMapReduceDriverBase<Mapper, K1, V1, K2, V2, MultipleInputsMapReduceDriver<K1, V1, K2, V2>> {
  public static final Log LOG = LogFactory
      .getLog(MultipleInputsMapReduceDriver.class);

  private Set<Mapper> mappers = new HashSet<Mapper>();

  /**
   * Add a mapper to use with this test driver
   * 
   * @param mapper
   *          The mapper instance to add
   * @param <K>
   *          The input key type to the mapper
   * @param <V>
   *          The input value type to the mapper
   */
  public <K, V> void addMapper(final Mapper<K, V, K1, V1> mapper) {
    this.mappers.add(returnNonNull(mapper));
  }

  /**
   * Identical to addMapper but supports a fluent programming style
   * 
   * @param mapper
   *          The mapper instance to add
   * @param <K>
   *          The input key type to the mapper
   * @param <V>
   *          The input value type to the mapper
   * @return this
   */
  public <K, V> MultipleInputsMapReduceDriver<K1, V1, K2, V2> withMapper(
      final Mapper<K, V, K1, V1> mapper) {
    addMapper(mapper);
    return this;
  }

  /**
   * @return The Mapper instances being used by this test
   */
  public Collection<Mapper> getMappers() {
    return mappers;
  }

  private Reducer<K1, V1, K1, V1> combiner;

  /**
   * Set the combiner to use with this test driver
   * 
   * @param combiner
   *          The combiner instance to use
   */
  public void setCombiner(final Reducer<K1, V1, K1, V1> combiner) {
    this.combiner = returnNonNull(combiner);
  }

  /**
   * Identical to setCombiner but supports a fluent programming style
   * 
   * @param combiner
   *          The combiner instance to use
   * @return this
   */
  public MultipleInputsMapReduceDriver<K1, V1, K2, V2> withCombiner(
      final Reducer<K1, V1, K1, V1> combiner) {
    setCombiner(combiner);
    return this;
  }

  /**
   * @return The combiner instance being used by this test
   */
  public Reducer<K1, V1, K1, V1> getCombiner() {
    return combiner;
  }

  private Reducer<K1, V1, K2, V2> reducer;

  /**
   * Set the reducer to use with this test driver
   * 
   * @param reducer
   *          The reducer instance to use
   */
  public void setReducer(final Reducer<K1, V1, K2, V2> reducer) {
    this.reducer = returnNonNull(reducer);
  }

  /**
   * Identical to setReducer but supports a fluent programming style
   * 
   * @param reducer
   *          The reducer instance to use
   * @return this
   */
  public MultipleInputsMapReduceDriver<K1, V1, K2, V2> withReducer(
      final Reducer<K1, V1, K2, V2> reducer) {
    setReducer(reducer);
    return this;
  }

  /**
   * @return Get the reducer instance being used by this test
   */
  public Reducer<K1, V1, K2, V2> getReducer() {
    return reducer;
  }

  private Counters counters;

  /**
   * @return The counters used in this test
   */
  public Counters getCounters() {
    return counters;
  }

  /**
   * Sets the counters object to use for this test
   * 
   * @param counters
   *          The counters object to use
   */
  public void setCounters(Counters counters) {
    this.counters = counters;
    counterWrapper = new CounterWrapper(counters);
  }

  /**
   * Identical to setCounters but supports a fluent programming style
   * 
   * @param counters
   *          The counters object to use
   * @return this
   */
  public MultipleInputsMapReduceDriver<K1, V1, K2, V2> withCounter(
      Counters counters) {
    setCounters(counters);
    return this;
  }

  private Class<? extends OutputFormat> outputFormatClass;

  /**
   * Configure {@link Reducer} to output with a real {@link OutputFormat}.
   * 
   * @param outputFormatClass
   *          The OutputFormat class
   * @return this
   */
  public MultipleInputsMapReduceDriver<K1, V1, K2, V2> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass) {
    this.outputFormatClass = returnNonNull(outputFormatClass);
    return this;
  }

  private Class<? extends InputFormat> inputFormatClass;

  /**
   * Set the InputFormat
   * 
   * @param inputFormatClass
   *          The InputFormat class
   * @return this
   */
  public MultipleInputsMapReduceDriver<K1, V1, K2, V2> withInputFormat(
      final Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = returnNonNull(inputFormatClass);
    return this;
  }

  /**
   * Construct a driver with the specified Reducer. Note that a Combiner can be
   * set separately.
   * 
   * @param reducer
   *          The reducer to use
   */
  public MultipleInputsMapReduceDriver(Reducer<K1, V1, K2, V2> reducer) {
    this();
    this.reducer = reducer;
  }

  /**
   * Construct a driver with the specified Combiner and Reducers
   * 
   * @param combiner
   *          The combiner to use
   * @param reducer
   *          The reducer to use
   */
  public MultipleInputsMapReduceDriver(Reducer<K1, V1, K1, V1> combiner,
                                       Reducer<K1, V1, K2, V2> reducer) {
    this(reducer);
    this.combiner = combiner;
  }

  /**
   * Construct a driver without specifying a Combiner nor a Reducer. Note that
   * these can be set with the appropriate set methods and that at least the
   * Reducer must be set.
   */
  public MultipleInputsMapReduceDriver() {
    setCounters(new Counters());
  }

  /**
   * Static factory-style method to construct a driver instance with the
   * specified Combiner and Reducer
   * 
   * @param combiner
   *          The combiner to use
   * @param reducer
   *          The reducer to use
   * @param <K1>
   *          The common output key type of the mappers
   * @param <V1>
   *          The common output value type of the mappers
   * @param <K2>
   *          The output key type of the reducer
   * @param <V2>
   *          The output value type of the reducer
   * @return this to support fluent programming style
   */
  public static <K1, V1, K2, V2> MultipleInputsMapReduceDriver<K1, V1, K2, V2> newMultipleInputMapReduceDriver(
      final Reducer<K1, V1, K1, V1> combiner,
      final Reducer<K1, V1, K2, V2> reducer) {
    return new MultipleInputsMapReduceDriver<K1, V1, K2, V2>(combiner, reducer);
  }

  /**
   * Static factory-style method to construct a driver instance with the
   * specified Reducer
   * 
   * @param reducer
   *          The reducer to use
   * @param <K1>
   *          The common output key type of the mappers
   * @param <V1>
   *          The common output value type of the mappers
   * @param <K2>
   *          The output key type of the reducer
   * @param <V2>
   *          The output value type of the reducer
   * @return this to support fluent programming style
   */
  public static <K1, V1, K2, V2> MultipleInputsMapReduceDriver<K1, V1, K2, V2> newMultipleInputMapReduceDriver(
      final Reducer<K1, V1, K2, V2> reducer) {
    return new MultipleInputsMapReduceDriver<K1, V1, K2, V2>(reducer);
  }

  /**
   * Static factory-style method to construct a driver instance without
   * specifying a Combiner nor a Reducer. Note that these can be set separately
   * by using the appropriate set (or with) methods and that at least a Reducer
   * must be set
   * 
   * @param <K1>
   *          The common output key type of the mappers
   * @param <V1>
   *          The common output value type of the mappers
   * @param <K2>
   *          The output key type of the reducer
   * @param <V2>
   *          The output value type of the reducer
   * @return this to support fluent programming style
   */
  public static <K1, V1, K2, V2> MultipleInputsMapReduceDriver<K1, V1, K2, V2> newMultipleInputMapReduceDriver() {
    return new MultipleInputsMapReduceDriver<K1, V1, K2, V2>();
  }

  /**
   * Add the specified (key, val) pair to the specified mapper
   * 
   * @param mapper
   *          The mapper to add the input pair to
   * @param key
   *          The key
   * @param val
   *          The value
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   */
  public <K, V> void addInput(final Mapper<K, V, K1, V1> mapper, final K key,
      final V val) {
    super.addInput(mapper, key, val);
  }

  /**
   * Add the specified input pair to the specified mapper
   * 
   * @param mapper
   *          The mapper to add the input pair to
   * @param input
   *          The (k,v) pair to add
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   */
  public <K, V> void addInput(final Mapper<K, V, K1, V1> mapper,
      final Pair<K, V> input) {
    super.addInput(mapper, input);
  }

  /**
   * Add the specified input pairs to the specified mapper
   * 
   * @param mapper
   *          The mapper to add the input pairs to
   * @param inputs
   *          The (k, v) pairs to add
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   */
  public <K, V> void addAll(final Mapper<K, V, K1, V1> mapper,
      final List<Pair<K, V>> inputs) {
    super.addAll(mapper, inputs);
  }

  /**
   * Identical to addInput but supports fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input pair to
   * @param key
   *          The key
   * @param val
   *          The value
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   * @return this
   */
  public <K, V> MultipleInputsMapReduceDriver<K1, V1, K2, V2> withInput(
      final Mapper<K, V, K1, V1> mapper, final K key, final V val) {
    return super.withInput(mapper, key, val);
  }

  /**
   * Identical to addInput but supports fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input pairs to
   * @param input
   *          The (k, v) pairs to add
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   * @return this
   */
  public <K, V> MultipleInputsMapReduceDriver<K1, V1, K2, V2> withInput(
      final Mapper<K, V, K1, V1> mapper, final Pair<K, V> input) {
    return super.withInput(mapper, input);
  }

  /**
   * Identical to addInput but supports fluent programming style
   * 
   * @param mapper
   *          The mapper to add the input pairs to
   * @param inputs
   *          The (k, v) pairs to add
   * @param <K>
   *          The type of the key
   * @param <V>
   *          The type of the value
   * @return this
   */
  public <K, V> MultipleInputsMapReduceDriver<K1, V1, K2, V2> withAll(
      final Mapper<K, V, K1, V1> mapper, final List<Pair<K, V>> inputs) {
    return super.withAll(mapper, inputs);
  }

  @Override
  protected void preRunChecks(Set<Mapper> mappers, Object reducer) {
    if (mappers.isEmpty()) {
      throw new IllegalStateException("No mappers were provided");
    }
    super.preRunChecks(mappers, reducer);
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    try {
      preRunChecks(mappers, reducer);
      initDistributedCache();

      List<Pair<K1, V1>> outputs = new ArrayList<Pair<K1, V1>>();

      MapOutputShuffler<K1, V1> shuffler = new MapOutputShuffler<K1, V1>(
          getConfiguration(), keyValueOrderComparator, keyGroupComparator);

      for (Mapper mapper : mappers) {
        MapDriver mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setCounters(counters);
        mapDriver.setConfiguration(getConfiguration());
        mapDriver.addAll(inputs.get(mapper));
        mapDriver.withMapInputPath(getMapInputPath(mapper));
        outputs.addAll(mapDriver.run());
      }

      if (combiner != null) {
        LOG.debug("Starting combine phase with combiner: " + combiner);
        outputs = new ReducePhaseRunner<K1, V1, K1, V1>(inputFormatClass,
            getConfiguration(), counters,
            getOutputSerializationConfiguration(), outputFormatClass)
            .runReduce(shuffler.shuffle(outputs), combiner);
      }

      LOG.debug("Starting reduce phase with reducer: " + reducer);

      return new ReducePhaseRunner<K1, V1, K2, V2>(inputFormatClass,
          getConfiguration(), counters, getOutputSerializationConfiguration(),
          outputFormatClass).runReduce(shuffler.shuffle(outputs), reducer);
    } finally {
      cleanupDistributedCache();
    }
  }
}
