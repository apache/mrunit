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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper and a Reducer instance together
 * (along with an optional combiner). You provide the input key and value that
 * should be sent to the Mapper, and outputs you expect to be sent by the
 * Reducer to the collector for those inputs. By calling runTest(), the harness
 * will deliver the input to the Mapper, feed the intermediate results to the
 * Reducer (without checking them), and will check the Reducer's outputs against
 * the expected results. This is designed to handle the (k, v)* -> (k, v)* case
 * from the Mapper/Reducer pair, representing a single unit test.
 * 
 * If a combiner is specified, then it will be run exactly once after the Mapper
 * and before the Reducer.
 */
@SuppressWarnings({ "deprecation", "rawtypes" })
public class MapReduceDriver<K1, V1, K2 extends Comparable, V2, K3, V3> extends
    MapReduceDriverBase<K1, V1, K2, V2, K3, V3> {

  public static final Log LOG = LogFactory.getLog(MapReduceDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;
  private Reducer<K2, V2, K3, V3> myReducer;
  private Reducer<K2, V2, K2, V2> myCombiner;
  private Counters counters;

  public MapReduceDriver(final Mapper<K1, V1, K2, V2> m,
      final Reducer<K2, V2, K3, V3> r) {
    myMapper = m;
    myReducer = r;
    counters = new Counters();
  }

  public MapReduceDriver(final Mapper<K1, V1, K2, V2> m,
      final Reducer<K2, V2, K3, V3> r, final Reducer<K2, V2, K2, V2> c) {
    myMapper = m;
    myReducer = r;
    myCombiner = c;
    counters = new Counters();
  }

  public MapReduceDriver() {
    counters = new Counters();
  }

  /** @return the counters used in this test */
  public Counters getCounters() {
    return counters;
  }

  /**
   * Sets the counters object to use for this test.
   * 
   * @param ctrs
   *          The counters object to use.
   */
  public void setCounters(final Counters ctrs) {
    this.counters = ctrs;
  }

  /** Sets the counters to use and returns self for fluent style */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withCounters(
      final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Set the Mapper instance to use with this test driver
   * 
   * @param m
   *          the Mapper instance to use
   */
  public void setMapper(final Mapper<K1, V1, K2, V2> m) {
    myMapper = m;
  }

  /** Sets the Mapper instance to use and returns self for fluent style */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withMapper(
      final Mapper<K1, V1, K2, V2> m) {
    setMapper(m);
    return this;
  }

  /**
   * @return the Mapper object being used by this test
   */
  public Mapper<K1, V1, K2, V2> getMapper() {
    return myMapper;
  }

  /**
   * Sets the reducer object to use for this test
   * 
   * @param r
   *          The reducer object to use
   */
  public void setReducer(final Reducer<K2, V2, K3, V3> r) {
    myReducer = r;
  }

  /**
   * Identical to setReducer(), but with fluent programming style
   * 
   * @param r
   *          The Reducer to use
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withReducer(
      final Reducer<K2, V2, K3, V3> r) {
    setReducer(r);
    return this;
  }

  /**
   * @return the Reducer object being used for this test
   */
  public Reducer<K2, V2, K3, V3> getReducer() {
    return myReducer;
  }

  /**
   * Sets the reducer object to use as a combiner for this test
   * 
   * @param c
   *          The combiner object to use
   */
  public void setCombiner(final Reducer<K2, V2, K2, V2> c) {
    myCombiner = c;
  }

  /**
   * Identical to setCombiner(), but with fluent programming style
   * 
   * @param c
   *          The Combiner to use
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withCombiner(
      final Reducer<K2, V2, K2, V2> c) {
    setCombiner(c);
    return this;
  }

  /**
   * @return the Combiner object being used for this test
   */
  public Reducer<K2, V2, K2, V2> getCombiner() {
    return myCombiner;
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param key
   * @param val
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withInput(final K1 key,
      final V1 val) {
    addInput(key, val);
    return this;
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param input
   *          The (k, v) pair to add
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withInput(
      final Pair<K1, V1> input) {
    addInput(input);
    return this;
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withOutput(
      final Pair<K3, V3> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Functions like addOutput() but returns self for fluent programming style
   * 
   * @param key
   * @param val
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withOutput(final K3 key,
      final V3 val) {
    addOutput(key, val);
    return this;
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
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withInputFromString(
      final String input) {
    addInputFromString(input);
    return this;
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
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withOutputFromString(
      final String output) {
    addOutputFromString(output);
    return this;
  }

  /**
   * The private class to manage starting the reduce phase is used for type
   * genericity reasons. This class is used in the run() method.
   */
  private class ReducePhaseRunner<OUTKEY, OUTVAL> {
    private List<Pair<OUTKEY, OUTVAL>> runReduce(
        final List<Pair<K2, List<V2>>> inputs,
        final Reducer<K2, V2, OUTKEY, OUTVAL> reducer) throws IOException {

      final List<Pair<OUTKEY, OUTVAL>> reduceOutputs = new ArrayList<Pair<OUTKEY, OUTVAL>>();

      for (final Pair<K2, List<V2>> input : inputs) {
        final K2 inputKey = input.getFirst();
        final List<V2> inputValues = input.getSecond();
        final StringBuilder sb = new StringBuilder();
        formatValueList(inputValues, sb);
        LOG.debug("Reducing input (" + inputKey.toString() + ", "
            + sb.toString() + ")");

        reduceOutputs.addAll(ReduceDriver.newReduceDriver(reducer)
            .withCounters(getCounters()).withConfiguration(configuration)
            .withInputKey(inputKey).withInputValues(inputValues).run());
      }

      return reduceOutputs;
    }
  }

  @Override
  public List<Pair<K3, V3>> run() throws IOException {
    if (inputList.size() == 0) {
      LOG.warn("No inputs configured to send to Mapper and Reducer; this is a trivial test.");
    }

    List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();

    // run map component
    for (final Pair<K1, V1> input : inputList) {
      LOG.debug("Mapping input " + input.toString() + ")");

      mapOutputs.addAll(MapDriver.newMapDriver(myMapper).withInput(input)
          .withCounters(getCounters()).withConfiguration(configuration).run());
    }

    if (myCombiner != null) {
      // User has specified a combiner. Run this and replace the mapper outputs
      // with the result of the combiner.
      LOG.debug("Starting combine phase with combiner: " + myCombiner);
      mapOutputs = new ReducePhaseRunner<K2, V2>().runReduce(
          shuffle(mapOutputs), myCombiner);
    }

    // Run the reduce phase.
    LOG.debug("Starting reduce phase with reducer: " + myReducer);
    return new ReducePhaseRunner<K3, V3>().runReduce(shuffle(mapOutputs),
        myReducer);
  }

  @Override
  public String toString() {
    return "MapReduceDriver (" + myMapper + ", " + myReducer + ")";
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper and reducer
   *          associated with the driver
   * @return this driver object for fluent coding
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withConfiguration(
      final Configuration configuration) {
    setConfiguration(configuration);
    return this;
  }

  /**
   * Identical to {@link #setKeyGroupingComparator(RawComparator)}, but with a
   * fluent programming style
   * 
   * @param groupingComparator
   *          Comparator to use in the shuffle stage for key grouping
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withKeyGroupingComparator(
      final RawComparator<K2> groupingComparator) {
    setKeyGroupingComparator(groupingComparator);
    return this;
  }

  /**
   * Identical to {@link #setKeyOrderComparator(RawComparator)}, but with a
   * fluent programming style
   * 
   * @param orderComparator
   *          Comparator to use in the shuffle stage for key value ordering
   * @return this
   */
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withKeyOrderComparator(
      final RawComparator<K2> orderComparator) {
    setKeyOrderComparator(orderComparator);
    return this;
  }

  /**
   * Returns a new MapReduceDriver without having to specify the generic types
   * on the right hand side of the object create statement.
   * 
   * @param mapper
   *          passed to MapReduceDriver constructor
   * @param reducer
   *          passed to MapReduceDriver constructor
   * @return new MapReduceDriver
   */
  public static <K1, V1, K2 extends Comparable, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver() {
    return new MapReduceDriver<K1, V1, K2, V2, K3, V3>();
  }

  /**
   * Returns a new MapReduceDriver without having to specify the generic types
   * on the right hand side of the object create statement.
   * 
   * @param mapper
   *          passed to MapReduceDriver constructor
   * @param reducer
   *          passed to MapReduceDriver constructor
   * @return new MapReduceDriver
   */
  public static <K1, V1, K2 extends Comparable, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
      final Mapper<K1, V1, K2, V2> mapper, final Reducer<K2, V2, K3, V3> reducer) {
    return new MapReduceDriver<K1, V1, K2, V2, K3, V3>(mapper, reducer);
  }

  /**
   * Returns a new MapReduceDriver without having to specify the generic types
   * on the right hand side of the object create statement.
   * 
   * @param mapper
   *          passed to MapReduceDriver constructor
   * @param reducer
   *          passed to MapReduceDriver constructor
   * @param combiner
   *          passed to MapReduceDriver constructor
   * @return new MapReduceDriver
   */
  public static <K1, V1, K2 extends Comparable, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
      final Mapper<K1, V1, K2, V2> mapper,
      final Reducer<K2, V2, K3, V3> reducer,
      final Reducer<K2, V2, K2, V2> combiner) {
    return new MapReduceDriver<K1, V1, K2, V2, K3, V3>(mapper, reducer,
        combiner);
  }
}
