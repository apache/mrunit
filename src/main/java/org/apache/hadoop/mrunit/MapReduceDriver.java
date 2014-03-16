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

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
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
public class MapReduceDriver<K1, V1, K2, V2, K3, V3>
    extends
    MapReduceDriverBase<K1, V1, K2, V2, K3, V3, MapReduceDriver<K1, V1, K2, V2, K3, V3>> {

  public static final Log LOG = LogFactory.getLog(MapReduceDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;
  private Reducer<K2, V2, K3, V3> myReducer;
  private Reducer<K2, V2, K2, V2> myCombiner;
  private Counters counters;

  @SuppressWarnings("rawtypes")
  private Class<? extends OutputFormat> outputFormatClass;
  @SuppressWarnings("rawtypes")
  private Class<? extends InputFormat> inputFormatClass;

  public MapReduceDriver(final Mapper<K1, V1, K2, V2> m,
      final Reducer<K2, V2, K3, V3> r) {
    this();
    setMapper(m);
    setReducer(r);
  }

  public MapReduceDriver(final Mapper<K1, V1, K2, V2> m,
      final Reducer<K2, V2, K3, V3> r, final Reducer<K2, V2, K2, V2> c) {
    this(m, r);
    setCombiner(c);
  }

  public MapReduceDriver() {
    setCounters(new Counters());
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
    counters = ctrs;
    counterWrapper = new CounterWrapper(counters);
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
    myMapper = returnNonNull(m);
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
    myReducer = returnNonNull(r);
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
    myCombiner = returnNonNull(c);
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
   * Configure {@link Reducer} to output with a real {@link OutputFormat}. Set
   * {@link InputFormat} to read output back in for use with run* methods
   *
   * @param outputFormatClass
   * @param inputFormatClass
   * @return this for fluent style
   */
  @SuppressWarnings("rawtypes")
  public MapReduceDriver<K1, V1, K2, V2, K3, V3> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass,
      final Class<? extends InputFormat> inputFormatClass) {
    this.outputFormatClass = returnNonNull(outputFormatClass);
    this.inputFormatClass = returnNonNull(inputFormatClass);
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

      if (!inputs.isEmpty()) {
        if (LOG.isDebugEnabled()) {
          final StringBuilder sb = new StringBuilder();
          for (Pair<K2, List<V2>> input : inputs) {
            formatValueList(input.getSecond(), sb);
            LOG.debug("Reducing input (" + input.getFirst() + ", " + sb + ")");
            sb.delete(0, sb.length());
          }
        }

        final ReduceDriver<K2, V2, OUTKEY, OUTVAL> reduceDriver = ReduceDriver
            .newReduceDriver(reducer).withCounters(getCounters())
            .withConfiguration(getConfiguration()).withAll(inputs);

        if (getOutputSerializationConfiguration() != null) {
          reduceDriver
              .withOutputSerializationConfiguration(getOutputSerializationConfiguration());
        }

        if (outputFormatClass != null) {
          reduceDriver.withOutputFormat(outputFormatClass, inputFormatClass);
        }

        reduceOutputs.addAll(reduceDriver.run());
      }

      return reduceOutputs;
    }
  }

  @Override
  public List<Pair<K3, V3>> run() throws IOException {
    try {
      preRunChecks(myMapper, myReducer);
      initDistributedCache();
      List<Pair<K2, V2>> mapOutputs = new ArrayList<Pair<K2, V2>>();

      // run map component
      LOG.debug("Starting map phase with mapper: " + myMapper);
      mapOutputs.addAll(MapDriver.newMapDriver(myMapper)
          .withCounters(getCounters()).withConfiguration(getConfiguration())
          .withAll(inputList).withMapInputPath(getMapInputPath()).run());

      if (myCombiner != null) {
        // User has specified a combiner. Run this and replace the mapper outputs
        // with the result of the combiner.
        LOG.debug("Starting combine phase with combiner: " + myCombiner);
        mapOutputs = new ReducePhaseRunner<K2, V2>().runReduce(
            shuffle(mapOutputs), myCombiner);
      }

      // Run the reduce phase.
      LOG.debug("Starting reduce phase with reducer: " + myReducer);

      return new ReducePhaseRunner<K3, V3>()
          .runReduce(shuffle(mapOutputs),myReducer);
    } finally {
      cleanupDistributedCache();
    }
  }

  @Override
  public String toString() {
    return "MapReduceDriver (" + myMapper + ", " + myReducer + ")";
  }

  /**
   * Returns a new MapReduceDriver without having to specify the generic types
   * on the right hand side of the object create statement.
   *
   * @return new MapReduceDriver
   */
  public static <K1, V1, K2, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver() {
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
  public static <K1, V1, K2, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
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
  public static <K1, V1, K2, V2, K3, V3> MapReduceDriver<K1, V1, K2, V2, K3, V3> newMapReduceDriver(
      final Mapper<K1, V1, K2, V2> mapper,
      final Reducer<K2, V2, K3, V3> reducer,
      final Reducer<K2, V2, K2, V2> combiner) {
    return new MapReduceDriver<K1, V1, K2, V2, K3, V3>(mapper, reducer,
        combiner);
  }
}
