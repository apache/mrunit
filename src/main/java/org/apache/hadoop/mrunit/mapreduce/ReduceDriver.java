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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.ReduceDriverBase;
import org.apache.hadoop.mrunit.counters.CounterWrapper;
import org.apache.hadoop.mrunit.mapreduce.mock.MockReduceContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Reducer instance. You provide a key and a
 * set of intermediate values for that key that represent inputs that should be
 * sent to the Reducer (as if they came from a Mapper), and outputs you expect
 * to be sent by the Reducer to the collector. By calling runTest(), the harness
 * will deliver the input to the Reducer and will check its outputs against the
 * expected results. This is designed to handle a single (k, v*) -> (k, v)* case
 * from the Reducer, representing a single unit test. Multiple input (k, v*)
 * sets should go in separate unit tests.
 */
public class ReduceDriver<K1, V1, K2, V2> extends
    ReduceDriverBase<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(ReduceDriver.class);

  private Reducer<K1, V1, K2, V2> myReducer;
  private Counters counters;

  public ReduceDriver(final Reducer<K1, V1, K2, V2> r) {
    myReducer = r;
    setCounters(new Counters());
  }

  public ReduceDriver() {
    setCounters(new Counters());
  }

  /**
   * Sets the reducer object to use for this test
   * 
   * @param r
   *          The reducer object to use
   */
  public void setReducer(final Reducer<K1, V1, K2, V2> r) {
    myReducer = r;
  }

  /**
   * Identical to setReducer(), but with fluent programming style
   * 
   * @param r
   *          The Reducer to use
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withReducer(
      final Reducer<K1, V1, K2, V2> r) {
    setReducer(r);
    return this;
  }

  public Reducer<K1, V1, K2, V2> getReducer() {
    return myReducer;
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
    counterWrapper = new CounterWrapper(ctrs);
  }

  /** Sets the counters to use and returns self for fluent style */
  public ReduceDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Identical to setInputKey() but with fluent programming style
   * 
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withInputKey(final K1 key) {
    setInputKey(key);
    return this;
  }

  /**
   * Identical to addInputValue() but with fluent programming style
   * 
   * @param val
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withInputValue(final V1 val) {
    addInputValue(val);
    return this;
  }

  /**
   * Identical to addInputValues() but with fluent programming style
   * 
   * @param values
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withInputValues(final List<V1> values) {
    addInputValues(values);
    return this;
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   * 
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withInput(final K1 key,
      final List<V1> values) {
    setInput(key, values);
    return this;
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withOutput(final Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param key
   *          The key part of a (k, v) pair to add
   * @param val
   *          The val part of a (k, v) pair to add
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withOutput(final K2 key, final V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Identical to setInput, but with a fluent programming style
   * 
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public ReduceDriver<K1, V1, K2, V2> withInputFromString(final String input) {
    setInputFromString(input);
    return this;
  }

  /**
   * Identical to addOutput, but with a fluent programming style
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public ReduceDriver<K1, V1, K2, V2> withOutputFromString(final String output) {
    addOutputFromString(output);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    if (inputKey == null || getInputValues().isEmpty()) {
      throw new IllegalStateException("No input was provided");
    }
    if (myReducer == null) {
      throw new IllegalStateException("No Reducer class was provided");
    }

    final List<Pair<K1, List<V1>>> inputs = new ArrayList<Pair<K1, List<V1>>>();
    inputs.add(new Pair<K1, List<V1>>(inputKey, getInputValues()));

    try {
      final MockReduceContextWrapper<K1, V1, K2, V2> wrapper = new MockReduceContextWrapper<K1, V1, K2, V2>(
          inputs, getCounters(), getConfiguration());
      final Reducer<K1, V1, K2, V2>.Context context = wrapper.getMockContext();
      myReducer.run(context);
      return wrapper.getOutputs();
    } catch (final InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public String toString() {
    return "ReduceDriver (0.20+) (" + myReducer + ")";
  }

  /**
   * @param configuration
   *          The configuration object that will given to the reducer associated
   *          with the driver
   * @return this object for fluent coding
   */
  public ReduceDriver<K1, V1, K2, V2> withConfiguration(
      final Configuration configuration) {
    setConfiguration(configuration);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public ReduceDriver<K1, V1, K2, V2> withCounter(Enum e, long expectedValue) {
    super.withCounter(e, expectedValue);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public ReduceDriver<K1, V1, K2, V2> withCounter(String g, String n, long e) {
    super.withCounter(g, n, e);
    return this;
  }

  /**
   * Returns a new ReduceDriver without having to specify the generic types on
   * the right hand side of the object create statement.
   * 
   * @return new ReduceDriver
   */
  public static <K1, V1, K2, V2> ReduceDriver<K1, V1, K2, V2> newReduceDriver() {
    return new ReduceDriver<K1, V1, K2, V2>();
  }

  /**
   * Returns a new ReduceDriver without having to specify the generic types on
   * the right hand side of the object create statement.
   * 
   * 
   * @param reducer
   *          passed to ReduceDriver constructor
   * @return new ReduceDriver
   */
  public static <K1, V1, K2, V2> ReduceDriver<K1, V1, K2, V2> newReduceDriver(
      final Reducer<K1, V1, K2, V2> reducer) {
    return new ReduceDriver<K1, V1, K2, V2>(reducer);
  }
}
