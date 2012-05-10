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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.mapred.MockReporter;
import org.apache.hadoop.mrunit.internal.output.MockOutputCreator;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper instance. You provide the input key
 * and value that should be sent to the Mapper, and outputs you expect to be
 * sent by the Mapper to the collector for those inputs. By calling runTest(),
 * the harness will deliver the input to the Mapper and will check its outputs
 * against the expected results. This is designed to handle a single (k, v) ->
 * (k, v)* case from the Mapper, representing a single unit test. Multiple input
 * (k, v) pairs should go in separate unit tests.
 */
@SuppressWarnings("deprecation")
public class MapDriver<K1, V1, K2, V2> extends MapDriverBase<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(MapDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;
  private Counters counters;

  private final MockOutputCreator<K2, V2> mockOutputCreator = new MockOutputCreator<K2, V2>();

  public MapDriver(final Mapper<K1, V1, K2, V2> m) {
    this();
    setMapper(m);
  }

  public MapDriver() {
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
  public MapDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
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
  public MapDriver<K1, V1, K2, V2> withMapper(final Mapper<K1, V1, K2, V2> m) {
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
   * Identical to setInputKey() but with fluent programming style
   * 
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputKey(final K1 key) {
    setInputKey(key);
    return this;
  }

  /**
   * Identical to setInputValue() but with fluent programming style
   * 
   * @param val
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInputValue(final V1 val) {
    setInputValue(val);
    return this;
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   * 
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(final K1 key, final V1 val) {
    setInput(key, val);
    return this;
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   * 
   * @param inputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withInput(final Pair<K1, V1> inputRecord) {
    setInput(inputRecord);
    return this;
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(final Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return this;
  }

  /**
   * Functions like addOutput() but returns self for fluent programming style
   * 
   * @return this
   */
  public MapDriver<K1, V1, K2, V2> withOutput(final K2 key, final V2 val) {
    addOutput(key, val);
    return this;
  }

  /**
   * Identical to setInputFromString, but with a fluent programming style
   * 
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public MapDriver<K1, V1, K2, V2> withInputFromString(final String input) {
    setInputFromString(input);
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
  public MapDriver<K1, V1, K2, V2> withOutputFromString(final String output) {
    addOutputFromString(output);
    return this;
  }

  @Override
  public MapDriver<K1, V1, K2, V2> withCounter(final Enum e,
      final long expectedValue) {
    super.withCounter(e, expectedValue);
    return this;
  }

  @Override
  public MapDriver<K1, V1, K2, V2> withCounter(final String g, final String n,
      final long expectedValue) {
    super.withCounter(g, n, expectedValue);
    return this;
  }

  public MapDriver<K1, V1, K2, V2> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass,
      final Class<? extends InputFormat> inputFormatClass) {
    mockOutputCreator.setMapredFormats(outputFormatClass, inputFormatClass);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    if (inputKey == null || inputVal == null) {
      throw new IllegalStateException("No input was provided");
    }
    if (myMapper == null) {
      throw new IllegalStateException("No Mapper class was provided");
    }

    final OutputCollectable<K2, V2> outputCollectable = mockOutputCreator
        .createOutputCollectable(getConfiguration());
    final MockReporter reporter = new MockReporter(
        MockReporter.ReporterType.Mapper, getCounters());

    if (myMapper instanceof Configurable) {
      ((Configurable) myMapper).setConf(getConfiguration());
    }
    myMapper.configure(new JobConf(getConfiguration()));
    myMapper.map(inputKey, inputVal, outputCollectable, reporter);
    myMapper.close();
    return outputCollectable.getOutputs();
  }

  @Override
  public String toString() {
    return "MapDriver (" + myMapper + ")";
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper associated
   *          with the driver
   * @return this object for fluent coding
   */
  public MapDriver<K1, V1, K2, V2> withConfiguration(
      final Configuration configuration) {
    setConfiguration(configuration);
    return this;
  }

  /**
   * Returns a new MapDriver without having to specify the generic types on the
   * right hand side of the object create statement.
   * 
   * @return new MapDriver
   */
  public static <K1, V1, K2, V2> MapDriver<K1, V1, K2, V2> newMapDriver() {
    return new MapDriver<K1, V1, K2, V2>();
  }

  /**
   * Returns a new MapDriver without having to specify the generic types on the
   * right hand side of the object create statement.
   * 
   * @param mapper
   * @return new MapDriver
   */
  public static <K1, V1, K2, V2> MapDriver<K1, V1, K2, V2> newMapDriver(
      final Mapper<K1, V1, K2, V2> mapper) {
    return new MapDriver<K1, V1, K2, V2>(mapper);
  }
}
