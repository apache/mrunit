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
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.mapred.MockReporter;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Harness that allows you to test a Reducer instance. You provide a key and a
 * set of intermediate values for that key that represent inputs that should be
 * sent to the Reducer (as if they came from a Mapper), and outputs you expect
 * to be sent by the Reducer to the collector. By calling runTest(), the harness
 * will deliver the input to the Reducer and will check its outputs against the
 * expected results.
 */
public class ReduceDriver<K1, V1, K2, V2> extends
    ReduceDriverBase<K1, V1, K2, V2, ReduceDriver<K1, V1, K2, V2>> {

  public static final Log LOG = LogFactory.getLog(ReduceDriver.class);

  private Reducer<K1, V1, K2, V2> myReducer;
  private Counters counters;

  public ReduceDriver(final Reducer<K1, V1, K2, V2> r) {
    this();
    setReducer(r);
  }

  public ReduceDriver() {
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
    counterWrapper = new CounterWrapper(ctrs);
  }

  /** Sets the counters to use and returns self for fluent style */
  public ReduceDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Sets the reducer object to use for this test
   * 
   * @param r
   *          The reducer object to use
   */
  public void setReducer(final Reducer<K1, V1, K2, V2> r) {
    myReducer = returnNonNull(r);
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

  /**
   * Configure {@link Reducer} to output with a real {@link OutputFormat}. Set
   * {@link InputFormat} to read output back in for use with {@link #run()} and
   * {@link #runTest()}
   * 
   * @param outputFormatClass
   * @param inputFormatClass
   * @return this for fluent style
   */
  @SuppressWarnings("rawtypes")
  public ReduceDriver<K1, V1, K2, V2> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass,
      final Class<? extends InputFormat> inputFormatClass) {
    mockOutputCreator.setMapredFormats(outputFormatClass, inputFormatClass);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    try {
      preRunChecks(myReducer);
      initDistributedCache();
      final OutputCollectable<K2, V2> outputCollectable = mockOutputCreator
          .createOutputCollectable(getConfiguration(),
              getOutputSerializationConfiguration());
      final MockReporter reporter = new MockReporter(
          MockReporter.ReporterType.Reducer, getCounters());

      ReflectionUtils.setConf(myReducer, new JobConf(getConfiguration()));

      for (Pair<K1, List<V1>> kv : inputs) {
        myReducer.reduce(kv.getFirst(), kv.getSecond().iterator(),
            outputCollectable, reporter);
      }
      myReducer.close();
      return outputCollectable.getOutputs();
    } finally {
      cleanupDistributedCache();  
    }
  }

  @Override
  public String toString() {
    return "ReduceDriver (" + myReducer + ")";
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
