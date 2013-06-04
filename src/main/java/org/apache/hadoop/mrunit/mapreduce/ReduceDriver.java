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

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.ReduceDriverBase;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.mapreduce.ContextDriver;
import org.apache.hadoop.mrunit.internal.mapreduce.MockReduceContextWrapper;
import org.apache.hadoop.mrunit.types.KeyValueReuseList;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Reducer instance. You provide a key and a
 * set of intermediate values for that key that represent inputs that should be
 * sent to the Reducer (as if they came from a Mapper), and outputs you expect
 * to be sent by the Reducer to the collector. By calling runTest(), the harness
 * will deliver the input to the Reducer and will check its outputs against the
 * expected results.
 */
public class ReduceDriver<K1, V1, K2, V2> extends
    ReduceDriverBase<K1, V1, K2, V2, ReduceDriver<K1, V1, K2, V2>> implements
    ContextDriver {

  protected List<KeyValueReuseList<K1, V1>> groupedInputs = new ArrayList<KeyValueReuseList<K1, V1>>();

  public static final Log LOG = LogFactory.getLog(ReduceDriver.class);

  protected Reducer<K1, V1, K2, V2> myReducer;
  private Counters counters;
  /**
   * Context creator, do not use directly, always use the 
   * the getter as it lazily creates the object in the case
   * the setConfiguration() method will be used by the user.
   */
  private MockReduceContextWrapper<K1, V1, K2, V2> wrapper;

  public List<Pair<K1,V1>> getInputs(final K1 firstKey) {
	for (KeyValueReuseList<K1, V1> p : groupedInputs) {
      if(p.getCurrentKey().equals(firstKey)){
        return p;
      }
	}
    return null;
  }

  /**
   * Clears the input to be sent to the Reducer
   */
  @Override
  public void clearInput() {
    super.clearInput();
    groupedInputs.clear();
  }

  /**
   * Add input (K*, V*) to send to the Reducer
   *
   * @param key
   *          The key too add
   * @param values
   *          The value to add
   */
  public void addInput(final KeyValueReuseList<K1, V1> input) {
    groupedInputs.add(input.clone(getConfiguration()));
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   *
   * @param input
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withInput(final KeyValueReuseList<K1, V1> input) {
    addInput(input);
    return this;
  }

  /**
   * Identical to addAllElements() but returns self for fluent programming style
   *
   * @param inputs
   * @return this
   */
  public ReduceDriver<K1, V1, K2, V2> withAllElements(
    // This method is called withAllElements to avoid erasure conflict with withAll method from ReduceDriverBase.
      final List<KeyValueReuseList<K1, V1>> inputs) {
    addAllElements(inputs);
    return this;
  }

  /**
   * Adds input to send to the Reducer
   *
   * @param inputs
   *          list of (K*, V*) pairs
   */
  public void addAllElements(final List<KeyValueReuseList<K1, V1>> inputs) {
    // This method is called addAllElements to avoid erasure conflict with addAll method from ReduceDriverBase.
	for (KeyValueReuseList<K1, V1> input : inputs) {
		addInput(input);
	}
  }

  @Override
  protected void printPreTestDebugLog() {
    final StringBuilder sb = new StringBuilder();
    for (List<Pair<K1, V1>> input : groupedInputs) {
      formatPairList(input, sb);
      LOG.debug("Reducing input " + sb);
      sb.delete(0, sb.length());
    }
  }

  public ReduceDriver(final Reducer<K1, V1, K2, V2> r) {
    this();
    setReducer(r);
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

  /** @return the counters used in this test */
  @Override
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
   * Configure {@link Reducer} to output with a real {@link OutputFormat}. Set
   * {@link InputFormat} to read output back in for use with run* methods
   * 
   * @param outputFormatClass
   * @param inputFormatClass
   * @return this for fluent style
   */
  @SuppressWarnings("rawtypes")
  public ReduceDriver<K1, V1, K2, V2> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass,
      final Class<? extends InputFormat> inputFormatClass) {
    mockOutputCreator.setMapreduceFormats(outputFormatClass, inputFormatClass);
    return this;
  }

  /**
   * Handle inputKey and inputValues and inputs for backwards compatibility.
   */
  @Override
  protected void preRunChecks(Object reducer) {
    if (inputKey != null && !getInputValues().isEmpty()) {
      clearInput();
      addInput(new ReduceFeeder<K1, V1>(getConfiguration()).updateInput(inputKey, getInputValues()));
    }

    if (inputs != null && !inputs.isEmpty()){
      groupedInputs.clear();
      groupedInputs = new ReduceFeeder<K1, V1>(getConfiguration()).updateAll(inputs);
    }

    if (groupedInputs == null || groupedInputs.isEmpty()) {
      throw new IllegalStateException("No input was provided");
    }

    if (reducer == null) {
      throw new IllegalStateException("No Reducer class was provided");
    }
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    try {
      preRunChecks(myReducer);
      initDistributedCache();
      MockReduceContextWrapper<K1, V1, K2, V2> wrapper = getContextWrapper();
      myReducer.run(wrapper.getMockContext());
      return wrapper.getOutputs();
    } catch (final InterruptedException ie) {
      throw new IOException(ie);
    } finally {
      cleanupDistributedCache();
    }
  }

  @Override
  public String toString() {
    return "ReduceDriver (0.20+) (" + myReducer + ")";
  }

  private MockReduceContextWrapper<K1, V1, K2, V2> getContextWrapper() {
    if(wrapper == null) {
      wrapper = new MockReduceContextWrapper<K1, V1, K2, V2>(
          getConfiguration(), groupedInputs, mockOutputCreator, this);
    }
    return wrapper;
  }

  /**
   * <p>Obtain Context object for furthering mocking with Mockito.
   * For example, causing write() to throw an exception:</p>
   * 
   * <pre>
   * import static org.mockito.Matchers.*;
   * import static org.mockito.Mockito.*;
   * doThrow(new IOException()).when(context).write(any(), any());
   * </pre>
   * 
   * <p>Or implement other logic:</p>
   * 
   * <pre>
   * import static org.mockito.Matchers.*;
   * import static org.mockito.Mockito.*;
   * doAnswer(new Answer<Object>() {
   *    public Object answer(final InvocationOnMock invocation) {
   *    ...
   *     return null;
   *   }
   * }).when(context).write(any(), any());
   * </pre>
   * @return the mocked context
   */
  public Reducer<K1, V1, K2, V2>.Context getContext() {
    return getContextWrapper().getMockContext();
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
