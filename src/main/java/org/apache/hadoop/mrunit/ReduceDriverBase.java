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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.internal.io.Serialization;
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
public abstract class ReduceDriverBase<K1, V1, K2, V2> extends
    TestDriver<K1, V1, K2, V2> {

  protected K1 inputKey;
  private final List<V1> inputValues;

  public ReduceDriverBase() {
    inputValues = new ArrayList<V1>();
  }

  /**
   * Returns a list which when iterated over, returns the same instance of the
   * value each time with different contents similar to how Hadoop currently
   * works with Writables.
   * 
   * @return List of values
   */
  public List<V1> getInputValues() {
    return new ValueClassInstanceReuseList<V1>(inputValues, getConfiguration());
  }

  /**
   * Sets the input key to send to the Reducer
   * 
   */
  public void setInputKey(final K1 key) {
    inputKey = copy(key);
  }

  /**
   * adds an input value to send to the reducer
   * 
   * @param val
   */
  public void addInputValue(final V1 val) {
    inputValues.add(copy(val));
  }

  /**
   * Sets the input values to send to the reducer; overwrites existing ones
   * 
   * @param values
   */
  public void setInputValues(final List<V1> values) {
    inputValues.clear();
    addInputValues(values);
  }

  /**
   * Adds a set of input values to send to the reducer
   * 
   * @param values
   */
  public void addInputValues(final List<V1> values) {
    for (V1 value : values) {
      addInputValue(value);
    }
  }

  /**
   * Sets the input to send to the reducer
   * 
   * @param key
   * @param values
   */
  public void setInput(final K1 key, final List<V1> values) {
    setInputKey(key);
    setInputValues(values);
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * 
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(final Pair<K2, V2> outputRecord) {
    addOutput(outputRecord.getFirst(), outputRecord.getSecond());
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * 
   * @param key
   *          The key part of a (k, v) pair to add
   * @param val
   *          The val part of a (k, v) pair to add
   */
  public void addOutput(final K2 key, final V2 val) {
    expectedOutputs.add(copyPair(key, val));
  }

  /**
   * Expects an input of the form "key \t val, val, val..." Forces the Reducer
   * input types to Text.
   * 
   * @param input
   *          A string of the form "key \t val,val,val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void setInputFromString(final String input) {
    final Pair<Text, Text> inputPair = parseTabbedPair(input);
    setInputKey((K1) inputPair.getFirst());
    setInputValues((List<V1>) parseCommaDelimitedList(inputPair.getSecond()
        .toString()));
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
    addOutput((Pair<K2, V2>) parseTabbedPair(output));
  }

  @Override
  public abstract List<Pair<K2, V2>> run() throws IOException;

  @Override
  public void runTest(final boolean orderMatters) {
    final StringBuilder sb = new StringBuilder();
    formatValueList(inputValues, sb);

    LOG.debug("Reducing input (" + inputKey + ", " + sb + ")");

    List<Pair<K2, V2>> outputs = null;
    try {
      outputs = run();
      validate(outputs, orderMatters);
      validate(counterWrapper);
    } catch (final IOException ioe) {
      LOG.error("IOException in reducer", ioe);
      throw new RuntimeException("IOException in reducer: ", ioe);
    }
  }

  protected static class ValueClassInstanceReuseList<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;
    private T value;
    private final Serialization serialization;

    @SuppressWarnings("unchecked")
    public ValueClassInstanceReuseList(final List<T> list,
        final Configuration conf) {
      super(list);
      serialization = new Serialization(conf);
    }

    @Override
    public Iterator<T> iterator() {
      final Iterator<T> listIterator = super.iterator();
      final T currentValue = value;
      return new Iterator<T>() {
        private T value = currentValue;
        private final Iterator<T> iterator = listIterator;

        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public T next() {
          final T next = iterator.next();
          value = serialization.copy(next, value);
          return value;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
}
