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
 * expected results. 
 */
public abstract class ReduceDriverBase<K1, V1, K2, V2> extends
    TestDriver<K1, V1, K2, V2> {

  protected List<Pair<K1, List<V1>>> inputs = new ArrayList<Pair<K1, List<V1>>>();
  @Deprecated
  protected K1 inputKey;
  @Deprecated
  private final List<V1> inputValues;

  public ReduceDriverBase() {
    inputValues = new ArrayList<V1>();
  }

  /**
   * Returns a list of values.
   * 
   * @return List of values
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v*)*. Replaced by {@link #getInputValues(Object)}
   */
  @Deprecated
  public List<V1> getInputValues() {
    return inputValues;
  }

  /**
   * Returns a list of values for the given key
   * 
   * @param key
   * @return List for the given key, or null if key does not exist
   */
  public List<V1> getInputValues(final K1 key) {
    for (Pair<K1, List<V1>> p : inputs) {
      if (p.getFirst().equals(key)) {
        return p.getSecond();
      }
    }
    return null;
  }
  
  /**
   * Sets the input key to send to the Reducer
   * 
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v*)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
  public void setInputKey(final K1 key) {
    inputKey = copy(key);
  }

  /**
   * adds an input value to send to the reducer
   * 
   * @param val
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v*)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
  public void addInputValue(final V1 val) {
    inputValues.add(copy(val));
  }

  /**
   * Sets the input values to send to the reducer; overwrites existing ones
   * 
   * @param values
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v*)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
  public void setInputValues(final List<V1> values) {
    inputValues.clear();
    addInputValues(values);
  }

  /**
   * Adds a set of input values to send to the reducer
   * 
   * @param values
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v*)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
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
    
    clearInput();
    addInput(key, values);
  }
  
  /**
   * Clears the input to be sent to the Reducer
   */
  public void clearInput() {
    inputs.clear();
  }
  
  /**
   * Add input (K, V*) to send to the Reducer
   * 
   * @param key
   *          The key too add
   * @param values
   *          The list of values to add
   */
  public void addInput(final K1 key, final List<V1> values) {
    List<V1> copyVals = new ArrayList<V1>();
    for (V1 val : values) {
      copyVals.add(copy(val));
    }

    inputs.add(new Pair<K1, List<V1>>(copy(key),
        new ValueClassInstanceReuseList<V1>(copyVals, getConfiguration())));
  }
  
  /**
   * Add input (K, V*) to send to the Reducer
   * 
   * @param input
   *          input pair
   */
  public void addInput(final Pair<K1, List<V1>> input) {
    addInput(input.getFirst(), input.getSecond());
  }

  /**
   * Adds input to send to the Reducer
   * 
   * @param inputs
   *          list of (K, V*) pairs
   */
  public void addAll(final List<Pair<K1, List<V1>>> inputs) {
    for (Pair<K1, List<V1>> input : inputs) {
      addInput(input);
    }
  }
  
  /**
   * Adds output (k, v)* pairs we expect from the Reducer
   * 
   * @param outputRecords
   *          The (k, v)* pairs to add
   */
  public void addAllOutput(final List<Pair<K2, V2>> outputRecords) {
    for (Pair<K2, V2> output : outputRecords) {
      addOutput(output);
    }
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
  protected void printPreTestDebugLog() {
    final StringBuilder sb = new StringBuilder();
    for (Pair<K1, List<V1>> input : inputs) {
      formatValueList(input.getSecond(), sb);
      LOG.debug("Reducing input (" + input.getFirst() + ", " + sb + ")");
      sb.delete(0, sb.length());
    }
  }

  protected static class ValueClassInstanceReuseList<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;
    private T value;
    private final Serialization serialization;

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
