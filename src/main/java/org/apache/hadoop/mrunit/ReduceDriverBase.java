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

import static org.apache.hadoop.mrunit.Serialization.copy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

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
  private List<V1> inputValues;

  public ReduceDriverBase() {
    inputValues = new ArrayList<V1>();
  }

  /**
   * Returns a list which when iterated over, returns
   * the same instance of the value each time with different
   * contents similar to how Hadoop currently works with 
   * Writables.
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
  public void setInputKey(K1 key) {
    inputKey = key;
  }

  /**
   * adds an input value to send to the reducer
   * 
   * @param val
   */
  public void addInputValue(V1 val) {
    inputValues.add(val);
  }

  /**
   * Sets the input values to send to the reducer; overwrites existing ones
   * 
   * @param values
   */
  public void setInputValues(List<V1> values) {
    inputValues.clear();
    inputValues.addAll(values);
  }

  /**
   * Adds a set of input values to send to the reducer
   * 
   * @param values
   */
  public void addInputValues(List<V1> values) {
    inputValues.addAll(values);
  }

  /**
   * Sets the input to send to the reducer
   * 
   * @param values
   */
  public void setInput(K1 key, List<V1> values) {
    setInputKey(key);
    setInputValues(values);
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * 
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(Pair<K2, V2> outputRecord) {
    if (null != outputRecord) {
      expectedOutputs.add(outputRecord);
    } else {
      throw new IllegalArgumentException("Tried to add null outputRecord");
    }
  }

  /**
   * Adds an output (k, v) pair we expect from the Reducer
   * 
   * @param key
   *          The key part of a (k, v) pair to add
   * @param val
   *          The val part of a (k, v) pair to add
   */
  public void addOutput(K2 key, V2 val) {
    addOutput(new Pair<K2, V2>(key, val));
  }

  /**
   * Expects an input of the form "key \t val, val, val..." Forces the Reducer
   * input types to Text.
   * 
   * @param input
   *          A string of the form "key \t val,val,val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void setInputFromString(String input) {
    if (null == input) {
      throw new IllegalArgumentException("null input");
    } else {
      Pair<Text, Text> inputPair = parseTabbedPair(input);
      if (null != inputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        setInputKey((K1) inputPair.getFirst());
        setInputValues((List<V1>) parseCommaDelimitedList(inputPair.getSecond()
            .toString()));
      } else {
        throw new IllegalArgumentException("Could not parse input pair");
      }
    }
  }

  /**
   * Expects an input of the form "key \t val" Forces the Reducer output types
   * to Text.
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addOutputFromString(String output) {
    if (null == output) {
      throw new IllegalArgumentException("null input");
    } else {
      Pair<Text, Text> outputPair = parseTabbedPair(output);
      if (null != outputPair) {
        // I know this is not type-safe, but I don't know a better way to do
        // this.
        addOutput((Pair<K2, V2>) outputPair);
      } else {
        throw new IllegalArgumentException("Could not parse output pair");
      }
    }
  }

  public abstract List<Pair<K2, V2>> run() throws IOException;

  @Override
  public void runTest() {

    String inputKeyStr = "(null)";

    if (null != inputKey) {
      inputKeyStr = inputKey.toString();
    }

    StringBuilder sb = new StringBuilder();
    formatValueList(inputValues, sb);

    LOG.debug("Reducing input (" + inputKeyStr + ", " + sb.toString() + ")");

    List<Pair<K2, V2>> outputs = null;
    try {
      outputs = run();
      validate(outputs);
    } catch (IOException ioe) {
      LOG.error("IOException in reducer", ioe);
      throw new RuntimeException("IOException in reducer: ", ioe);
    }
  }

  protected static class ValueClassInstanceReuseList<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;
    private T value;
    private final Configuration conf;
    @SuppressWarnings("unchecked")
    public ValueClassInstanceReuseList(List<T> list, Configuration conf) {
      super(list);
      this.conf = conf;
      if (!list.isEmpty()) {
        T first = list.get(0);
        Class<T> klass = (Class<T>) first.getClass();
        this.value = ReflectionUtils.newInstance(klass, conf);
      }
    }

    @Override
    public Iterator<T> iterator() {
      final Iterator<T> listIterator = super.iterator();
      final T currentValue = value;
      return new Iterator<T>() {
        private T value = currentValue;
        private Iterator<T> iterator = listIterator;
        @Override
        public boolean hasNext() {
          return iterator.hasNext();
        }

        @Override
        public T next() {
          T next = iterator.next();
          copy(next, value, conf);
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
