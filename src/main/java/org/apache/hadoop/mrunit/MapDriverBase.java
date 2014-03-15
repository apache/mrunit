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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.internal.output.MockOutputCreator;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper instance. You provide the input
 * (k, v)* pairs that should be sent to the Mapper, and outputs you expect to be
 * sent by the Mapper to the collector for those inputs. By calling runTest(),
 * the harness will deliver the input to the Mapper and will check its outputs
 * against the expected results.
 */
public abstract class MapDriverBase<K1, V1, K2, V2, T extends MapDriverBase<K1, V1, K2, V2, T>>
    extends TestDriver<K1, V1, K2, V2, T> {

  public static final Log LOG = LogFactory.getLog(MapDriverBase.class);

  protected List<Pair<K1, V1>> inputs = new ArrayList<Pair<K1, V1>>();

  protected Path mapInputPath = new Path("somefile");

  @Deprecated
  protected K1 inputKey;
  @Deprecated
  protected V1 inputVal;

  protected final MockOutputCreator<K2, V2> mockOutputCreator = new MockOutputCreator<K2, V2>();

  /**
   * Sets the input key to send to the mapper
   * 
   * @param key
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
  public void setInputKey(final K1 key) {
    inputKey = copy(key);
  }
  @Deprecated
  public K1 getInputKey() {
    return inputKey;
  }

  /**
   * Sets the input value to send to the mapper
   * 
   * @param val
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v)*. Replaced by {@link #setInput()},
   *             {@link #addInput()}, and {@link #addAll()}
   */
  @Deprecated
  public void setInputValue(final V1 val) {
    inputVal = copy(val);
  }
  @Deprecated
  public V1 getInputValue() {
    return inputVal;
  }

  /**
   * Sets the input to send to the mapper
   * 
   */
  public void setInput(final K1 key, final V1 val) {
  	setInput(new Pair<K1, V1>(key, val));
  }

  /**
   * Sets the input to send to the mapper
   * 
   * @param inputRecord
   *          a (key, val) pair
   */
  @SuppressWarnings("deprecation")
  public void setInput(final Pair<K1, V1> inputRecord) {
    setInputKey(inputRecord.getFirst());
    setInputValue(inputRecord.getSecond());

    clearInput();
    addInput(inputRecord);
  }

  /**
   * Adds an input to send to the mapper
   * 
   * @param key
   * @param val
   */
  public void addInput(final K1 key, final V1 val) {
    inputs.add(copyPair(key, val));
  }

  /**
   * Adds an input to send to the mapper
   * 
   * @param input
   *          a (K, V) pair
   */
  public void addInput(final Pair<K1, V1> input) {
    addInput(input.getFirst(), input.getSecond());
  }

  /**
   * Adds list of inputs to send to the mapper
   * 
   * @param inputs
   *          list of (K, V) pairs
   */
  public void addAll(final List<Pair<K1, V1>> inputs) {
    for (Pair<K1, V1> input : inputs) {
      addInput(input);
    }
  }

  /**
   * Clears the list of inputs to send to the mapper
   */
  public void clearInput() {
    inputs.clear();
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper input types to
   * Text.
   * 
   * @param input
   *          A string of the form "key \t val".
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void setInputFromString(final String input) {
    final Pair<Text, Text> inputPair = parseTabbedPair(input);
    setInputKey((K1) inputPair.getFirst());
    setInputValue((V1) inputPair.getSecond());
  }

  @SuppressWarnings("unchecked")
  private T thisAsMapDriver() {
    return (T) this;
  }

  /**
   * Identical to setInputKey() but with fluent programming style
   * 
   * @return this
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v)*. Replaced by {@link #withInput()} and
   *             {@link #withAll()}
   */
  @Deprecated
  public T withInputKey(final K1 key) {
    setInputKey(key);
    return thisAsMapDriver();
  }

  /**
   * Identical to setInputValue() but with fluent programming style
   * 
   * @param val
   * @return this
   * @deprecated MRUNIT-64. Moved to list implementation to support multiple
   *             input (k, v)*. Replaced by {@link #withInput()} and
   *             {@link #withAll()}
   */
  @Deprecated
  public T withInputValue(final V1 val) {
    setInputValue(val);
    return thisAsMapDriver();
  }

  /**
   * Similar to setInput() but uses addInput() instead so accumulates values, and returns
   * the class instance, conforming to the fluent programming style
   *
   * @return this
   */
  public T withInput(final K1 key, final V1 val) {
    addInput(key, val);
    return thisAsMapDriver();
  }

  /**
   * Identical to setInput() but returns self for fluent programming style
   * 
   * @param inputRecord
   * @return this
   */
  public T withInput(final Pair<K1, V1> inputRecord) {
    setInput(inputRecord);
    return thisAsMapDriver();
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
  public T withInputFromString(final String input) {
    setInputFromString(input);
    return thisAsMapDriver();
  }

  /**
   * Identical to addAll() but returns self for fluent programming style
   * 
   * @param inputRecords
   * @return this
   */
  public T withAll(final List<Pair<K1, V1>> inputRecords) {
    addAll(inputRecords);
    return thisAsMapDriver();
  }

  /**
   * @return the path passed to the mapper InputSplit
   */
  public Path getMapInputPath() {
    return mapInputPath;
  }

  /**
   * @param mapInputPath Path which is to be passed to the mappers InputSplit
   */
  public void setMapInputPath(Path mapInputPath) {
    this.mapInputPath = mapInputPath;
  }

  /**
   * @param mapInputPath
   *       The Path object which will be given to the mapper
   * @return this
   */
  public final T withMapInputPath(Path mapInputPath) {
    setMapInputPath(mapInputPath);
    return thisAsTestDriver();
  }

  /**
   * Handle inputKey and inputVal for backwards compatibility.
   */
  @SuppressWarnings("deprecation")
  protected void preRunChecks(Object mapper) {
    if (inputKey != null && inputVal != null) {
      setInput(inputKey, inputVal);
    }

    if (inputs == null || inputs.isEmpty()) {
      throw new IllegalStateException("No input was provided");
    }

    if (mapper == null) {
      throw new IllegalStateException("No Mapper class was provided");
    }

    if (driverReused()) {
      throw new IllegalStateException("Driver reuse not allowed");
    }
    else {
     setUsedOnceStatus();
    }
  }

  @Override
  public abstract List<Pair<K2, V2>> run() throws IOException;

  @Override
  protected void printPreTestDebugLog() {
    for (Pair<K1, V1> input : inputs) {
      LOG.debug("Mapping input (" + input.getFirst() + ", " + input.getSecond() + ")");
    }
  }

}
