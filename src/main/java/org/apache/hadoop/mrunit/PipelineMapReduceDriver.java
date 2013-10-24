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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a dataflow through a set of Mappers and
 * Reducers. You provide a set of (Mapper, Reducer) "jobs" that make up a
 * workflow, as well as a set of (key, value) pairs to pass in to the first
 * Mapper. You can also specify the outputs you expect to be sent to the final
 * Reducer in the pipeline.
 * 
 * By calling runTest(), the harness will deliver the input to the first Mapper,
 * feed the intermediate results to the first Reducer (without checking them),
 * and proceed to forward this data along to subsequent Mapper/Reducer jobs in
 * the pipeline until the final Reducer. The last Reducer's outputs are checked
 * against the expected results.
 * 
 * This is designed for slightly more complicated integration tests than the
 * MapReduceDriver, which is for smaller unit tests.
 * 
 * (K1, V1) in the type signature refer to the types associated with the inputs
 * to the first Mapper. (K2, V2) refer to the types associated with the final
 * Reducer's output. No intermediate types are specified.
 */
@SuppressWarnings("rawtypes")
public class PipelineMapReduceDriver<K1, V1, K2, V2> extends
    TestDriver<K1, V1, K2, V2, PipelineMapReduceDriver<K1, V1, K2, V2>> {

  public static final Log LOG = LogFactory
      .getLog(PipelineMapReduceDriver.class);

  private List<Pair<Mapper, Reducer>> mapReducePipeline;
  private final List<Pair<K1, V1>> inputList;
  private Counters counters;

  protected Path mapInputPath = new Path("somefile");

  public PipelineMapReduceDriver(final List<Pair<Mapper, Reducer>> pipeline) {
    this();
    mapReducePipeline = returnNonNull(pipeline);
  }

  public PipelineMapReduceDriver() {
    mapReducePipeline = new ArrayList<Pair<Mapper, Reducer>>();
    inputList = new ArrayList<Pair<K1, V1>>();
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
  public PipelineMapReduceDriver<K1, V1, K2, V2> withCounters(
      final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  /**
   * Add a Mapper and Reducer instance to the pipeline to use with this test
   * driver
   * 
   * @param m
   *          The Mapper instance to add to the pipeline
   * @param r
   *          The Reducer instance to add to the pipeline
   */
  public void addMapReduce(final Mapper m, final Reducer r) {
    mapReducePipeline.add(new Pair<Mapper, Reducer>(m, r));
  }

  /**
   * Add a Mapper and Reducer instance to the pipeline to use with this test
   * driver
   * 
   * @param p
   *          The Mapper and Reducer instances to add to the pipeline
   */
  public void addMapReduce(final Pair<Mapper, Reducer> p) {
    mapReducePipeline.add(returnNonNull(p));
  }

  /**
   * Add a Mapper and Reducer instance to the pipeline to use with this test
   * driver using fluent style
   * 
   * @param m
   *          The Mapper instance to use
   * @param r
   *          The Reducer instance to use
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(final Mapper m,
      final Reducer r) {
    addMapReduce(m, r);
    return this;
  }

  /**
   * Add a Mapper and Reducer instance to the pipeline to use with this test
   * driver using fluent style
   * 
   * @param p
   *          The Mapper and Reducer instances to add to the pipeline
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withMapReduce(
      final Pair<Mapper, Reducer> p) {
    addMapReduce(p);
    return this;
  }

  /**
   * @return A copy of the list of Mapper and Reducer objects under test
   */
  public List<Pair<Mapper, Reducer>> getMapReducePipeline() {
    return new ArrayList<Pair<Mapper, Reducer>>(mapReducePipeline);
  }

  /**
   * Adds an input to send to the mapper
   * 
   * @param key
   * @param val
   */
  public void addInput(final K1 key, final V1 val) {
    inputList.add(copyPair(key, val));
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
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param key
   * @param val
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(final K1 key,
      final V1 val) {
    addInput(key, val);
    return this;
  }

  /**
   * Adds an input to send to the Mapper
   * 
   * @param input
   *          The (k, v) pair to add to the input list.
   */
  public void addInput(final Pair<K1, V1> input) {
    addInput(input.getFirst(), input.getSecond());
  }

  /**
   * Identical to addInput() but returns self for fluent programming style
   * 
   * @param input
   *          The (k, v) pair to add
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInput(
      final Pair<K1, V1> input) {
    addInput(input);
    return this;
  }

  /**
   * Expects an input of the form "key \t val" Forces the Mapper input types to
   * Text.
   * 
   * @param input
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addInputFromString(final String input) {
    addInput((Pair<K1, V1>) parseTabbedPair(input));
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
  public PipelineMapReduceDriver<K1, V1, K2, V2> withInputFromString(
      final String input) {
    addInputFromString(input);
    return this;
  }

  /**
   * Identical to addAll() but returns self for fluent programming style
   * 
   * @param inputRecords input key/value pairs
   * @return this
   */
  public PipelineMapReduceDriver<K1, V1, K2, V2> withAll(
      final List<Pair<K1, V1>> inputRecords) {
    addAll(inputRecords);
    return this;
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
   * @return
   */
  public final PipelineMapReduceDriver<K1, V1, K2, V2> withMapInputPath(Path mapInputPath) {
    setMapInputPath(mapInputPath);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<Pair<K2, V2>> run() throws IOException {
    // inputs starts with the user-provided inputs.
    List<Pair<K1, V1>> inputs = inputList;

    if (inputs.isEmpty()) {
      throw new IllegalStateException("No input was provided");
    }
    if (mapReducePipeline.isEmpty()) {
      throw new IllegalStateException("No Mappers or Reducers in pipeline");
    }
    if (driverReused()) {
      throw new IllegalStateException("Driver reuse not allowed");
    }
    else {
      setUsedOnceStatus();
    }

    for (final Pair<Mapper, Reducer> job : mapReducePipeline) {
      // Create a MapReduceDriver to run this phase of the pipeline.
      final MapReduceDriver mrDriver = MapReduceDriver.newMapReduceDriver(
          job.getFirst(), job.getSecond());

      mrDriver.setCounters(getCounters());
      mrDriver.setConfiguration(getConfiguration());
      mrDriver.setMapInputPath(mapInputPath);

      // Add the inputs from the user, or from the previous stage of the
      // pipeline.
      for (final Pair<K1, V1> input : inputs) {
        mrDriver.addInput(input);
      }

      // Run the MapReduce "job". The output of this job becomes
      // the input to the next job.
      inputs = mrDriver.run();
    }

    // The last list of values stored in "inputs" is actually the outputs.
    // Unfortunately, due to the variable-length list of MR passes the user
    // can test, this is not type-safe.
    return (List) inputs;
  }

  /**
   * Returns a new PipelineMapReduceDriver without having to specify the generic
   * types on the right hand side of the object create statement.
   * 
   * @return new PipelineMapReduceDriver
   */
  public static <K1, V1, K2, V2> PipelineMapReduceDriver<K1, V1, K2, V2> newPipelineMapReduceDriver() {
    return new PipelineMapReduceDriver<K1, V1, K2, V2>();
  }

  /**
   * Returns a new PipelineMapReduceDriver without having to specify the generic
   * types on the right hand side of the object create statement.
   * 
   * @param pipeline
   *          passed to PipelineMapReduceDriver constructor
   * @return new PipelineMapReduceDriver
   */
  public static <K1, V1, K2, V2> PipelineMapReduceDriver<K1, V1, K2, V2> newPipelineMapReduceDriver(
      final List<Pair<Mapper, Reducer>> pipeline) {
    return new PipelineMapReduceDriver<K1, V1, K2, V2>(pipeline);
  }
}
