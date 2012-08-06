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

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mrunit.MapDriverBase;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.mapreduce.ContextDriver;
import org.apache.hadoop.mrunit.internal.mapreduce.MockMapContextWrapper;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * Harness that allows you to test a Mapper instance. You provide the input
 * (k, v)* pairs that should be sent to the Mapper, and outputs you expect to be
 * sent by the Mapper to the collector for those inputs. By calling runTest(),
 * the harness will deliver the input to the Mapper and will check its outputs
 * against the expected results.
 */
public class MapDriver<K1, V1, K2, V2> 
extends MapDriverBase<K1, V1, K2, V2, MapDriver<K1, V1, K2, V2> > implements ContextDriver {
  
  public static final Log LOG = LogFactory.getLog(MapDriver.class);

  private Mapper<K1, V1, K2, V2> myMapper;
  private Counters counters;

  private final MockMapContextWrapper<K1, V1, K2, V2> wrapper = new MockMapContextWrapper<K1, V1, K2, V2>(
      inputs, mockOutputCreator,  this);


  public MapDriver(final Mapper<K1, V1, K2, V2> m) {
    this();
    setMapper(m);
  }

  public MapDriver() {
    setCounters(new Counters());
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
    counters = ctrs;
    counterWrapper = new CounterWrapper(counters);
  }

  /** Sets the counters to use and returns self for fluent style */
  public MapDriver<K1, V1, K2, V2> withCounters(final Counters ctrs) {
    setCounters(ctrs);
    return this;
  }

  @SuppressWarnings("rawtypes")
  public MapDriver<K1, V1, K2, V2> withOutputFormat(
      final Class<? extends OutputFormat> outputFormatClass,
      final Class<? extends InputFormat> inputFormatClass) {
    mockOutputCreator.setMapreduceFormats(outputFormatClass, inputFormatClass);
    return this;
  }

  @Override
  public List<Pair<K2, V2>> run() throws IOException {
    preRunChecks(myMapper);

    try {
      myMapper.run(wrapper.getMockContext());
      return wrapper.getOutputs();
    } catch (final InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  @Override
  public String toString() {
    return "MapDriver (0.20+) (" + myMapper + ")";
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
  public Mapper<K1, V1, K2, V2>.Context getContext() {
    return wrapper.getMockContext();
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
   *          passed to MapDriver constructor
   * @return new MapDriver
   */
  public static <K1, V1, K2, V2> MapDriver<K1, V1, K2, V2> newMapDriver(
      final Mapper<K1, V1, K2, V2> mapper) {
    return new MapDriver<K1, V1, K2, V2>(mapper);
  }
}
