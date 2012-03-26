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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

/**
 * Test counters usage in various drivers.
 */
@SuppressWarnings("deprecation")
public class TestCounters {

  private static final String GROUP = "GROUP";
  private static final String ELEM = "ELEM";

  private class CounterMapper extends MapReduceBase implements
      Mapper<Text, Text, Text, Text> {
    @Override
    public void map(final Text k, final Text v,
        final OutputCollector<Text, Text> out, final Reporter r)
        throws IOException {

      r.incrCounter(GROUP, ELEM, 1);

      // Emit the same (k, v) pair twice.
      out.collect(k, v);
      out.collect(k, v);
    }
  }

  private class CounterReducer extends MapReduceBase implements
      Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(final Text k, final Iterator<Text> vals,
        final OutputCollector<Text, Text> out, final Reporter r)
        throws IOException {

      while (vals.hasNext()) {
        r.incrCounter(GROUP, ELEM, 1);
        out.collect(k, vals.next());
      }
    }
  }

  @Test
  public void testMapper() throws IOException {
    final Mapper<Text, Text, Text, Text> mapper = new CounterMapper();
    final MapDriver<Text, Text, Text, Text> driver = MapDriver
        .newMapDriver(mapper);
    driver.withInput(new Text("foo"), new Text("bar")).run();
    assertEquals("Expected 1 counter increment", 1, driver.getCounters()
        .findCounter(GROUP, ELEM).getValue());
  }

  @Test
  public void testReducer() throws IOException {
    final Reducer<Text, Text, Text, Text> reducer = new CounterReducer();
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver(reducer);
    driver.withInputKey(new Text("foo")).withInputValue(new Text("bar")).run();
    assertEquals("Expected 1 counter increment", 1, driver.getCounters()
        .findCounter(GROUP, ELEM).getValue());
  }

  @Test
  public void testMapReduce() throws IOException {
    final Mapper<Text, Text, Text, Text> mapper = new CounterMapper();
    final Reducer<Text, Text, Text, Text> reducer = new CounterReducer();
    final MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver(mapper, reducer);

    driver.withInput(new Text("foo"), new Text("bar")).run();

    assertEquals("Expected counter=3", 3,
        driver.getCounters().findCounter(GROUP, ELEM).getValue());
  }

  @Test
  public void testPipeline() throws IOException {
    final Mapper<Text, Text, Text, Text> mapper = new CounterMapper();
    final Reducer<Text, Text, Text, Text> reducer = new CounterReducer();
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    driver.withMapReduce(mapper, reducer).withMapReduce(mapper, reducer)
        .withInput(new Text("foo"), new Text("bar")).run();

    assertEquals("Expected counter=9", 9,
        driver.getCounters().findCounter(GROUP, ELEM).getValue());
  }
}
