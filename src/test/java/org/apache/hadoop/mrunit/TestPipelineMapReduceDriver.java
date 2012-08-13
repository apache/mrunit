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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("rawtypes")
public class TestPipelineMapReduceDriver {

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int BAR_IN = 12;
  private static final int FOO_OUT = 52;
  private static final int BAR_OUT = 12;

  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();

  @Test
  public void testEmptyPipeline() throws IOException {
    thrown.expectMessage(IllegalStateException.class,
        "No Mappers or Reducers in pipeline");
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver.withInput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testNoInput() throws IOException {
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver.runTest();
  }

  @Test
  public void testSingleIdentity() throws IOException {
    // Test that an identity mapper and identity reducer work
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver
        .withMapReduce(new IdentityMapper<Text, Text>(),
            new IdentityReducer<Text, Text>())
        .withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testMultipleIdentities() throws IOException {
    // Test that a pipeline of identity mapper and reducers work
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver
        .withMapReduce(new IdentityMapper<Text, Text>(),
            new IdentityReducer<Text, Text>())
        .withMapReduce(new IdentityMapper<Text, Text>(),
            new IdentityReducer<Text, Text>())
        .withMapReduce(new IdentityMapper<Text, Text>(),
            new IdentityReducer<Text, Text>())
        .withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testSumAtEnd() throws IOException {
    final PipelineMapReduceDriver<Text, LongWritable, Text, LongWritable> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new IdentityReducer<Text, LongWritable>())
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new IdentityReducer<Text, LongWritable>())
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new LongSumReducer<Text>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testSumInMiddle() throws IOException {
    final PipelineMapReduceDriver<Text, LongWritable, Text, LongWritable> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new IdentityReducer<Text, LongWritable>())
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new LongSumReducer<Text>())
        .withMapReduce(new IdentityMapper<Text, LongWritable>(),
            new IdentityReducer<Text, LongWritable>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testNoMapper() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    thrown.expect(NullPointerException.class);
    driver.addMapReduce(null, new IdentityReducer<Text, Text>());
    driver.addInput(new Text("a"), new Text("b"));
    driver.runTest();
  }

  @Test
  public void testNoReducer() throws IOException {
    final List<Pair<Mapper, Reducer>> pipeline = new ArrayList<Pair<Mapper, Reducer>>();
    pipeline.add(new Pair<Mapper, Reducer>(new IdentityMapper<Text, Text>(),
        new IdentityReducer<Text, Text>()));
    pipeline.add(null);
    pipeline.add(new Pair<Mapper, Reducer>(new IdentityMapper<Text, Text>(),
        new IdentityReducer<Text, Text>()));
    thrown.expectMessage(NullPointerException.class, "entry 1 in list is null");
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver
        .newPipelineMapReduceDriver(pipeline);
    driver.addInput(new Text("a"), new Text("b"));
    driver.runTest();
  }

  @Test
  public void testWithCounter() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());
    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());

    driver.withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 2)
        .withCounter("category", "name", 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }
  
  @Test
  public void testWithCounterAndNoneMissing() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());
    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());

    driver.withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 2)
        .withCounter("category", "name", 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
            + " was not found in expected counters");

    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());
    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());

    driver.withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter("category", "name", 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"category\",\"name\")"
            + " was not found in expected counters");

    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());
    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());

    driver.withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithFailedCounter() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();

    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());
    driver.addMapReduce(
        new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>(),
        new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>());

    thrown
        .expectAssertionErrorMessage("2 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.TestMapDriver.MapperWithCounters.Counters.X has value 2 instead of expected 20, "
            + "Counter with category category and name name has value 2 instead of expected 20)");

    driver.withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 20)
        .withCounter("category", "name", 20).runTest();
  }

  @Test
  public void testJavaSerialization() throws IOException {
    final JobConf conf = new JobConf();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    conf.setOutputKeyComparatorClass(JavaSerializationComparator.class);
    conf.setOutputValueGroupingComparator(TestMapReduceDriver.INTEGER_COMPARATOR
        .getClass());
    final PipelineMapReduceDriver<Integer, IntWritable, Integer, IntWritable> driver = PipelineMapReduceDriver
        .newPipelineMapReduceDriver();
    driver.addMapReduce(new IdentityMapper<Integer, IntWritable>(),
        new IdentityReducer<Integer, IntWritable>());
    driver.addMapReduce(new IdentityMapper<Integer, IntWritable>(),
        new IdentityReducer<Integer, IntWritable>());
    driver.withConfiguration(conf);

    driver.withInput(1, new IntWritable(2)).withInput(2, new IntWritable(3));
    driver.withOutput(1, new IntWritable(2)).withOutput(2, new IntWritable(3));
    driver.runTest();
  }

  @Test
  public void testCopy() throws IOException {
    final PipelineMapReduceDriver<Text, Text, Text, Text> driver = PipelineMapReduceDriver
        .newPipelineMapReduceDriver();
    driver.withMapReduce(new IdentityMapper<Text, Text>(),
        new IdentityReducer<Text, Text>());
    final Text key = new Text("a");
    final Text value = new Text("b");
    driver.addInput(key, value);
    key.set("c");
    value.set("d");
    driver.addInput(key, value);

    key.set("a");
    value.set("b");
    driver.addOutput(key, value);
    key.set("c");
    value.set("d");
    driver.addOutput(key, value);
    driver.runTest();
  }
  
  @Test
  public void testMapInputFile() throws IOException {
    InputPathStoringMapper<LongWritable,LongWritable> mapper = 
        new InputPathStoringMapper<LongWritable,LongWritable>();
    Path mapInputPath = new Path("myfile");
    final PipelineMapReduceDriver<Text, LongWritable, Text, LongWritable> driver = PipelineMapReduceDriver
        .newPipelineMapReduceDriver();
    driver.addMapReduce(mapper, new IdentityReducer<Text, LongWritable>());
    driver.setMapInputPath(mapInputPath);
    assertEquals(mapInputPath.getName(), driver.getMapInputPath().getName());
    driver.withInput(new Text("a"), new LongWritable(1));
    driver.runTest();
    assertNotNull(mapper.getMapInputPath());
    assertEquals(mapInputPath.getName(), mapper.getMapInputPath().getName());
  }
}
