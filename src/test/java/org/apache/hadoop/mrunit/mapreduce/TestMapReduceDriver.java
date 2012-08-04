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

import static org.apache.hadoop.mrunit.ExtendedAssert.assertListEquals;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.apache.hadoop.mrunit.TestMapReduceDriver.FirstCharComparator;
import org.apache.hadoop.mrunit.TestMapReduceDriver.SecondCharComparator;
import org.apache.hadoop.mrunit.mapreduce.TestMapDriver.ConfigurationMapper;
import org.apache.hadoop.mrunit.mapreduce.TestReduceDriver.ConfigurationReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.types.TestWritable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestMapReduceDriver {

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int BAR_IN = 12;
  private static final int FOO_OUT = 52;

  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();
  private Mapper<Text, LongWritable, Text, LongWritable> mapper;
  private Reducer<Text, LongWritable, Text, LongWritable> reducer;
  private MapReduceDriver<Text, LongWritable, Text, LongWritable, Text, LongWritable> driver;

  private MapReduceDriver<Text, Text, Text, Text, Text, Text> driver2;

  @Before
  public void setUp() throws Exception {
    mapper = new Mapper<Text, LongWritable, Text, LongWritable>(); // This is
                                                                   // the
                                                                   // IdentityMapper
    reducer = new LongSumReducer<Text>();
    driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    // for shuffle tests
    driver2 = MapReduceDriver.newMapReduceDriver();
  }

  @Test
  public void testRun() throws IOException {
    final List<Pair<Text, LongWritable>> out = driver
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN)).run();

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("bar"),
        new LongWritable(BAR_IN)));
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(FOO_OUT)));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() throws IOException {
    driver.withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testTestRun2() throws IOException {
    driver.withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testTestRun3() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, 52) but at "
            + "incorrect position 1 (expected position 0), "
            + "Matched expected output (bar, 12) but at incorrect position 0 (expected position 1))");
    driver.withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN)).runTest(true);
  }

  @Test
  public void testAddAll() throws IOException {
    final List<Pair<Text, LongWritable>> inputs = new ArrayList<Pair<Text, LongWritable>>();
    inputs.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(FOO_IN_A)));
    inputs.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(FOO_IN_B)));
    inputs.add(new Pair<Text, LongWritable>(new Text("bar"), new LongWritable(BAR_IN)));

    final List<Pair<Text, LongWritable>> outputs = new ArrayList<Pair<Text, LongWritable>>();
    outputs.add(new Pair<Text, LongWritable>(new Text("bar"), new LongWritable(BAR_IN)));
    outputs.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(FOO_OUT)));

    driver.withAll(inputs).withAllOutput(outputs).runTest();
  }
  
  @Test
  public void testTestRun3OrderInsensitive() throws IOException {
    driver.withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN)).runTest(false);
  }

  @Test
  public void testNoInput() throws IOException {
    driver = MapReduceDriver.newMapReduceDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  @Test
  public void testEmptyShuffle() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    final List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);
    assertEquals(0, outputs.size());
  }

  // just shuffle a single (k, v) pair
  @Test
  public void testSingleShuffle() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));

    final List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    final List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    final List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple values from the same key.
  @Test
  public void testShuffleOneKey() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("c")));

    final List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    final List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    final List<Text> sublist = new ArrayList<Text>();
    sublist.add(new Text("b"));
    sublist.add(new Text("c"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple keys
  @Test
  public void testMultiShuffle1() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    final List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    final List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    final List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    final List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }

  // shuffle multiple keys that are out-of-order to start.
  @Test
  public void testMultiShuffle2() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("z")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    final List<Pair<Text, List<Text>>> outputs = driver2.shuffle(inputs);

    final List<Pair<Text, List<Text>>> expected = new ArrayList<Pair<Text, List<Text>>>();
    final List<Text> sublist1 = new ArrayList<Text>();
    sublist1.add(new Text("x"));
    sublist1.add(new Text("y"));
    expected.add(new Pair<Text, List<Text>>(new Text("a"), sublist1));

    final List<Text> sublist2 = new ArrayList<Text>();
    sublist2.add(new Text("z"));
    sublist2.add(new Text("w"));
    expected.add(new Pair<Text, List<Text>>(new Text("b"), sublist2));

    assertListEquals(expected, outputs);
  }

  @Test
  public void testConfiguration() throws IOException {
    final Configuration conf = new Configuration();
    conf.set("TestKey", "TestValue");

    final MapReduceDriver<NullWritable, NullWritable, NullWritable, NullWritable, NullWritable, NullWritable> confDriver = MapReduceDriver
        .newMapReduceDriver();

    final ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable> mapper = new ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable>();
    final ConfigurationReducer<NullWritable, NullWritable, NullWritable, NullWritable> reducer = new ConfigurationReducer<NullWritable, NullWritable, NullWritable, NullWritable>();

    confDriver.withMapper(mapper).withReducer(reducer).withConfiguration(conf)
        .withInput(NullWritable.get(), NullWritable.get())
        .withOutput(NullWritable.get(), NullWritable.get()).runTest();
    assertEquals(mapper.setupConfiguration.get("TestKey"), "TestValue");
    assertEquals(reducer.setupConfiguration.get("TestKey"), "TestValue");
  }

  // Test the key grouping and value ordering comparators
  @Test
  public void testComparators() throws IOException {
    // reducer to track the order of the input values using bit shifting
    driver.withReducer(new Reducer<Text, LongWritable, Text, LongWritable>() {
      @Override
      protected void reduce(final Text key,
          final Iterable<LongWritable> values, final Context context)
          throws IOException, InterruptedException {
        long outputValue = 0;
        int count = 0;
        for (final LongWritable value : values) {
          outputValue |= (value.get() << (count++ * 8));
        }

        context.write(key, new LongWritable(outputValue));
      }
    });

    driver.withKeyGroupingComparator(new FirstCharComparator());
    driver.withKeyOrderComparator(new SecondCharComparator());

    driver.addInput(new Text("a1"), new LongWritable(1));
    driver.addInput(new Text("b1"), new LongWritable(1));
    driver.addInput(new Text("a3"), new LongWritable(3));
    driver.addInput(new Text("a2"), new LongWritable(2));

    driver.addOutput(new Text("a1"), new LongWritable(0x1));
    driver.addOutput(new Text("b1"), new LongWritable(0x1));
    driver.addOutput(new Text("a2"), new LongWritable(0x2 | (0x3 << 8)));

    driver.runTest();
  }

  // Test "combining" with an IdentityReducer. Result should be the same.
  @Test
  public void testIdentityCombiner() throws IOException {
    driver.withCombiner(new Reducer<Text, LongWritable, Text, LongWritable>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  // Test "combining" with another LongSumReducer. Result should be the same.
  @Test
  public void testLongSumCombiner() throws IOException {
    driver.withCombiner(new LongSumReducer<Text>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  // Test "combining" with another LongSumReducer, and with the Reducer
  // set to IdentityReducer. Result should be the same.
  @Test
  public void testLongSumCombinerAndIdentityReduce() throws IOException {
    driver.withCombiner(new LongSumReducer<Text>())
        .withReducer(new Reducer<Text, LongWritable, Text, LongWritable>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testNoMapper() throws IOException {
    driver = MapReduceDriver.newMapReduceDriver();
    driver.withReducer(reducer).withInput(new Text("a"), new LongWritable(0));
    thrown.expectMessage(IllegalStateException.class,
        "No Mapper class was provided");
    driver.runTest();
  }

  @Test
  public void testNoReducer() throws IOException {
    driver = MapReduceDriver.newMapReduceDriver();
    driver.withMapper(mapper).withInput(new Text("a"), new LongWritable(0));
    thrown.expectMessage(IllegalStateException.class,
        "No Reducer class was provided");
    driver.runTest();
  }

  @Test
  public void testWithCounter() throws IOException {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver();

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 1)
        .withCounter("category", "count", 1).withCounter("category", "sum", 1)
        .runTest();
  }
  
  @Test
  public void testWithCounterAndNoneMissing() throws IOException {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver();

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 1)
        .withCounter("category", "count", 1).withCounter("category", "sum", 1)
        .runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver();

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.mapreduce.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
            + " was not found in expected counters");

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter("category", "name", 1)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 1)
        .withCounter("category", "count", 1).withCounter("category", "sum", 1)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver();

    thrown.expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"name\")" + " was not found in expected counters");

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 1)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 1)
        .withCounter("category", "count", 1).withCounter("category", "sum", 1)
        .runTest();
  }

  @Test
  public void testWithFailedCounter() throws IOException {
    MapReduceDriver<Text, Text, Text, Text, Text, Text> driver = MapReduceDriver
        .newMapReduceDriver();

    thrown
        .expectAssertionErrorMessage("2 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.mapreduce.TestMapDriver.MapperWithCounters.Counters.X have value 1 instead of expected 20, "
            + "Counter with category category and name name have value 1 instead of expected 20)");

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 20)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter("category", "name", 20).runTest();
  }

  @Test
  public void testJavaSerialization() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final MapReduceDriver<IntWritable, Integer, Integer, IntWritable, Integer, IntWritable> driver = MapReduceDriver
        .newMapReduceDriver(new InverseMapper<IntWritable, Integer>(),
            new IntSumReducer<Integer>()).withConfiguration(conf);
    driver
        .setKeyGroupingComparator(org.apache.hadoop.mrunit.TestMapReduceDriver.INTEGER_COMPARATOR);
    driver.setKeyOrderComparator(new JavaSerializationComparator<Integer>());
    driver.withInput(new IntWritable(1), 2).withInput(new IntWritable(2), 3);
    driver.withOutput(2, new IntWritable(1)).withOutput(3, new IntWritable(2))
        .runTest();
  }

  @Test
  public void testOutputFormat() throws IOException {
    driver.withOutputFormat(SequenceFileOutputFormat.class,
        SequenceFileInputFormat.class);
    driver.withInput(new Text("a"), new LongWritable(1));
    driver.withInput(new Text("a"), new LongWritable(2));
    driver.withOutput(new Text("a"), new LongWritable(3));
    driver.runTest();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testOutputFormatWithMismatchInOutputClasses() throws IOException {
    final MapReduceDriver driver = this.driver;
    driver.withOutputFormat(TextOutputFormat.class, TextInputFormat.class);
    driver.withInput(new Text("a"), new LongWritable(1));
    driver.withInput(new Text("a"), new LongWritable(2));
    driver.withOutput(new LongWritable(), new Text("a\t3"));
    driver.runTest();
  }

  private static class InputPathStoringMapper extends
      Mapper<Text, LongWritable, Text, LongWritable> {
    private Path mapInputPath;

    @Override
    public void map(Text key, LongWritable value, Context context)
        throws IOException {
      if (context.getInputSplit() instanceof FileSplit) {
        mapInputPath = ((FileSplit) context.getInputSplit()).getPath();
      }
    }

    private Path getMapInputPath() {
      return mapInputPath;
    }
  }

  @Test
  public void testMapInputFile() throws IOException {
    InputPathStoringMapper mapper = new InputPathStoringMapper();
    Path mapInputPath = new Path("myfile");
    driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    driver.setMapInputPath(mapInputPath);
    assertEquals(mapInputPath.getName(), driver.getMapInputPath().getName());
    driver.withInput(new Text("a"), new LongWritable(1));
    driver.runTest();
    assertNotNull(mapper.getMapInputPath());
    assertEquals(mapInputPath.getName(), mapper.getMapInputPath().getName());
  }

  @Test
  public void testGroupingComparatorBehaviour1() throws IOException {
    driver.withInput(new Text("A1"),new LongWritable(1L))
      .withInput(new Text("A2"),new LongWritable(1L))
      .withInput(new Text("B1"),new LongWritable(1L))
      .withInput(new Text("B2"),new LongWritable(1L))
      .withInput(new Text("C1"),new LongWritable(1L))
      .withOutput(new Text("A1"),new LongWritable(2L))
      .withOutput(new Text("B1"),new LongWritable(2L))
      .withOutput(new Text("C1"),new LongWritable(1L))
      .withKeyGroupingComparator(new FirstCharComparator())
      .runTest(false);
  }

  @Test
  public void testGroupingComparatorBehaviour2() throws IOException {
    // this test fails pre-MRUNIT-127 because of the incorrect 
    // grouping of reduce keys in "shuffle". 
    // MapReduce doesn't group keys which aren't in a contiguous
    // range when sorted by their sorting comparator. 
    driver.withInput(new Text("1A"),new LongWritable(1L))
      .withInput(new Text("2A"),new LongWritable(1L))
      .withInput(new Text("1B"),new LongWritable(1L))
      .withInput(new Text("2B"),new LongWritable(1L))
      .withInput(new Text("1C"),new LongWritable(1L))
      .withOutput(new Text("1A"),new LongWritable(1L))
      .withOutput(new Text("2A"),new LongWritable(1L))
      .withOutput(new Text("1B"),new LongWritable(1L))
      .withOutput(new Text("2B"),new LongWritable(1L))
      .withOutput(new Text("1C"),new LongWritable(1L))
      .withKeyGroupingComparator(new SecondCharComparator())
      .runTest(false);
  }

  @Test
  public void testUseOfWritableRegisteredComparator() throws IOException {
    
    // this test should use the comparator registered inside TestWritable
    // to output the keys in reverse order
    MapReduceDriver<TestWritable,Text,TestWritable,Text,TestWritable,Text> driver 
      = MapReduceDriver.newMapReduceDriver(new Mapper(), new Reducer());
    
    driver.withInput(new TestWritable("A1"), new Text("A1"))
      .withInput(new TestWritable("A2"), new Text("A2"))
      .withInput(new TestWritable("A3"), new Text("A3"))
      .withKeyGroupingComparator(new TestWritable.SingleGroupComparator())
      // TODO: these output keys are incorrect because of MRUNIT-129 
      .withOutput(new TestWritable("A3"), new Text("A3"))
      .withOutput(new TestWritable("A3"), new Text("A2"))
      .withOutput(new TestWritable("A3"), new Text("A1"))
      //the following are the actual correct outputs
      //.withOutput(new TestWritable("A3"), new Text("A3"))
      //.withOutput(new TestWritable("A2"), new Text("A2"))
      //.withOutput(new TestWritable("A1"), new Text("A1"))
      .runTest(true); //ordering is important
  }

}
