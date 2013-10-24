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

import static org.apache.hadoop.mrunit.ExtendedAssert.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
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
    mapper = new IdentityMapper<Text, LongWritable>();
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
    thrown.expectAssertionErrorMessage("2 Error(s)");
    thrown.expectAssertionErrorMessage("Matched expected output (foo, 52) but "
        + "at incorrect position 1 (expected position 0)");
    thrown.expectAssertionErrorMessage("Matched expected output (bar, 12) but "
        + "at incorrect position 0 (expected position 1)");
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
  public void testDuplicateOutputOrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar))");
    driver2.withMapper(new IdentityMapper<Text, Text>()).withReducer(
        new IdentityReducer<Text, Text>());
    driver2.withInput(new Text("foo"), new Text("bar"))
        .withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
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

  // Test "combining" with an IdentityReducer. Result should be the same.
  @Test
  public void testIdentityCombiner() throws IOException {
    driver.withCombiner(new IdentityReducer<Text, LongWritable>())
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
        .withReducer(new IdentityReducer<Text, LongWritable>())
        .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("bar"), new LongWritable(BAR_IN))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
  }

  @Test
  public void testRepeatRun() throws IOException {
    driver.withCombiner(new IdentityReducer<Text, LongWritable>())
            .withInput(new Text("foo"), new LongWritable(FOO_IN_A))
            .withInput(new Text("foo"), new LongWritable(FOO_IN_B))
            .withInput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("bar"), new LongWritable(BAR_IN))
            .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest();
    thrown.expectMessage(IllegalStateException.class, "Driver reuse not allowed");
    driver.runTest();
  }

  /**
   * group comparator - group by first character
   */
  public static class FirstCharComparator implements RawComparator<Text> {

    @Override
    public int compare(final Text o1, final Text o2) {
      return o1.toString().substring(0, 1)
          .compareTo(o2.toString().substring(0, 1));
    }

    @Override
    public int compare(final byte[] arg0, final int arg1, final int arg2,
        final byte[] arg3, final int arg4, final int arg5) {
      throw new RuntimeException("Not implemented");
    }

  }

  /**
   * value order comparator - order by second character
   */
  public static class SecondCharComparator implements RawComparator<Text> {
    @Override
    public int compare(final Text o1, final Text o2) {
      return o1.toString().substring(1, 2)
          .compareTo(o2.toString().substring(1, 2));
    }

    @Override
    public int compare(final byte[] arg0, final int arg1, final int arg2,
        final byte[] arg3, final int arg4, final int arg5) {
      throw new RuntimeException("Not implemented");
    }
  }

  // Test the key grouping and value ordering comparators
  @Test
  public void testComparators() throws IOException {
    // reducer to track the order of the input values using bit shifting
    driver.withReducer(new Reducer<Text, LongWritable, Text, LongWritable>() {
      @Override
      public void reduce(final Text key, final Iterator<LongWritable> values,
          final OutputCollector<Text, LongWritable> output,
          final Reporter reporter) throws IOException {
        long outputValue = 0;
        int count = 0;
        while (values.hasNext()) {
          outputValue |= (values.next().get() << (count++ * 8));
        }

        output.collect(key, new LongWritable(outputValue));
      }

      @Override
      public void configure(final JobConf job) {
      }

      @Override
      public void close() throws IOException {
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
        .withOutput(new Text("hie"), new Text("Hi"))
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
        .withOutput(new Text("hie"), new Text("Hi"))
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
            + "\"org.apache.hadoop.mrunit.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
            + " was not found in expected counters");

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
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
        .withOutput(new Text("hie"), new Text("Hi"))
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
            + "Counter org.apache.hadoop.mrunit.TestMapDriver.MapperWithCounters.Counters.X has value 1 instead of expected 20, "
            + "Counter with category category and name name has value 1 instead of expected 20)");

    driver
        .withMapper(
            new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 20)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter("category", "name", 20).runTest();
  }

  public static final RawComparator<Integer> INTEGER_COMPARATOR = new RawComparator<Integer>() {

    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      throw new UnsupportedOperationException();
    }

  };

  @Test
  public void testJavaSerialization() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final MapReduceDriver<Integer, IntWritable, Integer, IntWritable, Integer, IntWritable> driver = MapReduceDriver
        .newMapReduceDriver(new IdentityMapper<Integer, IntWritable>(),
            new IdentityReducer<Integer, IntWritable>())
        .withConfiguration(conf);
    driver.withKeyOrderComparator(new JavaSerializationComparator<Integer>());
    driver.withKeyGroupingComparator(INTEGER_COMPARATOR);
    driver.withInput(1, new IntWritable(2)).withInput(2, new IntWritable(3));
    driver.withOutput(1, new IntWritable(2)).withOutput(2, new IntWritable(3));
    driver.runTest();
  }

  @Test
  public void testCopy() throws IOException {
    final Text key = new Text("a");
    final LongWritable value = new LongWritable(1);
    driver.addInput(key, value);
    key.set("b");
    value.set(2);
    driver.addInput(key, value);

    key.set("a");
    value.set(1);
    driver.addOutput(key, value);
    key.set("b");
    value.set(2);
    driver.addOutput(key, value);
    driver.runTest();
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

  @Test
  public void testMapInputFile() throws IOException {
    InputPathStoringMapper<LongWritable,LongWritable> mapper = 
        new InputPathStoringMapper<LongWritable,LongWritable>();
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
  public void testGroupingComparatorSpecifiedByConf() throws IOException {
    JobConf conf = new JobConf(new Configuration());
    conf.setOutputValueGroupingComparator(FirstCharComparator.class);
    driver.withInput(new Text("A1"),new LongWritable(1L))
      .withInput(new Text("A2"),new LongWritable(1L))
      .withInput(new Text("B1"),new LongWritable(1L))
      .withInput(new Text("B2"),new LongWritable(1L))
      .withInput(new Text("C1"),new LongWritable(1L))
      .withOutput(new Text("A1"),new LongWritable(2L))
      .withOutput(new Text("B1"),new LongWritable(2L))
      .withOutput(new Text("C1"),new LongWritable(1L))
      .withConfiguration(conf)
      .runTest(false);
  }

  @Test
  public void testUseOfWritableRegisteredComparator() throws IOException {
    MapReduceDriver<TestWritable, Text, TestWritable, Text, TestWritable, Text> driver = MapReduceDriver
        .newMapReduceDriver(new IdentityMapper<TestWritable, Text>(),
            new IdentityReducer<TestWritable, Text>());
    driver.withInput(new TestWritable("A1"), new Text("A1"))
      .withInput(new TestWritable("A2"), new Text("A2"))
      .withInput(new TestWritable("A3"), new Text("A3"))
      .withKeyGroupingComparator(new TestWritable.SingleGroupComparator())
      .withOutput(new TestWritable("A3"), new Text("A3"))
      .withOutput(new TestWritable("A3"), new Text("A2"))
      .withOutput(new TestWritable("A3"), new Text("A1"))
      .runTest(true); //ordering is important
  }

}
