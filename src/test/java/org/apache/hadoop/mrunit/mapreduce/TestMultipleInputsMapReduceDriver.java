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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.apache.hadoop.mrunit.TestMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.types.TestWritable;
import org.apache.hadoop.mrunit.types.UncomparableWritable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.mrunit.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestMultipleInputsMapReduceDriver {
  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();

  private static final int FOO_IN_A = 42;
  private static final int FOO_IN_B = 10;
  private static final int TOKEN_IN_A = 1;
  private static final int TOKEN_IN_B = 2;
  private static final int BAR_IN = 12;
  private static final int BAR_OUT = BAR_IN + TOKEN_IN_A + TOKEN_IN_B;
  private static final int FOO_OUT = FOO_IN_A + FOO_IN_B + TOKEN_IN_A + 2
      * TOKEN_IN_B;
  private static final String TOKEN_A = "foo bar";
  private static final String TOKEN_B = "foo foo bar";

  private Mapper<Text, LongWritable, Text, LongWritable> mapper;
  private Reducer<Text, LongWritable, Text, LongWritable> reducer;
  private TokenMapper tokenMapper;
  private MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable> driver;

  @Before
  public void setUp() {
    mapper = new Mapper<Text, LongWritable, Text, LongWritable>();
    reducer = new LongSumReducer<Text>();
    tokenMapper = new TokenMapper();
    driver = new MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable>(
        reducer);
    driver.addMapper(mapper);
    driver.addMapper(tokenMapper);
  }

  @Test
  public void testRun() throws IOException {
    final List<Pair<Text, LongWritable>> out = driver
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .run();

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("bar"),
        new LongWritable(BAR_OUT)));
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(FOO_OUT)));

    assertListEquals(expected, out);
  }

  @Test
  public void testUncomparable() throws IOException {
    MultipleInputsMapReduceDriver<Text, Object, Text, Object> testDriver = MultipleInputsMapReduceDriver
        .newMultipleInputMapReduceDriver(new Reducer<Text, Object, Text, Object>());

    Mapper<Text, Object, Text, Object> identity = new Mapper<Text, Object, Text, Object>();
    testDriver.addMapper(identity);
    Text k1 = new Text("foo");
    Object v1 = new UncomparableWritable(1);
    testDriver.withInput(identity, k1, v1);

    ReverseMapper<Object, Text> reverse = new ReverseMapper<Object, Text>();
    testDriver.addMapper(reverse);
    Text k2 = new Text("bar");
    Object v2 = new UncomparableWritable(2);
    testDriver.withInput(reverse, v2, k2);

    testDriver.withOutput(k1, v1).withOutput(k2, v2);

    testDriver.runTest(false);
  }

  @Test
  public void testTestRun() throws IOException {
    driver
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT)).runTest(false);
  }

  @Test
  public void testAddAll() throws IOException {
    final List<Pair<Text, LongWritable>> mapperInputs = new ArrayList<Pair<Text, LongWritable>>();
    mapperInputs.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(FOO_IN_A)));
    mapperInputs.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(FOO_IN_B)));
    mapperInputs.add(new Pair<Text, LongWritable>(new Text("bar"),
        new LongWritable(BAR_IN)));

    final List<Pair<LongWritable, Text>> tokenMapperInputs = new ArrayList<Pair<LongWritable, Text>>();
    tokenMapperInputs.add(new Pair<LongWritable, Text>(new LongWritable(
        TOKEN_IN_A), new Text(TOKEN_A)));
    tokenMapperInputs.add(new Pair<LongWritable, Text>(new LongWritable(
        TOKEN_IN_B), new Text(TOKEN_B)));

    final List<Pair<Text, LongWritable>> outputs = new ArrayList<Pair<Text, LongWritable>>();
    outputs.add(new Pair<Text, LongWritable>(new Text("bar"), new LongWritable(
        BAR_OUT)));
    outputs.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(
        FOO_OUT)));

    driver.withAll(mapper, mapperInputs)
        .withAll(tokenMapper, tokenMapperInputs).withAllOutput(outputs)
        .runTest(false);
  }

  @Test
  public void testNoInput() throws IOException {
    thrown.expectMessage(IllegalStateException.class,
        "No input was provided for mapper");
    driver.runTest(false);
  }

  @Test
  public void testNoInputForMapper() throws IOException {
    MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable> testDriver = new MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable>();
    testDriver.addMapper(mapper);
    testDriver.addMapper(tokenMapper);
    testDriver.withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A));
    thrown.expectMessage(IllegalStateException.class,
        String.format("No input was provided for mapper %s", tokenMapper));
    testDriver.runTest(false);
  }

  @Test
  public void testNoReducer() throws IOException {
    MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable> testDriver = new MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable>();
    testDriver.addMapper(mapper);
    testDriver.withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A));
    thrown.expectMessage(IllegalStateException.class,
        "No reducer class was provided");
    testDriver.runTest(false);
  }

  @Test
  public void testIdentityCombiner() throws IOException {
    driver
        .withCombiner(new Reducer<Text, LongWritable, Text, LongWritable>())
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT)).runTest(false);
  }

  @Test
  public void testLongSumCombiner() throws IOException {
    driver
        .withCombiner(new LongSumReducer<Text>())
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT)).runTest(false);
  }

  @Test
  public void testLongSumCombinerAndIdentityReducer() throws IOException {
    driver
        .withCombiner(new LongSumReducer<Text>())
        .withReducer(new Reducer<Text, LongWritable, Text, LongWritable>())
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT)).runTest(false);
  }

  @Test
  public void testRepeatRun() throws IOException {
    driver
        .withCombiner(new Reducer<Text, LongWritable, Text, LongWritable>())
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_A))
        .withInput(mapper, new Text("foo"), new LongWritable(FOO_IN_B))
        .withInput(mapper, new Text("bar"), new LongWritable(BAR_IN))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_A), new Text(TOKEN_A))
        .withInput(tokenMapper, new LongWritable(TOKEN_IN_B), new Text(TOKEN_B))
        .withOutput(new Text("foo"), new LongWritable(FOO_OUT))
        .withOutput(new Text("bar"), new LongWritable(BAR_OUT)).runTest(false);
    thrown.expectMessage(IllegalStateException.class,
        "Driver reuse not allowed");
    driver.runTest(false);
  }

  // Test the key grouping and value ordering comparators
  @Test
  public void testComparators() throws IOException {
    // reducer to track the order of the input values using bit shifting
    driver.withReducer(new Reducer<Text, LongWritable, Text, LongWritable>() {
      @Override
      protected void reduce(Text key, Iterable<LongWritable> values,
          Context context) throws IOException, InterruptedException {
        Text outKey = new Text(key);
        long outputValue = 0;
        int count = 0;
        for (LongWritable value : values) {
          outputValue |= (value.get() << (count++ * 8));
        }

        context.write(outKey, new LongWritable(outputValue));
      }
    });

    driver
        .withKeyGroupingComparator(new org.apache.hadoop.mrunit.TestMapReduceDriver.FirstCharComparator());
    driver
        .withKeyOrderComparator(new org.apache.hadoop.mrunit.TestMapReduceDriver.SecondCharComparator());

    driver.addInput(mapper, new Text("a1"), new LongWritable(1));
    driver.addInput(mapper, new Text("b1"), new LongWritable(1));
    driver.addInput(mapper, new Text("a3"), new LongWritable(3));
    driver.addInput(mapper, new Text("a2"), new LongWritable(2));

    driver.addInput(tokenMapper, new LongWritable(1), new Text("c1 d1"));

    driver.addOutput(new Text("a1"), new LongWritable(0x1));
    driver.addOutput(new Text("b1"), new LongWritable(0x1));
    driver.addOutput(new Text("a2"), new LongWritable(0x2 | (0x3 << 8)));
    driver.addOutput(new Text("c1"), new LongWritable(0x1));
    driver.addOutput(new Text("d1"), new LongWritable(0x1));

    driver.runTest(false);
  }

  @Test
  public void testNoMapper() throws IOException {
    MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable> testDriver = new MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable>();
    testDriver.withReducer(reducer);
    thrown.expectMessage(IllegalStateException.class,
        "No mappers were provided");
    testDriver.runTest(false);
  }

  @Test
  public void testWithCounter() throws IOException {
    MultipleInputsMapReduceDriver<Text, Text, Text, Text> testDriver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();
    Mapper<Text, Text, Text, Text> mapperWithCounters = new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>();
    Mapper<Text, Text, Text, Text> tokenMapperWithCounters = new TokenMapperWithCounters();
    testDriver
        .withMapper(mapperWithCounters)
        .withInput(mapperWithCounters, new Text("hie"), new Text("Hi"))
        .withMapper(tokenMapperWithCounters)
        .withInput(tokenMapperWithCounters, new Text("bie"),
            new Text("Goodbye Bye"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 1)
        .withCounter(TokenMapperWithCounters.Counters.Y, 2)
        .withCounter("category", "name", 3)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 3)
        .withCounter("category", "count", 2).withCounter("category", "sum", 3)
        .runTest(false);
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    MultipleInputsMapReduceDriver<Text, Text, Text, Text> testDriver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();

    thrown
        .expectAssertionErrorMessage("2 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.mapreduce.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
            + " was not found in expected counters, Actual counter ("
            + "\"org.apache.hadoop.mrunit.mapreduce.TestMultipleInputsMapReduceDriver$TokenMapperWithCounters$Counters\",\"Y\")"
            + " was not found in expected counters");

    Mapper<Text, Text, Text, Text> mapperWithCounters = new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>();
    Mapper<Text, Text, Text, Text> tokenMapperWithCounters = new TokenMapperWithCounters();

    testDriver
        .withMapper(mapperWithCounters)
        .withInput(mapperWithCounters, new Text("hie"), new Text("Hi"))
        .withMapper(tokenMapperWithCounters)
        .withInput(tokenMapperWithCounters, new Text("bie"),
            new Text("Goodbye Bye"))
        .withStrictCounterChecking()
        .withCounter("category", "name", 3)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 3)
        .withCounter("category", "count", 2).withCounter("category", "sum", 3)
        .runTest(false);
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    MultipleInputsMapReduceDriver<Text, Text, Text, Text> testDriver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();

    thrown.expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"name\")" + " was not found in expected counters");

    Mapper<Text, Text, Text, Text> mapperWithCounters = new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>();
    Mapper<Text, Text, Text, Text> tokenMapperWithCounters = new TokenMapperWithCounters();

    testDriver
        .withMapper(mapperWithCounters)
        .withInput(mapperWithCounters, new Text("hie"), new Text("Hi"))
        .withMapper(tokenMapperWithCounters)
        .withInput(tokenMapperWithCounters, new Text("bie"),
            new Text("Goodbye Bye"))
        .withStrictCounterChecking()
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 1)
        .withCounter(TokenMapperWithCounters.Counters.Y, 2)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(TestReduceDriver.ReducerWithCounters.Counters.SUM, 3)
        .withCounter("category", "count", 2).withCounter("category", "sum", 3)
        .runTest(false);
  }

  @Test
  public void testWithFailedCounter() throws IOException {
    MultipleInputsMapReduceDriver<Text, Text, Text, Text> testDriver = new MultipleInputsMapReduceDriver<Text, Text, Text, Text>();

    thrown
        .expectAssertionErrorMessage("3 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.mapreduce.TestMapDriver.MapperWithCounters.Counters.X has value 1 instead of expected 20, "
            + "Counter org.apache.hadoop.mrunit.mapreduce.TestMultipleInputsMapReduceDriver.TokenMapperWithCounters.Counters.Y has value 2 instead of expected 30, "
            + "Counter with category category and name name has value 3 instead of expected 20)");

    Mapper<Text, Text, Text, Text> mapperWithCounters = new TestMapDriver.MapperWithCounters<Text, Text, Text, Text>();
    Mapper<Text, Text, Text, Text> tokenMapperWithCounters = new TokenMapperWithCounters();

    testDriver
        .withMapper(mapperWithCounters)
        .withInput(mapperWithCounters, new Text("hie"), new Text("Hi"))
        .withMapper(tokenMapperWithCounters)
        .withInput(tokenMapperWithCounters, new Text("bie"),
            new Text("Goodbye Bye"))
        .withCounter(TestMapDriver.MapperWithCounters.Counters.X, 20)
        .withCounter(TokenMapperWithCounters.Counters.Y, 30)
        .withReducer(
            new TestReduceDriver.ReducerWithCounters<Text, Text, Text, Text>())
        .withCounter("category", "name", 20).runTest(false);
  }

  @Test
  public void testJavaSerialization() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final MultipleInputsMapReduceDriver<Integer, IntWritable, Integer, IntWritable> testDriver = MultipleInputsMapReduceDriver
        .newMultipleInputMapReduceDriver(
                new Reducer<Integer, IntWritable, Integer, IntWritable>())
        .withConfiguration(conf);
    Mapper<Integer, IntWritable, Integer, IntWritable> identityMapper = new Mapper<Integer, IntWritable, Integer, IntWritable>();
    Mapper<Integer, IntWritable, Integer, IntWritable> anotherIdentityMapper = new Mapper<Integer, IntWritable, Integer, IntWritable>();
    testDriver.addMapper(identityMapper);
    testDriver.withInput(identityMapper, 1, new IntWritable(2)).withInput(
        identityMapper, 2, new IntWritable(3));
    testDriver.addMapper(anotherIdentityMapper);
    testDriver.withInput(anotherIdentityMapper, 3, new IntWritable(4))
        .withInput(anotherIdentityMapper, 4, new IntWritable(5));
    testDriver
        .withKeyOrderComparator(new JavaSerializationComparator<Integer>());
    testDriver
        .withKeyGroupingComparator(org.apache.hadoop.mrunit.TestMapReduceDriver.INTEGER_COMPARATOR);

    testDriver.withOutput(1, new IntWritable(2))
        .withOutput(2, new IntWritable(3)).withOutput(3, new IntWritable(4))
        .withOutput(4, new IntWritable(5));
    testDriver.runTest(false);
  }

  @Test
  public void testCopy() throws IOException {
    final Text key = new Text("a");
    final LongWritable value = new LongWritable(1);
    driver.addInput(mapper, key, value);
    key.set("b");
    value.set(2);
    driver.addInput(mapper, key, value);

    key.set("a");
    value.set(1);
    driver.addOutput(key, value);
    key.set("b");
    value.set(2);
    driver.addOutput(key, value);

    final LongWritable longKey = new LongWritable(3);
    final Text textValue = new Text("c d");
    driver.addInput(tokenMapper, longKey, textValue);
    longKey.set(4);
    textValue.set("e f g");
    driver.addInput(tokenMapper, longKey, textValue);

    key.set("c");
    value.set(3);
    driver.addOutput(key, value);
    key.set("d");
    value.set(3);
    driver.addOutput(key, value);
    key.set("e");
    value.set(4);
    driver.addOutput(key, value);
    key.set("f");
    value.set(4);
    driver.addOutput(key, value);
    key.set("g");
    value.set(4);
    driver.addOutput(key, value);

    driver.runTest(false);
  }

  @Test
  public void testOutputFormat() throws IOException {
    driver.withInputFormat(SequenceFileInputFormat.class);
    driver.withOutputFormat(SequenceFileOutputFormat.class);
    driver.withInput(mapper, new Text("a"), new LongWritable(1));
    driver.withInput(mapper, new Text("a"), new LongWritable(2));
    driver.withInput(tokenMapper, new LongWritable(3), new Text("a b"));
    driver.withOutput(new Text("a"), new LongWritable(6));
    driver.withOutput(new Text("b"), new LongWritable(3));
    driver.runTest(false);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testOutputFormatWithMismatchInOutputClasses() throws IOException {
    final MultipleInputsMapReduceDriver testDriver = this.driver;
    testDriver.withInputFormat(TextInputFormat.class);
    testDriver.withOutputFormat(TextOutputFormat.class);
    testDriver.withInput(mapper, new Text("a"), new LongWritable(1));
    testDriver.withInput(mapper, new Text("a"), new LongWritable(2));
    testDriver.withInput(tokenMapper, new LongWritable(3), new Text("a b"));
    testDriver.withOutput(new LongWritable(0), new Text("a\t6"));
    testDriver.withOutput(new LongWritable(4), new Text("b\t3"));
    testDriver.runTest(false);
  }

  @Test
  public void testMapInputFile() throws IOException {
    MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable> testDriver = new MultipleInputsMapReduceDriver<Text, LongWritable, Text, LongWritable>(
        reducer);

    InputPathStoringMapper<LongWritable, LongWritable> inputPathStoringMapper = new InputPathStoringMapper<LongWritable, LongWritable>();
    Path mapInputPath = new Path("myfile");
    testDriver.addMapper(inputPathStoringMapper);
    testDriver.setMapInputPath(inputPathStoringMapper, mapInputPath);
    assertEquals(mapInputPath.getName(),
        testDriver.getMapInputPath(inputPathStoringMapper).getName());
    testDriver.withInput(inputPathStoringMapper, new Text("a"),
        new LongWritable(1));

    InputPathStoringMapper<LongWritable, LongWritable> anotherInputPathStoringMapper = new InputPathStoringMapper<LongWritable, LongWritable>();
    Path anotherMapInputPath = new Path("myotherfile");
    testDriver.addMapper(anotherInputPathStoringMapper);
    testDriver.setMapInputPath(anotherInputPathStoringMapper,
        anotherMapInputPath);
    assertEquals(anotherMapInputPath.getName(),
        testDriver.getMapInputPath(anotherInputPathStoringMapper).getName());
    testDriver.withInput(anotherInputPathStoringMapper, new Text("b"),
        new LongWritable(2));

    testDriver.runTest(false);
    assertNotNull(inputPathStoringMapper.getMapInputPath());
    assertEquals(mapInputPath.getName(), inputPathStoringMapper
        .getMapInputPath().getName());
  }

  @Test
  public void testGroupComparatorBehaviorFirst() throws IOException {
    driver
        .withInput(mapper, new Text("A1"), new LongWritable(1L))
        .withInput(mapper, new Text("A2"), new LongWritable(1L))
        .withInput(mapper, new Text("B1"), new LongWritable(1L))
        .withInput(mapper, new Text("B2"), new LongWritable(1L))
        .withInput(mapper, new Text("C1"), new LongWritable(1L))
        .withInput(tokenMapper, new LongWritable(3L), new Text("D1 D2 E1"))
        .withOutput(new Text("A2"), new LongWritable(2L))
        .withOutput(new Text("B2"), new LongWritable(2L))
        .withOutput(new Text("C1"), new LongWritable(1L))
        .withOutput(new Text("D2"), new LongWritable(6L))
        .withOutput(new Text("E1"), new LongWritable(3L))
        .withKeyGroupingComparator(
            new org.apache.hadoop.mrunit.TestMapReduceDriver.FirstCharComparator())
        .runTest(false);
  }

  @Test
  public void testGroupComparatorBehaviorSecond() throws IOException {
    driver
        .withInput(mapper, new Text("1A"), new LongWritable(1L))
        .withInput(mapper, new Text("2A"), new LongWritable(1L))
        .withInput(mapper, new Text("1B"), new LongWritable(1L))
        .withInput(mapper, new Text("2B"), new LongWritable(1L))
        .withInput(mapper, new Text("1C"), new LongWritable(1L))
        .withInput(tokenMapper, new LongWritable(2L), new Text("1D 2D 1E"))
        .withOutput(new Text("1A"), new LongWritable(1L))
        .withOutput(new Text("2A"), new LongWritable(1L))
        .withOutput(new Text("1B"), new LongWritable(1L))
        .withOutput(new Text("2B"), new LongWritable(1L))
        .withOutput(new Text("1C"), new LongWritable(1L))
        .withOutput(new Text("1D"), new LongWritable(2L))
        .withOutput(new Text("2D"), new LongWritable(2L))
        .withOutput(new Text("1E"), new LongWritable(2L))
        .withKeyGroupingComparator(
            new org.apache.hadoop.mrunit.TestMapReduceDriver.SecondCharComparator())
        .runTest(false);
  }

  @Test
  public void testGroupingComparatorSpecifiedByConf() throws IOException {
    JobConf conf = new JobConf(new Configuration());
    conf.setOutputValueGroupingComparator(TestMapReduceDriver.FirstCharComparator.class);
    driver.withInput(mapper, new Text("A1"), new LongWritable(1L))
        .withInput(mapper, new Text("A2"), new LongWritable(1L))
        .withInput(mapper, new Text("B1"), new LongWritable(1L))
        .withInput(mapper, new Text("B2"), new LongWritable(1L))
        .withInput(mapper, new Text("C1"), new LongWritable(1L))
        .withInput(tokenMapper, new LongWritable(3L), new Text("D1 D2 E1"))
        .withOutput(new Text("A2"), new LongWritable(2L))
        .withOutput(new Text("B2"), new LongWritable(2L))
        .withOutput(new Text("C1"), new LongWritable(1L))
        .withOutput(new Text("D2"), new LongWritable(6L))
        .withOutput(new Text("E1"), new LongWritable(3L))
        .withConfiguration(conf).runTest(false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testUseOfWritableRegisteredComparator() throws IOException {
    MultipleInputsMapReduceDriver<TestWritable, Text, TestWritable, Text> testDriver = new MultipleInputsMapReduceDriver<TestWritable, Text, TestWritable, Text>(
        new Reducer<TestWritable, Text, TestWritable, Text>());

    Mapper<TestWritable, Text, TestWritable, Text> identityMapper = new Mapper<TestWritable, Text, TestWritable, Text>();
    Mapper<TestWritable, Text, TestWritable, Text> anotherIdentityMapper = new Mapper<TestWritable, Text, TestWritable, Text>();
    testDriver.addMapper(identityMapper);
    testDriver.addMapper(anotherIdentityMapper);
    testDriver
        .withInput(identityMapper, new TestWritable("A1"), new Text("A1"))
        .withInput(identityMapper, new TestWritable("A2"), new Text("A2"))
        .withInput(identityMapper, new TestWritable("A3"), new Text("A3"))
        .withInput(anotherIdentityMapper, new TestWritable("B1"),
            new Text("B1"))
        .withInput(anotherIdentityMapper, new TestWritable("B2"),
            new Text("B2"))
        .withKeyGroupingComparator(new TestWritable.SingleGroupComparator())
        .withOutput(new TestWritable("B2"), new Text("B2"))
        .withOutput(new TestWritable("B1"), new Text("B1"))
        .withOutput(new TestWritable("A3"), new Text("A3"))
        .withOutput(new TestWritable("A2"), new Text("A2"))
        .withOutput(new TestWritable("A1"), new Text("A1")).runTest(true); // ordering
                                                                           // is
                                                                           // important
  }

  static class TokenMapperWithCounters extends Mapper<Text, Text, Text, Text> {
    private final Text output = new Text();

    @Override
    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\s");
      for (String token : tokens) {
        output.set(token);
        context.write(key, output);
        context.getCounter(Counters.Y).increment(1);
        context.getCounter("category", "name").increment(1);
      }
    }

    public static enum Counters {
      Y
    }
  }

  static class TokenMapper extends
      Mapper<LongWritable, Text, Text, LongWritable> {
    private final Text output = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] tokens = value.toString().split("\\s");
      for (String token : tokens) {
        output.set(token);
        context.write(output, key);
      }
    }
  }

  static class ReverseMapper<KEYIN, VALUEIN> extends
      Mapper<KEYIN, VALUEIN, VALUEIN, KEYIN> {
    @Override
    protected void map(KEYIN key, VALUEIN value, Context context)
        throws IOException, InterruptedException {
      context.write(value, key);
    }
  }
}
