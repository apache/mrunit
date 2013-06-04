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

import static org.apache.hadoop.mrunit.ExtendedAssert.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestReduceDriver {

  private static final int IN_A = 4;
  private static final int IN_B = 6;
  private static final int OUT_VAL = 10;
  private static final int INCORRECT_OUT = 12;
  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();
  private Reducer<Text, LongWritable, Text, LongWritable> reducer;
  private ReduceDriver<Text, LongWritable, Text, LongWritable> driver;
  private ReduceFeeder<Text, LongWritable> reduceFeeder;

  @Before
  public void setUp() throws Exception {
    reducer = new LongSumReducer<Text>();
    driver = ReduceDriver.newReduceDriver(reducer);
    reduceFeeder = new ReduceFeeder<Text, LongWritable>(driver.getConfiguration());
  }

  @Test
  public void testRun() throws IOException {
    final List<Pair<Text, LongWritable>> out = driver
        .withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B)).run();

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(OUT_VAL)));

    assertListEquals(out, expected);

  }

  @Test
  public void testTestRun1() throws IOException {
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testTestRun2() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (bar, 10) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun2OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (bar, 10), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun3() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 12) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(true);
  }

  @Test
  public void testTestRun3OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 12), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(false);
  }

  @Test
  public void testTestRun4() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 4) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A)).runTest(true);
  }

  @Test
  public void testTestRun4OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 4), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A)).runTest(false);
  }

  @Test
  public void testTestRun5() throws IOException {
    thrown.expectAssertionErrorMessage("3 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 6) at position 1.");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 4) at position 0.");
    thrown.expectAssertionErrorMessage("Received unexpected output (foo, 10) at position 0.");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest(true);
  }

  @Test
  public void testTestRun5OrderInsensitive() throws IOException {
    thrown.expectAssertionErrorMessage("3 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 6)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 4)");
    thrown.expectAssertionErrorMessage("Received unexpected output (foo, 10)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest(false);
  }

  @Test
  public void testTestRun6() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun7() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (bar, 10) at position 0., "
            + "Matched expected output (foo, 10) but at incorrect position 0 (expected position 1))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun7OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun8() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun8OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testAddAll() throws IOException {
    final List<LongWritable> vals = new ArrayList<LongWritable>();
    vals.add(new LongWritable(IN_A));
    vals.add(new LongWritable(IN_B));

    final List<Pair<Text, List<LongWritable>>> inputs = new ArrayList<Pair<Text, List<LongWritable>>>();
    inputs.add(new Pair<Text, List<LongWritable>>(new Text("foo"), vals));

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(OUT_VAL)));

    driver.withAll(inputs).withAllOutput(expected).runTest();
  }

  @Test
  public void testAddAllElements() throws IOException {
    final List<Pair<Text, LongWritable>> input = new ArrayList<Pair<Text, LongWritable>>();
    input.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(IN_A)));
    input.add(new Pair<Text, LongWritable>(new Text("foo"), new LongWritable(IN_B)));

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
        new LongWritable(OUT_VAL)));

    driver.withAllElements(reduceFeeder.sortAndGroup(input)).withAllOutput(expected).runTest();
  }

  @Test
  public void testNoInput() throws IOException {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  /**
   * Reducer that counts its values twice; the second iteration according to
   * mapreduce semantics should be empty.
   */
  private static class DoubleIterReducer<K, V> extends
      Reducer<K, V, K, LongWritable> {
    @Override
    @SuppressWarnings("unused")
    public void reduce(final K key, final Iterable<V> values, final Context c)
        throws IOException, InterruptedException {
      long count = 0;

      for (final V val : values) {
        count++;
      }

      // This time around, iteration should yield no values.
      for (final V val : values) {
        count++;
      }
      c.write(key, new LongWritable(count));
    }
  }

  @Test
  public void testDoubleIteration() throws IOException {
    reducer = new DoubleIterReducer<Text, LongWritable>();
    driver = ReduceDriver.newReduceDriver(reducer);

    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withOutput(new Text("foo"), new LongWritable(4)).runTest();
  }

  @Test
  public void testConfiguration() throws IOException {
    final Configuration conf = new Configuration();
    conf.set("TestKey", "TestValue");
    final ReduceDriver<NullWritable, NullWritable, NullWritable, NullWritable> confDriver = ReduceDriver
        .newReduceDriver();
    final ConfigurationReducer<NullWritable, NullWritable, NullWritable, NullWritable> reducer = new ConfigurationReducer<NullWritable, NullWritable, NullWritable, NullWritable>();
    confDriver.withReducer(reducer).withConfiguration(conf)
        .withInput(NullWritable.get(), Arrays.asList(NullWritable.get()))
        .withOutput(NullWritable.get(), NullWritable.get()).runTest();
    assertEquals(reducer.setupConfiguration.get("TestKey"), "TestValue");
  }

  @Test
  public void testWithCounter() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();
    ReduceFeeder<Text, Text> reduceFeeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(reduceFeeder.updateInput(new Text("hie"), values))
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }
  
  @Test
  public void testWithCounterAndNoneMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();
    ReduceFeeder<Text, Text> reduceFeeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(reduceFeeder.updateInput(new Text("hie"), values))
        .withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();
    ReduceFeeder<Text, Text> reduceFeeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.mapreduce.TestReduceDriver$ReducerWithCounters$Counters\",\"COUNT\")"
            + " was not found in expected counters");

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(reduceFeeder.updateInput(new Text("hie"), values))
        .withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();
    ReduceFeeder<Text, Text> reduceFeeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());

    thrown.expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"count\")" + " was not found in expected counters");

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(reduceFeeder.updateInput(new Text("hie"), values))
        .withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "sum", 2).runTest();
  }

  @Test
  public void testWithFailedCounter() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    thrown
        .expectAssertionErrorMessage("2 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.mapreduce.TestReduceDriver.ReducerWithCounters.Counters.SUM has value 1 instead of expected 4, "
            + "Counter with category category and name sum has value 1 instead of expected 4)");

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInputKey(new Text("hie")).withInputValue(new Text(""))
        .withCounter(ReducerWithCounters.Counters.SUM, 4)
        .withCounter("category", "sum", 4).runTest();
  }

  /**
   * Test reducer which stores the configuration object it was passed during its
   * setup method
   */
  public static class ConfigurationReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
      extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public Configuration setupConfiguration;

    @Override
    protected void setup(final Context context) throws IOException,
        InterruptedException {
      setupConfiguration = context.getConfiguration();
    }
  }

  @Test
  public void testNoReducer() throws IOException {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Reducer class was provided");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .runTest();
  }

  /**
   * Simple reducer that have custom counters that are increased each map() call
   */
  public static class ReducerWithCounters<KI, VI, KO, VO> extends
      Reducer<KI, VI, KO, VO> {
    public static enum Counters {
      COUNT, SUM
    }

    @Override
    protected void reduce(final KI key, final Iterable<VI> values,
        final Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.COUNT).increment(1);
      context.getCounter("category", "count").increment(1);
      for (@SuppressWarnings("unused") final VI vi : values) {
        context.getCounter(Counters.SUM).increment(1);
        context.getCounter("category", "sum").increment(1);
      }
    }
  }

  @Test
  public void testJavaSerialization() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final ReduceDriver<Integer, IntWritable, Integer, IntWritable> driver = ReduceDriver
        .newReduceDriver(new IntSumReducer<Integer>()).withConfiguration(conf);
    driver.withInputKey(1).withInputValue(new IntWritable(2))
        .withOutput(1, new IntWritable(2)).runTest();
  }
  
  @Test
  public void testOutputFormat() throws IOException {
    driver.withOutputFormat(SequenceFileOutputFormat.class,
        SequenceFileInputFormat.class);
    driver.withInputKey(new Text("a"));
    driver.withInputValue(new LongWritable(1)).withInputValue(
        new LongWritable(2));
    driver.withOutput(new Text("a"), new LongWritable(3));
    driver.runTest();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testOutputFormatWithMismatchInOutputClasses() throws IOException {
    final ReduceDriver driver = ReduceDriver.newReduceDriver(reducer);
    driver.withOutputFormat(TextOutputFormat.class, TextInputFormat.class);
    driver.withInputKey(new Text("a"));
    driver.withInputValue(new LongWritable(1)).withInputValue(
        new LongWritable(2));
    driver.withOutput(new LongWritable(), new Text("a\t3"));
    driver.runTest();
  }
  
  @Test
  public void textMockContext() throws IOException, InterruptedException {
    thrown.expectMessage(RuntimeException.class, "Injected!");
    Reducer<Text, LongWritable, Text, LongWritable>.Context context = driver.getContext();
    doThrow(new RuntimeException("Injected!"))
      .when(context)
        .write(any(Text.class), any(LongWritable.class));
    driver.withInputKey(new Text("a"));
    driver.withInputValue(new LongWritable(1)).withInputValue(
        new LongWritable(2));
    driver.runTest();
  }

  static class TaskAttemptReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) 
        throws IOException,InterruptedException {
      context.write(new Text(context.getTaskAttemptID().toString()), NullWritable.get());
    }
  }

  @Test
  public void testWithTaskAttemptUse() throws IOException {
    final ReduceDriver<Text,NullWritable,Text,NullWritable> driver 
      = ReduceDriver.newReduceDriver(new TaskAttemptReducer());
    ReduceFeeder<Text, NullWritable> reduceFeeder = new ReduceFeeder<Text, NullWritable>(driver.getConfiguration());

    driver.withInput(reduceFeeder.updateInput(new Text("anything"), Arrays.asList(NullWritable.get()))).withOutput(
        new Text("attempt__0000_r_000000_0"), NullWritable.get()).runTest();
  }
}
