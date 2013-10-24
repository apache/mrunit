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

import static org.apache.hadoop.mrunit.ExtendedAssert.assertListEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("deprecation")
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

  @Before
  public void setUp() throws Exception {
    reducer = new LongSumReducer<Text>();
    driver = ReduceDriver.newReduceDriver(reducer);
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
  public void testTestRun3ComparatorOK() throws IOException {
    Comparator<LongWritable> toleranceComparator = new Comparator<LongWritable>() {

      @Override
      public int compare(LongWritable o1, LongWritable o2) {
        if (Math.abs(o1.get() - o2.get()) < 5) {
            return 0;
        }
        return o1.compareTo(o2);
      }
    };
    driver.setValueComparator(toleranceComparator);
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(true);
  }

  @Test
  public void testTestRun3ComparatorFail() throws IOException {
    Comparator<LongWritable> toleranceComparator = new Comparator<LongWritable>() {

      @Override
      public int compare(LongWritable o1, LongWritable o2) {
        if (Math.abs(o1.get() - o2.get()) < 2) {
            return 0;
        }
        return o1.compareTo(o2);
      }
    };
    driver.setValueComparator(toleranceComparator);
    thrown.expectAssertionErrorMessage("2 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 12)");
    thrown.expectAssertionErrorMessage("Received unexpected output (foo, 10)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(true);
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
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 4) at position 0.");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 6) at position 1.");
    thrown.expectAssertionErrorMessage("Received unexpected output (foo, 10) at position 0.");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest(true);
  }

  @Test
  public void testTestRun5OrderInsensitive() throws IOException {
    thrown.expectAssertionErrorMessage("3 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 4)");
    thrown.expectAssertionErrorMessage("Missing expected output (foo, 6)");
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
            + "Matched expected output (foo, 10) but at "
            + "incorrect position 0 (expected position 1))");
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
  public void testDuplicateOutputOrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar))");
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver(new IdentityReducer<Text, Text>());
    driver.withInputKey(new Text("foo")).withInputValue(new Text("bar"))
        .withInputValue(new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
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
  public void testNoInput() throws IOException {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  /**
   * Reducer that counts its values twice; the second iteration according to
   * mapreduce semantics should be empty.
   */
  private static class DoubleIterReducer<K, V> extends MapReduceBase implements
      Reducer<K, V, K, LongWritable> {
    @Override
    public void reduce(final K key, final Iterator<V> values,
        final OutputCollector<K, LongWritable> out, final Reporter r)
        throws IOException {
      long count = 0;

      while (values.hasNext()) {
        count++;
        values.next();
      }

      // This time around, iteration should yield no values.
      while (values.hasNext()) {
        count++;
        values.next();
      }
      out.collect(key, new LongWritable(count));
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
  public void testNoReducer() throws IOException {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Reducer class was provided");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .runTest();
  }

  @Test
  public void testJavaSerialization() throws IOException {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final ReduceDriver<Integer, Integer, Integer, Integer> driver = ReduceDriver
        .newReduceDriver(new IdentityReducer<Integer, Integer>())
        .withConfiguration(conf);
    driver.withInputKey(1).withInputValue(2).withOutput(1, 2).runTest();
  }

  @Test
  public void testWithCounter() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), values)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b"))
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndNoneMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), values)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.TestReduceDriver$ReducerWithCounters$Counters\",\"COUNT\")"
            + " was not found in expected counters");

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), values)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 1).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    thrown.expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"count\")" + " was not found in expected counters");

    final LinkedList<Text> values = new LinkedList<Text>();
    values.add(new Text("a"));
    values.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), values)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 1)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "sum", 2).runTest();
  }

  @Test
  public void testWithCounterMultipleInput() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> valuesAlpha = new LinkedList<Text>();
    valuesAlpha.add(new Text("a"));
    final LinkedList<Text> valuesBeta = new LinkedList<Text>();
    valuesBeta.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), valuesAlpha)
        .withInput(new Text("hie"), valuesBeta)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b"))
        .withCounter(ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }
  
  @Test
  public void testWithCounterAndNoneMissingMultipleInput() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> valuesAlpha = new LinkedList<Text>();
    valuesAlpha.add(new Text("a"));
    final LinkedList<Text> valuesBeta = new LinkedList<Text>();
    valuesBeta.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), valuesAlpha)
        .withInput(new Text("hie"), valuesBeta)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissingMultipleInput() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.TestReduceDriver$ReducerWithCounters$Counters\",\"COUNT\")"
            + " was not found in expected counters");

    final LinkedList<Text> valuesAlpha = new LinkedList<Text>();
    valuesAlpha.add(new Text("a"));
    final LinkedList<Text> valuesBeta = new LinkedList<Text>();
    valuesBeta.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), valuesAlpha)
        .withInput(new Text("hie"), valuesBeta)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "count", 2).withCounter("category", "sum", 2)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissingMultipleInput() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    thrown.expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"count\")" + " was not found in expected counters");

    final LinkedList<Text> valuesAlpha = new LinkedList<Text>();
    valuesAlpha.add(new Text("a"));
    final LinkedList<Text> valuesBeta = new LinkedList<Text>();
    valuesBeta.add(new Text("b"));

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), valuesAlpha)
        .withInput(new Text("hie"), valuesBeta)
        .withOutput(new Text("hie"), new Text("a"))
        .withOutput(new Text("hie"), new Text("b")).withStrictCounterChecking()
        .withCounter(ReducerWithCounters.Counters.COUNT, 2)
        .withCounter(ReducerWithCounters.Counters.SUM, 2)
        .withCounter("category", "sum", 2).runTest();
  }
  
  @Test
  public void testWithFailedEnumCounter() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> inputValues = new LinkedList<Text>();
    inputValues.add(new Text("Hi"));

    thrown
        .expectAssertionErrorMessage("2 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.TestReduceDriver.ReducerWithCounters.Counters.SUM has value 1 instead of expected 4, "
            + "Counter with category category and name sum has value 1 instead of expected 4)");

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), inputValues)
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(ReducerWithCounters.Counters.SUM, 4)
        .withCounter("category", "sum", 4).runTest();
  }

  @Test
  public void testCopy() throws IOException {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver(new IdentityReducer<Text, Text>());
    final Text input = new Text("a");
    driver.withInputKey(input);
    input.set("b");
    driver.withInputValue(input);
    input.set("c");
    driver.withInputValue(input);

    final Text output = new Text("a");
    driver.withOutput(output, new Text("b"));
    output.set("c");
    driver.withOutput(new Text("a"), output);
    driver.runTest();
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
  public void testRepeatRun() throws IOException {
    final List<LongWritable> vals = new ArrayList<LongWritable>();
    vals.add(new LongWritable(IN_A));
    vals.add(new LongWritable(IN_B));

    final List<Pair<Text, List<LongWritable>>> inputs = new ArrayList<Pair<Text, List<LongWritable>>>();
    inputs.add(new Pair<Text, List<LongWritable>>(new Text("foo"), vals));

    final List<Pair<Text, LongWritable>> expected = new ArrayList<Pair<Text, LongWritable>>();
    expected.add(new Pair<Text, LongWritable>(new Text("foo"),
            new LongWritable(OUT_VAL)));

    driver.withAll(inputs).withAllOutput(expected).runTest();
    thrown.expectMessage(IllegalStateException.class, "Driver reuse not allowed");
    driver.withAll(inputs).withAllOutput(expected).runTest();
  }

  /**
   * Simple reducer that have custom counters that are increased each map() call
   */
  public static class ReducerWithCounters<KI, VI, KO, VO> implements
      Reducer<KI, VI, KO, VO> {
    public static enum Counters {
      COUNT, SUM
    }

    @Override
    public void reduce(final KI ki, final Iterator<VI> viIterator,
        final OutputCollector<KO, VO> outputCollector, final Reporter reporter)
        throws IOException {
      reporter.getCounter(Counters.COUNT).increment(1);
      reporter.getCounter("category", "count").increment(1);
      while (viIterator.hasNext()) {
        final VI vi = viIterator.next();
        reporter.getCounter(Counters.SUM).increment(1);
        reporter.getCounter("category", "sum").increment(1);
        outputCollector.collect((KO) ki, (VO) vi);
      }
    }



    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(final JobConf entries) {
    }
  }

}
