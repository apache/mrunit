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

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;

import java.io.IOException;
import java.util.ArrayList;
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
  public void testTestRun1() {
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testTestRun2() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (bar, 10) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun2OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (bar, 10), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun3() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 12) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(true);
  }

  @Test
  public void testTestRun3OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 12), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT))
        .runTest(false);
  }

  @Test
  public void testTestRun4() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 4) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A)).runTest(true);
  }

  @Test
  public void testTestRun4OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, 4), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A)).runTest(false);
  }

  @Test
  public void testTestRun5() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Missing expected output (foo, 6) at position 1., "
            + "Missing expected output (foo, 4) at position 0., "
            + "Received unexpected output (foo, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest(true);
  }

  @Test
  public void testTestRun5OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Missing expected output (foo, 6), "
            + "Missing expected output (foo, 4), "
            + "Received unexpected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest(false);
  }

  @Test
  public void testTestRun6() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun7() {
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
  public void testTestRun7OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testTestRun8() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(true);
  }

  @Test
  public void testTestRun8OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10))");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest(false);
  }

  @Test
  public void testDuplicateOutputOrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar))");
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver(new IdentityReducer<Text, Text>());
    driver.withInputKey(new Text("foo")).withInputValue(new Text("bar"))
        .withInputValue(new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testNoInput() {
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
  public void testDoubleIteration() {
    reducer = new DoubleIterReducer<Text, LongWritable>();
    driver = ReduceDriver.newReduceDriver(reducer);

    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withInputValue(new LongWritable(1))
        .withOutput(new Text("foo"), new LongWritable(4)).runTest();
  }

  @Test
  public void testNoReducer() {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Reducer class was provided");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .runTest();
  }

  @Test
  public void testJavaSerialization() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final ReduceDriver<Integer, Integer, Integer, Integer> driver = ReduceDriver
        .newReduceDriver(new IdentityReducer<Integer, Integer>())
        .withConfiguration(conf);
    driver.withInputKey(1).withInputValue(2).withOutput(1, 2).runTest();
  }

  @Test
  public void testWithCounter() {
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
  public void testWithFailedEnumCounter() {
    final ReduceDriver<Text, Text, Text, Text> driver = ReduceDriver
        .newReduceDriver();

    final LinkedList<Text> inputValues = new LinkedList<Text>();
    inputValues.add(new Text("Hi"));

    thrown
        .expectAssertionErrorMessage("2 Error(s): ("
            + "Counter org.apache.hadoop.mrunit.TestReduceDriver.ReducerWithCounters.Counters.SUM have value 1 instead of expected 4, "
            + "Counter with category category and name sum have value 1 instead of expected 4)");

    driver.withReducer(new ReducerWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), inputValues)
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(ReducerWithCounters.Counters.SUM, 4)
        .withCounter("category", "sum", 4).runTest();
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
