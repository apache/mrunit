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

import static org.apache.hadoop.mrunit.testutil.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
  private static final int OUT_EMPTY = 0;

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
    driver.withInputKey(new Text("foo"))
        .withOutput(new Text("foo"), new LongWritable(0)).runTest();
  }

  @Test
  public void testTestRun2() {
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testTestRun3() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Received unexpected output (foo, 10), "
            + "Missing expected output (bar, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest();

  }

  @Test
  public void testTestRun4() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Received unexpected output (foo, 10), "
            + "Missing expected output (foo, 12) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(INCORRECT_OUT)).runTest();
  }

  @Test
  public void testTestRun5() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Received unexpected output (foo, 10), "
            + "Missing expected output (foo, 4) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A)).runTest();
  }

  @Test
  public void testTestRun6() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Received unexpected output (foo, 10), "
            + "Missing expected output (foo, 4) at position 0., "
            + "Missing expected output (foo, 6) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(IN_A))
        .withOutput(new Text("foo"), new LongWritable(IN_B)).runTest();
  }

  @Test
  public void testTestRun7() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testTestRun8() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, 10) but at incorrect position 0 (expected position 1), "
            + "Missing expected output (bar, 10) at position 0.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testTestRun9() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bar, 10) at position 1.)");
    driver.withInputKey(new Text("foo")).withInputValue(new LongWritable(IN_A))
        .withInputValue(new LongWritable(IN_B))
        .withOutput(new Text("foo"), new LongWritable(OUT_VAL))
        .withOutput(new Text("bar"), new LongWritable(OUT_VAL)).runTest();
  }

  @Test
  public void testEmptyInput() {
    // (null, <empty>) will be forcibly fed as input
    // since we use LongSumReducer, expect (null, 0) out.
    driver.withOutput(null, new LongWritable(OUT_EMPTY)).runTest();
  }

  @Test
  public void testEmptyInput2() {
    // because a null key with zero inputs will be fed as input
    // to this reducer, do not accept no outputs.
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Expected no outputs; got 1 outputs., Received unexpected output (null, 0))");
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
  public void testConfiguration() {
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
  public void testNoReducer() {
    driver = ReduceDriver.newReduceDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Reducer class was provided");
    driver.runTest();
  }

  @Test
  public void testConf() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final ReduceDriver<Integer, IntWritable, Integer, IntWritable> driver = ReduceDriver
        .newReduceDriver(new IntSumReducer<Integer>()).withConfiguration(conf);
    driver.withInputKey(1).withInputValue(new IntWritable(2))
        .withOutput(1, new IntWritable(2)).runTest();
  }
}
