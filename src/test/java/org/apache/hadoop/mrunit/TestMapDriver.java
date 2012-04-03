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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestMapDriver {

  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();
  private Mapper<Text, Text, Text, Text> mapper;
  private MapDriver<Text, Text, Text, Text> driver;

  @Before
  public void setUp() {
    mapper = new IdentityMapper<Text, Text>();
    driver = MapDriver.newMapDriver(mapper);
  }

  @Test
  public void testRun() throws IOException {
    final List<Pair<Text, Text>> out = driver.withInput(new Text("foo"),
        new Text("bar")).run();

    final List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
    expected.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() {
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testTestRun2() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Expected no outputs; got 1 outputs., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testTestRun3() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, bar) at position 1.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun3OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun4() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bonusfoo, bar) at position 1.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("bonusfoo"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun4OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bonusfoo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("bonusfoo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun5() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse) at position 0., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse")).runTest(true);
  }

  @Test
  public void testTestRun5OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse), "
            + "Received unexpected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse")).runTest(false);
  }

  @Test
  public void testTestRun6() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar) at position 0., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar), "
        		+ "Received unexpected output (foo, bar)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun7() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, bar) but at "
            + "incorrect position 0 (expected position 1), "
            + "Missing expected output (someotherkey, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun7OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (someotherkey, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }
  
  @Test
  public void testTestRun8OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Missing expected output (foo, bar), "
           + "Missing expected output (foo, bar), "
           + "Missing expected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }
  
  @Test
  public void testUnexpectedOutput() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar) at position 1.)");
    driver.withMapper(new DuplicatingMapper()).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(true);
  }
  
  @Test
  public void testUnexpectedOutputMultiple() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Received unexpected output (foo, bar) at position 1., "
            + "Received unexpected output (foo, bar) at position 2., "
            + "Received unexpected output (foo, bar) at position 3.)");
    driver.withMapper(new DuplicatingMapper(4)).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(true);
  }
  
  @Test
  public void testUnexpectedOutputOrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar))");
    driver.withMapper(new DuplicatingMapper()).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(false);
  }
  
  @Test
  public void testUnexpectedOutputMultipleOrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Received unexpected output (foo, bar), "
            + "Received unexpected output (foo, bar), "
            + "Received unexpected output (foo, bar))");
    driver.withMapper(new DuplicatingMapper(4)).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(false);
  }

  @Test
  public void testSetInput() {
    driver.setInput(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    assertEquals(driver.getInputKey(), new Text("foo"));
    assertEquals(driver.getInputValue(), new Text("bar"));
  }

  @Test
  public void testSetInputNull() {
    thrown.expect(NullPointerException.class);
    driver.setInput((Pair<Text, Text>) null);
  }

  @Test
  public void testNoInput() {
    driver = MapDriver.newMapDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  @Test
  public void testNoMapper() {
    driver = MapDriver.newMapDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Mapper class was provided");
    driver.withInput(new Text("foo"), new Text("bar")).runTest();
  }

  private static class NonTextWritableInput extends MapReduceBase implements
      Mapper<LongWritable, LongWritable, Text, Text> {

    @Override
    public void map(final LongWritable key, final LongWritable value,
        final OutputCollector<Text, Text> output, final Reporter reporter)
        throws IOException {
      output.collect(new Text("a"), new Text("b"));
    }

  }

  @Test
  public void testWithCounter() {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1).runTest();
  }

  @Test
  public void testWithFailedCounter() {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    thrown.expectAssertionErrorMessage("2 Error(s): (" +
      "Counter org.apache.hadoop.mrunit.TestMapDriver.MapperWithCounters.Counters.X have value 1 instead of expected 4, " +
      "Counter with category category and name name have value 1 instead of expected 4)");

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 4)
        .withCounter("category", "name", 4).runTest();
  }

  /**
   * Simple mapper that have custom counter that is increased each map() call
   */
  public static class MapperWithCounters<KI, VI, KO, VO> implements Mapper<KI, VI, KO, VO> {
    @Override
    public void map(KI ki, VI vi, OutputCollector<KO, VO> outputCollector, Reporter reporter) throws IOException {
      outputCollector.collect((KO) ki, (VO) vi);
      reporter.getCounter(Counters.X).increment(1);
      reporter.getCounter("category", "name").increment(1);
    }

    public static enum Counters {
      X
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

  @Test
  public void testNonTextWritableWithInputFromString() {
    final MapDriver<LongWritable, LongWritable, Text, Text> driver = MapDriver
        .newMapDriver(new NonTextWritableInput());
    driver.withInputFromString("a\tb");
    thrown
        .expectMessage(ClassCastException.class,
            "org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.LongWritable");
    driver.runTest();
  }

  private static class NonTextWritableOutputKey extends MapReduceBase implements
      Mapper<Text, Text, LongWritable, Text> {

    @Override
    public void map(final Text key, final Text value,
        final OutputCollector<LongWritable, Text> output,
        final Reporter reporter) throws IOException {
      output.collect(new LongWritable(1), new Text("a"));
    }

  }

  @Test
  public void testNonTextWritableKeyWithOutputFromString() {
    final MapDriver<Text, Text, LongWritable, Text> driver = MapDriver
        .newMapDriver(new NonTextWritableOutputKey());
    driver.withInputFromString("a\tb");
    driver.withOutputFromString("1\ta");
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (1, a): Mismatch in key class: "
            + "expected: class org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable, "
            + "Received unexpected output (1, a): "
            + "Mismatch in key class: expected: class org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable)");
    driver.runTest();
  }

  private static class NonTextWritableOutputValue extends MapReduceBase
      implements Mapper<Text, Text, Text, LongWritable> {

    @Override
    public void map(final Text key, final Text value,
        final OutputCollector<Text, LongWritable> output,
        final Reporter reporter) throws IOException {
      output.collect(new Text("a"), new LongWritable(1));
    }

  }
  
  /**
   * Similar to IdentityMapper, but outputs each key/value pair twice
   */
  private static class DuplicatingMapper extends MapReduceBase
    implements Mapper<Text, Text, Text, Text> {
    private int duplicationFactor = 2;
    public DuplicatingMapper() {
     
    }
    public DuplicatingMapper(int factor) {
      duplicationFactor = factor;
    }
    @Override
    public void map(Text key, Text value, OutputCollector<Text, Text> output,
        Reporter reporter) throws IOException {
      for (int i = 0; i < duplicationFactor; i++)
        output.collect(key,value);
    }
  }

  @Test
  public void testNonTextWritableValueWithOutputFromString() {
    final MapDriver<Text, Text, Text, LongWritable> driver = MapDriver
        .newMapDriver(new NonTextWritableOutputValue());
    driver.withInputFromString("a\tb");
    driver.withOutputFromString("a\t1");
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (a, 1): Mismatch in value class: "
            + "expected: class org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable, "
            + "Received unexpected output (a, 1): Mismatch in value class: expected: class "
            + "org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable)");
    driver.runTest();
  }

  @Test
  public void testJavaSerialization() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final MapDriver<Integer, IntWritable, Integer, IntWritable> driver = MapDriver
        .newMapDriver(new IdentityMapper<Integer, IntWritable>())
        .withConfiguration(conf);
    driver.setInput(1, new IntWritable(2));
    driver.addOutput(1, new IntWritable(2));
    driver.runTest();
  }
}
