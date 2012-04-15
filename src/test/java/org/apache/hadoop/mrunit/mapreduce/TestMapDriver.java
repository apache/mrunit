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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestMapDriver {

  @Rule
  public final ExpectedSuppliedException thrown = ExpectedSuppliedException
      .none();
  private Mapper<Text, Text, Text, Text> mapper;
  private MapDriver<Text, Text, Text, Text> driver;

  @Before
  public void setUp() {
    mapper = new Mapper<Text, Text, Text, Text>(); // default action is identity
                                                   // mapper.
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
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse) at position 0.," 
            + " Received unexpected output (foo, bar) at position 0.)");
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
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar) at position 0.,"
            + " Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar), "
            + "Received unexpected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun7() {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, bar) but at "
            + "incorrect position 0 (expected position 1), Missing expected output (someotherkey, bar) at position 0.)");
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
      "Counter org.apache.hadoop.mrunit.mapreduce.TestMapDriver.MapperWithCounters.Counters.X have value 1 instead of expected 4, " +
      "Counter with category category and name name have value 1 instead of expected 4)");

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 4)
        .withCounter("category", "name", 4).runTest();
  }

  @Test
  public void testConfiguration() {
    final Configuration conf = new Configuration();
    conf.set("TestKey", "TestValue");
    final MapDriver<NullWritable, NullWritable, NullWritable, NullWritable> confDriver = MapDriver
        .newMapDriver();
    final ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable> mapper = new ConfigurationMapper<NullWritable, NullWritable, NullWritable, NullWritable>();
    confDriver.withMapper(mapper).withConfiguration(conf)
        .withInput(NullWritable.get(), NullWritable.get())
        .withOutput(NullWritable.get(), NullWritable.get()).runTest();
    assertEquals("TestValue", mapper.setupConfiguration.get("TestKey"));
  }

  /**
   * Test mapper which stores the configuration object it was passed during its
   * setup method
   */
  public static class ConfigurationMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
      extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public Configuration setupConfiguration;

    @Override
    protected void setup(final Context context) throws IOException,
        InterruptedException {
      setupConfiguration = context.getConfiguration();
    }
  }

  @Test
  public void testInputSplitDetails() {
    final MapDriver<NullWritable, NullWritable, Text, LongWritable> driver = 
        MapDriver.newMapDriver(new InputSplitDetailMapper());
    driver.withInput(NullWritable.get(), NullWritable.get())
      .withOutput(new Text("somefile"), new LongWritable(0L)).runTest();
  }
  
  public static class InputSplitDetailMapper
    extends Mapper<NullWritable, NullWritable, Text, LongWritable> {
    protected void map(NullWritable key, NullWritable value, Context context) 
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit)context.getInputSplit();
      context.write(new Text(split.getPath().toString()), 
          new LongWritable(split.getLength()));
    }
  }

  @Test
  public void testNoMapper() {
    driver = MapDriver.newMapDriver();
    thrown.expectMessage(IllegalStateException.class,
        "No Mapper class was provided");
    driver.withInput(new Text("foo"), new Text("bar")).runTest();
  }

  /**
   * Simple mapper that have custom counter that is increased each map() call
   */
  public static class MapperWithCounters<KI, VI, KO, VO> extends Mapper<KI, VI, KO, VO> {
    public static enum Counters {
      X
    }

    @Override
    protected void map(KI key, VI value, Context context) throws IOException, InterruptedException {
      context.getCounter(Counters.X).increment(1);
      context.getCounter("category", "name").increment(1);
      super.map(key, value, context);
    }
  }

  @Test
  public void testJavaSerialization() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations", conf.get("io.serializations"),
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final MapDriver<Integer, IntWritable, IntWritable, Integer> driver = MapDriver
        .newMapDriver(new InverseMapper<Integer, IntWritable>())
        .withConfiguration(conf);
    driver.setInput(1, new IntWritable(2));
    driver.addOutput(new IntWritable(2), 1);
    driver.runTest();
  }
}
