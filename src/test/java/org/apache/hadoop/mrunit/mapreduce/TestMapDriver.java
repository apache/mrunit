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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
  public void testTestRun1() throws IOException {
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testTestRun2() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Expected no outputs; got 1 outputs., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar")).runTest();
  }

  @Test
  public void testTestRun3() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, bar) at position 1.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun3OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun4() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bonusfoo, bar) at position 1.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("bonusfoo"), new Text("bar")).runTest(true);

  }

  @Test
  public void testTestRun4OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (bonusfoo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("bonusfoo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun5() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse) at position 0.," 
            + " Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse")).runTest(true);
  }

  @Test
  public void testTestRun5OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse), "
            + "Received unexpected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse")).runTest(false);
  }

  @Test
  public void testTestRun6() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar) at position 0.,"
            + " Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar), "
            + "Received unexpected output (foo, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun7() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, bar) but at "
            + "incorrect position 0 (expected position 1), Missing expected output (someotherkey, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun7OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Missing expected output (someotherkey, bar))");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("bar")).runTest(false);
  }

  @Test
  public void testAddAll() throws IOException {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    inputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    final List<Pair<Text, Text>> outputs = new ArrayList<Pair<Text, Text>>();
    outputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    outputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    driver.withAll(inputs).withAllOutput(outputs).runTest();
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
  public void testNoInput() throws IOException {
    driver = MapDriver.newMapDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  @Test
  public void testWithCounter() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1).runTest();
  }
  
  @Test
  public void testWithCounterAndNoneMissing() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1).runTest();
  }

  @Test
  public void testWithCounterAndEnumCounterMissing() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();
    
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
            + "\"org.apache.hadoop.mrunit.mapreduce.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
            + " was not found in expected counters");

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking().withCounter("category", "name", 1)
        .runTest();
  }

  @Test
  public void testWithCounterAndStringCounterMissing() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();
    
    thrown
    .expectAssertionErrorMessage("1 Error(s): (Actual counter ("
        + "\"category\",\"name\")"
        + " was not found in expected counters");

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withStrictCounterChecking()
        .withCounter(MapperWithCounters.Counters.X, 1).runTest();
  }

  @Test
  public void testWithFailedCounter() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    thrown.expectAssertionErrorMessage("2 Error(s): (" +
      "Counter org.apache.hadoop.mrunit.mapreduce.TestMapDriver.MapperWithCounters.Counters.X has value 1 instead of expected 4, " +
      "Counter with category category and name name has value 1 instead of expected 4)");

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 4)
        .withCounter("category", "name", 4).runTest();
  }

  @Test
  public void testConfiguration() throws IOException {
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
  public void testInputSplitDetails() throws IOException {
    final MapDriver<NullWritable, NullWritable, Text, LongWritable> driver = 
        MapDriver.newMapDriver(new InputSplitDetailMapper());
    driver.withInput(NullWritable.get(), NullWritable.get())
      .withOutput(new Text("somefile"), new LongWritable(0L)).runTest();
  }
  
  public static class InputSplitDetailMapper
    extends Mapper<NullWritable, NullWritable, Text, LongWritable> {
    @Override
    protected void map(NullWritable key, NullWritable value, Context context) 
        throws IOException, InterruptedException {
      FileSplit split = (FileSplit)context.getInputSplit();
      context.write(new Text(split.getPath().toString()), 
          new LongWritable(split.getLength()));
    }
  }

  @Test
  public void testNoMapper() throws IOException {
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
  public void testJavaSerialization() throws IOException {
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

  @Test
  public void testOutputFormat() throws IOException {
    driver.withOutputFormat(SequenceFileOutputFormat.class,
        SequenceFileInputFormat.class);
    driver.withInput(new Text("a"), new Text("1"));
    driver.withOutput(new Text("a"), new Text("1"));
    driver.runTest();
  }

  @Test
  public void testOutputFormatWithMismatchInOutputClasses() throws IOException {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    final MapDriver<Text, Text, LongWritable, Text> driver = MapDriver
        .newMapDriver(new Mapper());
    driver.withOutputFormat(TextOutputFormat.class, TextInputFormat.class);
    driver.withInput(new Text("a"), new Text("1"));
    driver.withOutput(new LongWritable(), new Text("a\t1"));
    driver.runTest();
  }
  


  @Test
  public void testMapInputFile() throws IOException {
    InputPathStoringMapper<Text, Text> mapper = new InputPathStoringMapper<Text, Text>();
    Path mapInputPath = new Path("myfile");
    driver = MapDriver.newMapDriver(mapper);
    driver.setMapInputPath(mapInputPath);
    assertEquals(mapInputPath.getName(), driver.getMapInputPath().getName());
    driver.withInput(new Text("a"), new Text("1"));
    driver.runTest();
    assertNotNull(mapper.getMapInputPath());
    assertEquals(mapInputPath.getName(), mapper.getMapInputPath().getName());
  }
  
  @Test
  public void textMockContext() throws IOException, InterruptedException {
    thrown.expectMessage(RuntimeException.class, "Injected!");
    Mapper<Text, Text, Text, Text>.Context context = driver.getContext();
    doThrow(new RuntimeException("Injected!"))
      .when(context)
        .write(any(Text.class), any(Text.class));
    driver.withInput(new Text("a"), new Text("1"));
    driver.withOutput(new Text("a"), new Text("1"));
    driver.runTest();
  }
  
  static class TaskAttemptMapper extends Mapper<Text,NullWritable,Text,NullWritable> {
    @Override
    protected void map(Text key, NullWritable value, Context context) 
        throws IOException,InterruptedException {
      context.write(new Text(context.getTaskAttemptID().toString()), NullWritable.get());
    }
  }

  @Test
  public void testWithTaskAttemptUse() throws IOException {
    final MapDriver<Text,NullWritable,Text,NullWritable> driver 
      = MapDriver.newMapDriver(new TaskAttemptMapper());
    driver.withInput(new Text("anything"), NullWritable.get()).withOutput(
        new Text("attempt__0000_m_000000_0"), NullWritable.get()).runTest();
  }

  @Test
  public void testRepeatRun() throws IOException {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    inputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    final List<Pair<Text, Text>> outputs = new ArrayList<Pair<Text, Text>>();
    outputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    outputs.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    driver.withAll(inputs).withAllOutput(outputs).runTest();
    thrown.expectMessage(IllegalStateException.class, "Driver reuse not allowed");
    driver.withAll(inputs).withAllOutput(outputs).runTest();
  }
  
}
