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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
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
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (foo, somethingelse) at position 0., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse")).runTest(true);
  }

  @Test
  public void testTestRun5Comparator() throws IOException {
    Comparator<Text> valueComparator = new Comparator<Text>() {

      @Override
      public int compare(Text o1, Text o2) {
        return 0;
      }
    };
    driver.setValueComparator(valueComparator);
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("foo"), new Text("somethingelse"))
        .runTest(true);
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
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar) at position 0., "
            + "Received unexpected output (foo, bar) at position 0.)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(true);
  }

  @Test
  public void testTestRun6Comparator() throws IOException {
    Comparator<Text> keyComparator = new Comparator<Text>() {

      @Override
      public int compare(Text o1, Text o2) {
        if (o2.toString().equals("foo") && o1.toString().equals("someotherkey")) {
            return 0;
        }
        return o1.compareTo(o2);
      }
    };
    driver.setKeyComparator(keyComparator);
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar"))
        .runTest(true);
  }

  @Test
  public void testTestRun6OrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Missing expected output (someotherkey, bar), "
        		+ "Received unexpected output (foo, bar)");
    driver.withInput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("someotherkey"), new Text("bar")).runTest(false);
  }

  @Test
  public void testTestRun7() throws IOException {
    thrown
        .expectAssertionErrorMessage("2 Error(s): (Matched expected output (foo, bar) but at "
            + "incorrect position 0 (expected position 1), "
            + "Missing expected output (someotherkey, bar) at position 0.)");
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
  public void testTestRun8OrderInsensitive() throws IOException {
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
  public void testUnexpectedOutput() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar) at position 1.)");
    driver.withMapper(new DuplicatingMapper()).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(true);
  }
  
  @Test
  public void testUnexpectedOutputMultiple() throws IOException {
    thrown
        .expectAssertionErrorMessage("3 Error(s): (Received unexpected output (foo, bar) at position 1., "
            + "Received unexpected output (foo, bar) at position 2., "
            + "Received unexpected output (foo, bar) at position 3.)");
    driver.withMapper(new DuplicatingMapper(4)).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(true);
  }
  
  @Test
  public void testUnexpectedOutputMultipleComparator() throws IOException {
    Comparator<Text> comparatorAlwaysEqual = new Comparator<Text>() {

      @Override
      public int compare(Text o1, Text o2) {
        return 0;
      }
    };
    driver.setKeyComparator(comparatorAlwaysEqual);
    driver.setValueComparator(comparatorAlwaysEqual);
    thrown.expectAssertionErrorMessage("3 Error(s)");
    thrown.expectAssertionErrorMessage(
        "Received unexpected output (foo, bar) at position 1.");
    thrown.expectAssertionErrorMessage(
        "Received unexpected output (foo, bar) at position 2.");
    thrown.expectAssertionErrorMessage(
        "Received unexpected output (foo, bar) at position 3.");
    driver.withMapper(new DuplicatingMapper(4))
        .withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(true);
  }

  @Test
  public void testUnexpectedOutputOrderInsensitive() throws IOException {
    thrown
        .expectAssertionErrorMessage("1 Error(s): (Received unexpected output (foo, bar))");
    driver.withMapper(new DuplicatingMapper()).withInput(new Text("foo"),new Text("bar"))
        .withOutput(new Text("foo"),new Text("bar"))
        .runTest(false);
  }
  
  @Test
  public void testUnexpectedOutputMultipleOrderInsensitive() throws IOException {
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
  public void testNoInput() throws IOException {
    driver = MapDriver.newMapDriver();
    thrown.expectMessage(IllegalStateException.class, "No input was provided");
    driver.runTest();
  }

  @Test
  public void testNoMapper() throws IOException {
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
  public void testWithCounter() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1).runTest();
  }

  @Test
  public void testWithCounterUsingRunMethodPerformingCounterChecking() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 1)
        .withCounter("category", "name", 1).run(true);
  }

  @Test
  public void testWithCounterUsingRunMethodExplicitIgnoreCounterChecking() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 999)
        .withCounter("INVALIDCOUNTER", "NOTSET", 999).run(false);
  }

  @Test
  public void testWithCounterUsingRunMethodImplicitIgnoreCounterChecking() throws IOException {
    MapDriver<Text, Text, Text, Text> driver = MapDriver.newMapDriver();

    driver.withMapper(new MapperWithCounters<Text, Text, Text, Text>())
        .withInput(new Text("hie"), new Text("Hi"))
        .withOutput(new Text("hie"), new Text("Hi"))
        .withCounter(MapperWithCounters.Counters.X, 999)
        .withCounter("INVALIDCOUNTER", "NOTSET", 999).run();
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
            + "\"org.apache.hadoop.mrunit.TestMapDriver$MapperWithCounters$Counters\",\"X\")"
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
      "Counter org.apache.hadoop.mrunit.TestMapDriver.MapperWithCounters.Counters.X has value 1 instead of expected 4, " +
      "Counter with category category and name name has value 1 instead of expected 4)");

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
  public void testNonTextWritableWithInputFromString() throws IOException {
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
  public void testNonTextWritableKeyWithOutputFromString() throws IOException {
    final MapDriver<Text, Text, LongWritable, Text> driver = MapDriver
        .newMapDriver(new NonTextWritableOutputKey());
    driver.withInputFromString("a\tb");
    driver.withOutputFromString("1\ta");
    thrown.expectAssertionErrorMessage("2 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected output (1, a): "
        + "Mismatch in key class: expected: class org.apache.hadoop.io.Text "
        + "actual: class org.apache.hadoop.io.LongWritable");
    thrown.expectAssertionErrorMessage("Received unexpected output (1, a): "
        + "Mismatch in key class: expected: class org.apache.hadoop.io.Text "
        + "actual: class org.apache.hadoop.io.LongWritable");
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
  public void testNonTextWritableValueWithOutputFromString() throws IOException {
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
  public void testJavaSerialization() throws IOException {
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

  @Test
  public void testCopy() throws IOException {
    driver = MapDriver.newMapDriver(new DuplicatingMapper());
    final Text input = new Text("a");
    driver.withInputKey(input);
    input.set("b");
    driver.withInputValue(input);
    input.set("c");

    final Text output = new Text("a");
    driver.withOutput(output, new Text("b"));
    output.set("b");
    driver.withOutput(new Text("a"), output);
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
        .newMapDriver(new IdentityMapper());
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
  public void testMultipleWithInput() throws IOException {
    driver.withInput(new Text("foo"), new Text("bar"))
        .withInput(new Text("bar"), new Text("baz"))
        .withOutput(new Text("foo"), new Text("bar"))
        .withOutput(new Text("bar"), new Text("baz")).runTest(false);
  }
}
