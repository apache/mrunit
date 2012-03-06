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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestMapDriver {

  private Mapper<Text, Text, Text, Text> mapper;
  private MapDriver<Text, Text, Text, Text> driver;

  @Before
  public void setUp() {
    mapper = new IdentityMapper<Text, Text>();
    driver = MapDriver.newMapDriver(mapper);
  }

  @Test
  public void testRun() {
    List<Pair<Text, Text>> out = null;

    try {
      out = driver.withInput(new Text("foo"), new Text("bar")).run();
    } catch (IOException ioe) {
      fail();
    }

    List<Pair<Text, Text>> expected = new ArrayList<Pair<Text, Text>>();
    expected.add(new Pair<Text, Text>(new Text("foo"), new Text("bar")));

    assertListEquals(out, expected);
  }

  @Test
  public void testTestRun1() {
    driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
  }

  @Test
  public void testTestRun2() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun3() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun4() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("bonusfoo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }

  }
  @Test
  public void testTestRun5() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("somethingelse"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun6() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
              .withOutput(new Text("someotherkey"), new Text("bar"))
              .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testTestRun7() {
    try {
      driver.withInput(new Text("foo"), new Text("bar"))
            .withOutput(new Text("someotherkey"), new Text("bar"))
            .withOutput(new Text("foo"), new Text("bar"))
            .runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testSetInput() {
    try {
      driver.setInput(new Pair<Text, Text>(new Text("foo"), new Text("bar")));
    } catch (Exception e) {
      fail();
    }

    assertEquals(driver.getInputKey(), new Text("foo"));
    assertEquals(driver.getInputValue(), new Text("bar"));
  }

  @Test
  public void testSetInputNull() {
    try {
      driver.setInput((Pair<Text, Text>) null);
      fail();
    } catch (Exception e) {
      // expect this.
    }
  }

  @Test
  public void testEmptyInput() {
    // MapDriver will forcibly map (null, null) as undefined input;
    // identity mapper expects (null, null) back.
    driver.withOutput(null, null).runTest();
  }

  @Test
  public void testEmptyInput2() {
    // it is an error to expect no output because we expect
    // the mapper to be fed (null, null) as an input if the
    // user doesn't set any input.
    try {
      driver.runTest();
      fail();
    } catch (RuntimeException re) {
      // expected.
    }
  }

  @Test
  public void testNoMapper() {
    driver = MapDriver.newMapDriver();
    try {
      driver.runTest();
      fail();
    } catch (IllegalStateException e) {
      assertEquals("No Mapper class was provided", e.getMessage());
    }
  }

  private static class NonTextWritableInput extends MapReduceBase implements Mapper<LongWritable, LongWritable, Text, Text> {

    @Override
    public void map(LongWritable key, LongWritable value,
        OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      output.collect(new Text("a"), new Text("b"));
    }

  }

  @Test
  public void testNonTextWritableWithInputFromString() {
    MapDriver<LongWritable, LongWritable, Text, Text> driver = MapDriver.newMapDriver(new NonTextWritableInput());
    driver.withInputFromString("a\tb");
    try {
      driver.runTest();
      fail();
    } catch (ClassCastException e) {
      assertEquals("org.apache.hadoop.io.Text cannot be cast to org.apache.hadoop.io.LongWritable", e.getMessage());
    }
  }

  private static class NonTextWritableOutputKey extends MapReduceBase implements Mapper<Text, Text, LongWritable, Text> {

    @Override
    public void map(Text key, Text value,
        OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
      output.collect(new LongWritable(1), new Text("a"));
    }

  }

  @Test
  public void testNonTextWritableKeyWithOutputFromString() {
    MapDriver<Text, Text, LongWritable, Text> driver = MapDriver.newMapDriver(new NonTextWritableOutputKey());
    driver.withInputFromString("a\tb");
    driver.withOutputFromString("1\ta");
    try {
      driver.runTest();
      fail();
    } catch (RuntimeException e) {
      assertEquals("1 Error(s): (Missing expected output (1, a): Mismatch in key class: " +
          "expected: class org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable)", e.getMessage());
    }
  }

  private static class NonTextWritableOutputValue extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

    @Override
    public void map(Text key, Text value,
        OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
      output.collect(new Text("a"), new LongWritable(1));
    }

  }

  @Test
  public void testNonTextWritableValueWithOutputFromString() {
    MapDriver<Text, Text, Text, LongWritable> driver = MapDriver.newMapDriver(new NonTextWritableOutputValue());
    driver.withInputFromString("a\tb");
    driver.withOutputFromString("a\t1");
    try {
      driver.runTest();
      fail();
    } catch (RuntimeException e) {
      assertEquals("1 Error(s): (Missing expected output (a, 1): Mismatch in value class: " +
          "expected: class org.apache.hadoop.io.Text actual: class org.apache.hadoop.io.LongWritable)", e.getMessage());
    }
  }
}

