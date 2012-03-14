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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class TestTestDriver {

  /**
   * Test method for
   * {@link org.apache.hadoop.mrunit.TestDriver#parseTabbedPair(java.lang.String)}
   * .
   */
  @Test
  public void testParseTabbedPair1() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair2() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("   foo\tbar");
    assertEquals(pr.getFirst().toString(), "   foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair3() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar   ");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar   ");
  }

  @Test
  public void testParseTabbedPair4() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo    \tbar");
    assertEquals(pr.getFirst().toString(), "foo    ");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair5() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t  bar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar");
  }

  @Test
  public void testParseTabbedPair6() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "\tbar");
  }

  @Test
  public void testParseTabbedPair7() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\tbar\n");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar\n");
  }

  @Test
  public void testParseTabbedPair8() {
    final Pair<Text, Text> pr = TestDriver.parseTabbedPair("foo\t  bar\tbaz");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar\tbaz");
  }

  /**
   * Test method for
   * {@link org.apache.hadoop.mrunit.TestDriver#parseCommaDelimitedList(java.lang.String)}
   * .
   */
  @Test
  public void testParseCommaDelimList1() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList2() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList3() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo   ,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList4() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,   bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList5() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("   foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList6() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar   ");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList7() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar, baz");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    verify.add(new Text("baz"));
    assertListEquals(out, verify);
  }

  // note: we decide that correct behavior is that this does *not*
  // add a tailing empty element by itself.
  @Test
  public void testParseCommaDelimList8() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar,");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  // but this one does.
  @Test
  public void testParseCommaDelimList8a() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,bar,,");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    verify.add(new Text(""));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList9() {
    final List<Text> out = TestDriver.parseCommaDelimitedList("foo,,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text(""));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList10() {
    final List<Text> out = TestDriver.parseCommaDelimitedList(",foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text(""));
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

}
