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
package org.apache.hadoop.mrunit.internal.util;

import static org.apache.hadoop.mrunit.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class TestStringUtils {

  /**
   * Test method for {@link StringUtils#parseTabbedPair(java.lang.String)} .
   */
  @Test
  public void testParseTabbedPair1() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair2() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("   foo\tbar");
    assertEquals(pr.getFirst().toString(), "   foo");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair3() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\tbar   ");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar   ");
  }

  @Test
  public void testParseTabbedPair4() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo    \tbar");
    assertEquals(pr.getFirst().toString(), "foo    ");
    assertEquals(pr.getSecond().toString(), "bar");
  }

  @Test
  public void testParseTabbedPair5() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\t  bar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar");
  }

  @Test
  public void testParseTabbedPair6() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\t\tbar");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "\tbar");
  }

  @Test
  public void testParseTabbedPair7() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\tbar\n");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "bar\n");
  }

  @Test
  public void testParseTabbedPair8() {
    final Pair<Text, Text> pr = StringUtils.parseTabbedPair("foo\t  bar\tbaz");
    assertEquals(pr.getFirst().toString(), "foo");
    assertEquals(pr.getSecond().toString(), "  bar\tbaz");
  }

  /**
   * Test method for
   * {@link StringUtils#parseCommaDelimitedList(java.lang.String)}.
   */
  @Test
  public void testParseCommaDelimList1() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList2() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList3() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo   ,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList4() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,   bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList5() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("   foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList6() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,bar   ");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList7() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,bar, baz");
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
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,bar,");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  // but this one does.
  @Test
  public void testParseCommaDelimList8a() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,bar,,");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    verify.add(new Text(""));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList9() {
    final List<Text> out = StringUtils.parseCommaDelimitedList("foo,,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text("foo"));
    verify.add(new Text(""));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  @Test
  public void testParseCommaDelimList10() {
    final List<Text> out = StringUtils.parseCommaDelimitedList(",foo,bar");
    final ArrayList<Text> verify = new ArrayList<Text>();
    verify.add(new Text(""));
    verify.add(new Text("foo"));
    verify.add(new Text("bar"));
    assertListEquals(out, verify);
  }

  /**
   * Test method for {@link StringUtils#formatValueList(List, StringBuilder)}.
   */
  @Test
  public void shouldFormatValueListWhenEmpty() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    StringUtils.formatValueList(Arrays.asList(), stringBuilder);
    assertEquals("previous message ()", stringBuilder.toString());
  }

  @Test
  public void shouldFormatValueListWithSingleElement() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    StringUtils.formatValueList(Arrays.asList("single"), stringBuilder);
    assertEquals("previous message (single)", stringBuilder.toString());
  }
  
  @Test
  public void shouldFormatValueListWithManyElement() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    StringUtils.formatValueList(Arrays.asList("first", "second", "third"), stringBuilder);
    assertEquals("previous message (first, second, third)", stringBuilder.toString());
  }

  /**
   * Test method for {@link StringUtils#formatPairList(List, StringBuilder)}
   */
  @Test
  public void shouldFormatPairListWhenEmpty() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    List<Pair<String, String>> list = new ArrayList<Pair<String, String>>();
    StringUtils.formatPairList(list, stringBuilder);
    assertEquals("previous message []", stringBuilder.toString());
  }

  @Test
  public void shouldFormatPairListWithSingleElement() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    StringUtils.formatPairList(Arrays.asList(new Pair<String, String>("key", "value")), stringBuilder);
    assertEquals("previous message [(key, value)]", stringBuilder.toString());
  }

  @Test
  public void shouldFormatPairListWithManyElement() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("previous message ");
    StringUtils.formatPairList(Arrays.asList(
            new Pair<String, String>("first_key", "first_value"),
            new Pair<String, String>("second_key", "second_value"),
            new Pair<String, String>("third_key", "third_value")), stringBuilder);
    assertEquals("previous message [(first_key, first_value), (second_key, second_value), (third_key, third_value)]", stringBuilder.toString());
  }
}
