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

import static org.apache.hadoop.mrunit.ExtendedAssert.assertListEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceFeeder;
import org.apache.hadoop.mrunit.types.KeyValueReuseList;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestReduceFeeder {
  ReduceFeeder<Text, Text> feeder;
  ReduceDriver<Text, Text, Text, Text> driver;

  Comparator<Text> firstCharComparator = new Comparator<Text>(){
    @Override
    public int compare(Text arg0, Text arg1) {
      return new Integer(arg0.charAt(0)).compareTo(new Integer(arg1.charAt(0)));
    }
  };

  Comparator<Text> secondCharComparator = new Comparator<Text>(){
    @Override
    public int compare(Text arg0, Text arg1) {
      return new Integer(arg0.charAt(1)).compareTo(new Integer(arg1.charAt(1)));
    }
  };

  @Before
  public void setUp() {
	  driver = ReduceDriver.newReduceDriver();
	  feeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());
  }

  @Test
  public void testEmptySortAndGroup() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs);
    assertEquals(0, outputs.size());
  }

  // just sort and group a single (k, v) pair
  @Test
  public void testSingleSortAndGroup() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    expected.add(sublist);

    assertListEquals(expected, outputs);
  }

  // sort and group multiple values from the same key.
  @Test
  public void testSortAndGroupOneKey() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("c")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist.add(new Pair<Text, Text>(new Text("a"), new Text("b")));
    sublist.add(new Pair<Text, Text>(new Text("a"), new Text("c")));
    expected.add(sublist);

    assertListEquals(expected, outputs);
  }

  // sort and group multiple keys
  @Test
  public void testMultiSortAndGroup1() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("v")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist1 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist1.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    sublist1.add(new Pair<Text, Text>(new Text("a"), new Text("y")));
    expected.add(sublist1);

    final KeyValueReuseList<Text, Text> sublist2 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist2.add(new Pair<Text, Text>(new Text("b"), new Text("v")));
    sublist2.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    expected.add(sublist2);

    assertListEquals(expected, outputs);
  }

  // sort and group multiple keys that are out-of-order to start.
  @Test
  public void testMultiSortAndGroup2() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("v")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    inputs.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    inputs.add(new Pair<Text, Text>(new Text("a"), new Text("y")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist1 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist1.add(new Pair<Text, Text>(new Text("a"), new Text("x")));
    sublist1.add(new Pair<Text, Text>(new Text("a"), new Text("y")));
    expected.add(sublist1);

    final KeyValueReuseList<Text, Text> sublist2 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist2.add(new Pair<Text, Text>(new Text("b"), new Text("v")));
    sublist2.add(new Pair<Text, Text>(new Text("b"), new Text("w")));
    expected.add(sublist2);

    assertListEquals(expected, outputs);
  }

  @Test
  public void testSortAndGroupWithOneComparator() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a1"), new Text("v1")));
    inputs.add(new Pair<Text, Text>(new Text("a2"), new Text("v2")));
    inputs.add(new Pair<Text, Text>(new Text("c1"), new Text("v3")));
    inputs.add(new Pair<Text, Text>(new Text("a2"), new Text("v4")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs, secondCharComparator);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist1 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist1.add(new Pair<Text, Text>(new Text("a1"), new Text("v1")));
    sublist1.add(new Pair<Text, Text>(new Text("c1"), new Text("v3")));
    expected.add(sublist1);

    final KeyValueReuseList<Text, Text> sublist2 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist2.add(new Pair<Text, Text>(new Text("a2"), new Text("v2")));
    sublist2.add(new Pair<Text, Text>(new Text("a2"), new Text("v4")));
    expected.add(sublist2);

    assertListEquals(expected, outputs);
  }

  @Test
  public void testSortAndGroupWithTwoComparators() {
    final List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("a1"), new Text("v1")));
    inputs.add(new Pair<Text, Text>(new Text("a2"), new Text("v5")));
    inputs.add(new Pair<Text, Text>(new Text("c1"), new Text("v3")));
    inputs.add(new Pair<Text, Text>(new Text("a2"), new Text("v2")));
    inputs.add(new Pair<Text, Text>(new Text("a3"), new Text("v4")));

    final List<KeyValueReuseList<Text, Text>> outputs = feeder.sortAndGroup(inputs, secondCharComparator, firstCharComparator);

    final List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    final KeyValueReuseList<Text, Text> sublist1 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist1.add(new Pair<Text, Text>(new Text("a1"), new Text("v1")));
    expected.add(sublist1);

    final KeyValueReuseList<Text, Text> sublist2 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist2.add(new Pair<Text, Text>(new Text("c1"), new Text("v3")));
    expected.add(sublist2);

    final KeyValueReuseList<Text, Text> sublist3 = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    sublist3.add(new Pair<Text, Text>(new Text("a2"), new Text("v5")));
    sublist3.add(new Pair<Text, Text>(new Text("a2"), new Text("v2")));
    sublist3.add(new Pair<Text, Text>(new Text("a3"), new Text("v4")));
    expected.add(sublist3);

    assertListEquals(expected, outputs);
  }

  @Test
  public void testUpdateInput() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("v1"));
    values.add(new Text("v2"));
    values.add(new Text("v3"));
    Text key = new Text("k");
    Pair<Text, List<Text>> oldInput = new Pair(key, values);
    KeyValueReuseList<Text, Text> updated = feeder.updateInput(oldInput);

    KeyValueReuseList<Text, Text> expected = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    for(Text value : values){
      expected.add(new Pair(key, value));
    }

    assertListEquals(expected, updated);
  }

  @Test
  public void testUpdateAll() {
    List<Text> values = new ArrayList<Text>();
    values.add(new Text("v1"));
    values.add(new Text("v2"));
    values.add(new Text("v3"));
    Text key = new Text("k");
    List<Pair<Text, List<Text>>> oldInputs = new ArrayList<Pair<Text, List<Text>>>();
    oldInputs.add(new Pair(key, values));
    List<KeyValueReuseList<Text, Text>> updated = feeder.updateAll(oldInputs);

    KeyValueReuseList<Text, Text> kvList = new KeyValueReuseList<Text, Text>(new Text(), new Text(), driver.getConfiguration());
    for(Text value : values){
      kvList.add(new Pair(key, value));
    }
    List<KeyValueReuseList<Text, Text>> expected = new ArrayList<KeyValueReuseList<Text, Text>>();
    expected.add(kvList);

    assertListEquals(expected, updated);
  }
}
