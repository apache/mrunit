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
package org.apache.hadoop.mrunit.types;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceFeeder;
import org.junit.Before;
import org.junit.Test;
import static org.apache.hadoop.mrunit.ExtendedAssert.*;

public class TestKeyValueReuseList {
  KeyValueReuseList<Text, Text> kvList;
  ReduceDriver<Text, Text, Text, Text> driver;

  @Before
  public void setUp() {
    driver = ReduceDriver.newReduceDriver();
    ReduceFeeder<Text, Text> feeder = new ReduceFeeder<Text, Text>(driver.getConfiguration());

    List<Pair<Text, Text>> inputs = new ArrayList<Pair<Text, Text>>();
    inputs.add(new Pair<Text, Text>(new Text("k1"), new Text("v1")));
    inputs.add(new Pair<Text, Text>(new Text("k2"), new Text("v2")));
    inputs.add(new Pair<Text, Text>(new Text("k3"), new Text("v3")));
    inputs.add(new Pair<Text, Text>(new Text("k4"), new Text("v4")));
    kvList = feeder.sortAndGroup(inputs, new Comparator<Text>(){
      @Override
      public int compare(Text arg0, Text arg1) {
        return 0;
      }
    }).get(0);
  }

  @Test
  public void testValueIterator() {
    Iterator<Text> iterator = kvList.valueIterator();

    Text oldKey = null, oldValue = null;
    for(int i = 1; i<5; i++){
      Text currentValue = iterator.next();
      Text currentKey = kvList.getCurrentKey();
      Assert.assertEquals("v"+i, currentValue.toString());
      Assert.assertEquals("k"+i, currentKey.toString());
      if (oldKey != null && oldValue != null){
        Assert.assertTrue( oldKey == currentKey );
        Assert.assertTrue( oldValue == currentValue );
      }
      oldKey = currentKey;
      oldValue = currentValue;
    }
  }

  @Test
  public void testClone() {
    KeyValueReuseList<Text, Text> clone = kvList.clone(driver.getConfiguration());
    Assert.assertTrue(kvList != clone);
    assertListEquals(kvList, clone);

    for (int i = 0 ; i < kvList.size() ; i++){
      Assert.assertTrue(kvList.get(i) != clone.get(i));
    }
  }
}
