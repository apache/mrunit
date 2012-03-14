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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestSerialization {

  @Test
  public void testClassWithoutNoArgConstructor() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations",
        "org.apache.hadoop.io.serializer.JavaSerialization");
    assertEquals(new Integer(1), Serialization.copy(new Integer(1), conf));
  }

  @Test
  public void testChangeStateOfCopyArgument() {
    final IntWritable int1 = new IntWritable(1);
    final IntWritable int2 = new IntWritable(2);
    final IntWritable copy = (IntWritable) Serialization.copy(int1, int2,
        new Configuration());
    assertEquals(int1, copy);
    assertEquals(int1, int2);
  }

  @Test
  public void testDontChangeStateOfCopyArgument() {
    final Configuration conf = new Configuration();
    conf.setStrings("io.serializations",
        "org.apache.hadoop.io.serializer.JavaSerialization");
    final Integer int1 = new Integer(1);
    final Integer int2 = new Integer(2);
    final Integer copy = (Integer) Serialization.copy(int1, int2, conf);
    assertEquals(int1, copy);
    assertEquals(new Integer(2), int2);
  }

}
