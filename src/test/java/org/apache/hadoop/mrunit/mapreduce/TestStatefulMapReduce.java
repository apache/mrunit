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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

/**
 * Test that the mapper state is maintained. Essentially checks that the cleanup
 * method is called only once per task.
 */
public class TestStatefulMapReduce {

  @Test
  public void testClosedFormMapReduce() throws IOException {
    
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver 
      = MapReduceDriver.newMapReduceDriver(new StatefulMapper(), new Reducer());
    
    mapReduceDriver.addInput(new LongWritable(1L), new Text("hello"));
    mapReduceDriver.addInput(new LongWritable(2L), new Text("schmo"));
    mapReduceDriver.withOutput(new Text("SomeKey"), new IntWritable(2));
    mapReduceDriver.runTest();

  }

}
