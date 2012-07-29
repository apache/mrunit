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
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class TestWordCount {
  private static final String FILE01 = "Hello World Bye World";
  private static final String FILE02 = "Hello Hadoop Goodbye Hadoop";
  private static final int ONE = 1;
  private static final int TWO = 2;
  
  private Mapper<LongWritable, Text, Text, IntWritable> mapper;
  private Reducer<Text, IntWritable, Text, IntWritable> reducer;
  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver;
  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private List<Pair<Text, IntWritable>> expectedOutput;
  
  @Before
  public void setup() {
    mapper = new Map();
    reducer = new Reduce();
    driver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    
    expectedOutput = new ArrayList<Pair<Text, IntWritable>>();
    expectedOutput.add(new Pair<Text, IntWritable>(new Text("Bye"), new IntWritable(ONE)));
    expectedOutput.add(new Pair<Text, IntWritable>(new Text("Goodbye"), new IntWritable(ONE)));
    expectedOutput.add(new Pair<Text, IntWritable>(new Text("Hadoop"), new IntWritable(TWO)));
    expectedOutput.add(new Pair<Text, IntWritable>(new Text("Hello"), new IntWritable(TWO)));
    expectedOutput.add(new Pair<Text, IntWritable>(new Text("World"), new IntWritable(TWO)));
  }

  @Test
  public void TestMapDriver() throws IOException {
    final List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
    inputs.add(new Pair<LongWritable, Text>(new LongWritable(21), new Text(FILE01)));
    inputs.add(new Pair<LongWritable, Text>(new LongWritable(48), new Text(FILE02)));

    final List<Pair<Text, IntWritable>> outputs = new ArrayList<Pair<Text, IntWritable>>();
    outputs.add(new Pair<Text, IntWritable>(new Text("Hello"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("World"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("Bye"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("World"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("Hello"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("Hadoop"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("Goodbye"), new IntWritable(ONE)));
    outputs.add(new Pair<Text, IntWritable>(new Text("Hadoop"), new IntWritable(ONE)));

    mapDriver.withAll(inputs).withAllOutput(outputs).runTest(true);
  }

  @Test
  public void TestReduceDriver() throws IOException {
    final List<IntWritable> input1 = new ArrayList<IntWritable>();
    input1.add(new IntWritable(ONE));
    
    final List<IntWritable> input2 = new ArrayList<IntWritable>();
    input2.add(new IntWritable(ONE));
    input2.add(new IntWritable(ONE));
    
    final List<Pair<Text, List<IntWritable>>> inputs = new ArrayList<Pair<Text, List<IntWritable>>>();
    inputs.add(new Pair<Text, List<IntWritable>>(new Text("Bye"), input1));
    inputs.add(new Pair<Text, List<IntWritable>>(new Text("Goodbye"), input1));
    inputs.add(new Pair<Text, List<IntWritable>>(new Text("Hadoop"), input2));
    inputs.add(new Pair<Text, List<IntWritable>>(new Text("Hello"), input2));    
    inputs.add(new Pair<Text, List<IntWritable>>(new Text("World"), input2));
    
    reduceDriver.withAll(inputs).withAllOutput(expectedOutput).runTest(true);
  }
  
  @Test
  public void TestRun() throws IOException {
    final List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
    inputs.add(new Pair<LongWritable, Text>(new LongWritable(21), new Text(FILE01)));
    inputs.add(new Pair<LongWritable, Text>(new LongWritable(48), new Text(FILE02)));

    driver.withAll(inputs).withAllOutput(expectedOutput).runTest(true);
  }

  /**
   * Word count mapper
   */
  public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }

  /**
   * Word count reducer
   */
  public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

}