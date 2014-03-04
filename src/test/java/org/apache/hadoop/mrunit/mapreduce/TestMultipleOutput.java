/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.ExpectedSuppliedException;
import org.apache.hadoop.mrunit.mapreduce.TestMultipleOutput.MyMap;
import org.apache.hadoop.mrunit.mapreduce.TestMultipleOutput.MyReduce;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MultipleOutputs.class, TestMultipleOutput.MyMap.class, TestMultipleOutput.NullKeyMap.class, TestMultipleOutput.MyReduce.class})
public class TestMultipleOutput {

  @Rule
  public ExpectedSuppliedException thrown = ExpectedSuppliedException.none();

  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

  @Test
  public void screwYou() {
    thrown.expect(java.lang.NullPointerException.class);
    throw new NullPointerException();
  }

  @Test
  public void TestMapDriver() throws IOException {
    mapDriver = MapDriver.newMapDriver(new MyMap());
    mapDriver.withInput(new LongWritable(0), new Text("this has 4 words"))
            .withInput(new LongWritable(0), new Text("name1-first named output"))
            .withInput(new LongWritable(0), new Text("name1- still first named output"))
            .withInput(new LongWritable(0), new Text("name2- second"))
            .withOutput(new Text("this"), new IntWritable(1))
            .withOutput(new Text("has"), new IntWritable(1))
            .withOutput(new Text("4"), new IntWritable(1))
            .withOutput(new Text("words"), new IntWritable(1))
            .withMultiOutput("name1", new Text("first named output"), new IntWritable(3))
            .withMultiOutput("name1", new Text("still first named output"), new IntWritable(4))
            .withMultiOutput("name2", new Text("second"), new IntWritable(1))
            .runTest();
  }

  @Test
  public void TestMapDriverWithPathOutput() throws IOException {
    mapDriver = MapDriver.newMapDriver(new PathOutputMapper("path1", "path2"));
    mapDriver.withInput(new LongWritable(0), new Text("first"))
        .withInput(new LongWritable(0), new Text("second"))
        .withInput(new LongWritable(0), new Text("third"))
        .withOutput(new Text("first"), new IntWritable(1))
        .withOutput(new Text("second"), new IntWritable(1))
        .withOutput(new Text("third"), new IntWritable(1))
        .withPathOutput(new Text("first"), new IntWritable(1), "path1")
        .withPathOutput(new Text("second"), new IntWritable(1), "path1")
        .withPathOutput(new Text("third"), new IntWritable(1), "path1")
        .withPathOutput(new Text("first"), new IntWritable(1), "path2")
        .withPathOutput(new Text("second"), new IntWritable(1), "path2")
        .withPathOutput(new Text("third"), new IntWritable(1), "path2")
        .runTest();
  }

  @Test
  public void TestMapDriverNullKey() throws IOException {
    mapDriver = MapDriver.newMapDriver(new NullKeyMap());
    mapDriver.withInput(new LongWritable(0), new Text("this"))
            .withOutput(new Text("this"), new IntWritable(1))
            .withMultiOutput("mo", NullWritable.get(), new Text("message in multiOutput"))
            .withMultiOutput("mo", NullWritable.get(), new Text("second message same key"))
            .runTest();
  }

  @Test
  public void TestMapDriverMissingExpectedOutput() throws IOException {
    thrown.expectAssertionErrorMessage("2 Error(s)");
    thrown.expectAssertionErrorMessage("Missing expected outputs for namedOutput 'mo2'");
    thrown.expectAssertionErrorMessage("Missing expected output ((null), message in multiOutput) for namedOutput 'mo2'");
    mapDriver = MapDriver.newMapDriver(new NullKeyMap());
    mapDriver.withInput(new LongWritable(0), new Text("this"))
            .withOutput(new Text("this"), new IntWritable(1))
            .withMultiOutput("mo", NullWritable.get(), new Text("message in multiOutput"))
            .withMultiOutput("mo", NullWritable.get(), new Text("second message same key"))
            .withMultiOutput("mo2", NullWritable.get(), new Text("message in multiOutput"))
            .runTest();
  }

  @Test
  public void TestMapDriverUnexpectOutput() throws IOException {
    thrown.expectAssertionErrorMessage("3 Error(s)");
    thrown.expectMessage("Expected no multiple outputs");
    thrown.expectAssertionErrorMessage("Received unexpected output ((null), message in multiOutput) for unexpected namedOutput 'mo'");
    thrown.expectAssertionErrorMessage("Received unexpected output ((null), second message same key) for unexpected namedOutput 'mo'");
    mapDriver = MapDriver.newMapDriver(new NullKeyMap());
    mapDriver.withInput(new LongWritable(0), new Text("this"))
            .withOutput(new Text("this"), new IntWritable(1))
            .runTest();
  }

  @Test
  public void TestMapDriverUnexpectOutputValue() throws IOException {
    thrown.expectAssertionErrorMessage("1 Error(s)");
    thrown.expectAssertionErrorMessage("Received unexpected output ((null), second message same key) for namedOutput 'mo' at position 1");
    mapDriver = MapDriver.newMapDriver(new NullKeyMap());
    mapDriver.withInput(new LongWritable(0), new Text("this"))
            .withOutput(new Text("this"), new IntWritable(1))
            .withMultiOutput("mo", NullWritable.get(), new Text("message in multiOutput"))
            .runTest();
  }

  @Test
  public void TestReduceDriver() throws IOException {
    reduceDriver = ReduceDriver.newReduceDriver(new MyReduce());
    List<IntWritable> inputs = new ArrayList<IntWritable>();
    inputs.add(new IntWritable(1));
    inputs.add(new IntWritable(2));
    reduceDriver.withInput(new Text("abc"), inputs)
            .withOutput(new Text("hello"), new IntWritable(1))
            .withMultiOutput("test", new Text("key"), new IntWritable(2))
            .runTest();
  }

  @Test
  public void TestReduceDriverWithPathOutput() throws IOException {
    reduceDriver = ReduceDriver.newReduceDriver(new PathOutputReducer("path"));
    List<IntWritable> inputs = new ArrayList<IntWritable>();
    inputs.add(new IntWritable(1));
    inputs.add(new IntWritable(2));
    reduceDriver.withInput(new Text("abc"), inputs)
        .withOutput(new Text("hello"), new IntWritable(1))
        .withPathOutput(new Text("key"), new IntWritable(2), "path").runTest();
  }

  @Test
  public void TestObjectReuese() throws IOException {
    mapDriver = MapDriver.newMapDriver(new ValueReuseMapper());
    mapDriver.withInput(new LongWritable(0), new Text("first"))
        .withInput(new LongWritable(1), new Text("second"))
        .withInput(new LongWritable(2), new Text("third"))
        .withMultiOutput("test", new Text("first"), new IntWritable(2))
        .withMultiOutput("test", new Text("second"), new IntWritable(3))
        .withMultiOutput("test", new Text("third"), new IntWritable(4))
        .runTest();
  }

  public class ValueReuseMapper extends MyMap {
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      word.set(value);
      number.set(number.get() + 1);
      mos.write("test", word, number);
    }
  }

  public class PathOutputMapper extends MyMap {
    private List<String> paths = new ArrayList<String>();

    public PathOutputMapper(String... paths) {
      for (String path : paths) {
        this.paths.add(path);
      }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      for (String path : paths) {
        mos.write(value, number, path);
      }
      context.write(value, number);
    }
  }
  public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected final IntWritable number = new IntWritable(1);
    protected Text word = new Text();
    protected MultipleOutputs<Text, IntWritable> mos = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      mos = new MultipleOutputs<Text, IntWritable>(context);
      super.setup(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (mos != null) {
        mos.close();
      }
      super.cleanup(context);
    }

	@Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      //The first word before a "dash" used to create a named output
      String namedOutput = null;
      int dash = line.indexOf("-");
      if (dash != -1) {
        namedOutput = line.substring(0, dash);
        line = line.substring(dash + 1).trim();
      }

      StringTokenizer tokenizer = new StringTokenizer(line);
      if (namedOutput != null) {
        mos.write(namedOutput, new Text(line), new IntWritable(tokenizer.countTokens()));
      } else {
        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          context.write(word, number);
        }
      }
    }
  }

  public class NullKeyMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private MultipleOutputs<Text, IntWritable> mos = null;

    @Override
    public void setup(Mapper.Context context) throws IOException, InterruptedException {
      mos = new MultipleOutputs<Text, IntWritable>(context);
      super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      mos.write("mo", NullWritable.get(), new Text("message in multiOutput"));
      mos.write("mo", NullWritable.get(), new Text("second message same key"));
      context.write(value, new IntWritable(1));

    }

  };

  public class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    protected MultipleOutputs<Text, IntWritable> mos = null;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      mos = new MultipleOutputs<Text, IntWritable>(context);
      super.setup(context);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      if (mos != null) {
        mos.close();
      }
      super.cleanup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      mos.write("test", new Text("key"), new IntWritable(2));
      context.write(new Text("hello"), new IntWritable(1));
    }
  }

  public class PathOutputReducer extends MyReduce {
    private List<String> paths = new ArrayList<String>();

    public PathOutputReducer(String... paths) {
      for (String path : paths) {
        this.paths.add(path);
      }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      for (String path : paths) {
        mos.write(new Text("key"), new IntWritable(2), path);
      }
      context.write(new Text("hello"), new IntWritable(1));
    }
  }
}
