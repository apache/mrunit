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
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A Mapper implementation which maintains some state in the form of a counter.
 */
public class StatefulMapper extends
    Mapper<LongWritable, Text, Text, IntWritable> {

  public static final Text KEY = new Text("SomeKey");
  private Integer someState = 0;

  /**
   * Increment someState for each input.
   * 
   * @param context
   *          the Hadoop job Map context
   * @throws java.io.IOException
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    this.someState++;
  }

  /**
   * Runs once after all maps have occurred. Dumps the accumulated state to the
   * output.
   * 
   * @param context
   *          the Hadoop job Map context
   */
  @Override
  protected void cleanup(Context context) throws IOException,
      InterruptedException {
    context.write(KEY, new IntWritable(this.someState));
  }

}
