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
package org.apache.hadoop.mrunit.internal.output;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.internal.mapred.MockMapredOutputFormat;
import org.apache.hadoop.mrunit.internal.mapreduce.MockMapreduceOutputFormat;

@SuppressWarnings("rawtypes")
public class MockOutputCreator<K, V> {

  private Class<? extends OutputFormat> mapredOutputFormatClass;
  private Class<? extends InputFormat> mapredInputFormatClass;

  private Class<? extends org.apache.hadoop.mapreduce.OutputFormat> mapreduceOutputFormatClass;
  private Class<? extends org.apache.hadoop.mapreduce.InputFormat> mapreduceInputFormatClass;

  public void setMapredFormats(Class<? extends OutputFormat> outputFormatClass,
      Class<? extends InputFormat> inputFormatClass) {
    mapredOutputFormatClass = returnNonNull(outputFormatClass);
    mapredInputFormatClass = returnNonNull(inputFormatClass);
  }

  public void setMapreduceFormats(
      Class<? extends org.apache.hadoop.mapreduce.OutputFormat> outputFormatClass,
      Class<? extends org.apache.hadoop.mapreduce.InputFormat> inputFormatClass) {
    mapreduceOutputFormatClass = returnNonNull(outputFormatClass);
    mapreduceInputFormatClass = returnNonNull(inputFormatClass);
  }

  @SuppressWarnings("deprecation")
  public OutputCollectable<K, V> createMapReduceOutputCollectable(
      Configuration configuration,
      Configuration outputCopyingOrInputFormatConfiguration,
      TaskInputOutputContext context) throws IOException {
    outputCopyingOrInputFormatConfiguration = outputCopyingOrInputFormatConfiguration == null ? configuration
        : outputCopyingOrInputFormatConfiguration;
    if (mapreduceOutputFormatClass != null) {
      return new MockMapreduceOutputFormat<K, V>(new Job(configuration),
          mapreduceOutputFormatClass, mapreduceInputFormatClass, new Job(
              outputCopyingOrInputFormatConfiguration),
              context);
    }
    return new MockOutputCollector<K, V>(
        outputCopyingOrInputFormatConfiguration);
  }
  public OutputCollectable<K, V> createMapredOutputCollectable(
      Configuration configuration,
      Configuration outputCopyingOrInputFormatConfiguration) throws IOException {
    outputCopyingOrInputFormatConfiguration = outputCopyingOrInputFormatConfiguration == null ? configuration
        : outputCopyingOrInputFormatConfiguration;    
    if (mapredOutputFormatClass != null) {
      return new MockMapredOutputFormat<K, V>(new JobConf(configuration),
          mapredOutputFormatClass, mapredInputFormatClass, new JobConf(
              outputCopyingOrInputFormatConfiguration));
    }
    return new MockOutputCollector<K, V>(
        outputCopyingOrInputFormatConfiguration);
  }
}
