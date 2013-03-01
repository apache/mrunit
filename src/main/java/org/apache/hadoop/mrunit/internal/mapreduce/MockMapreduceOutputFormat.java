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
package org.apache.hadoop.mrunit.internal.mapreduce;

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.internal.io.Serialization;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

public class MockMapreduceOutputFormat<K, V> implements OutputCollectable<K, V> {

  private static String ATTEMPT = "attempt_000000000000_0000_m_000000_0";
  private static TaskAttemptID TASK_ID = TaskAttemptID.forName(ATTEMPT);

  private final File outputPath = new File(
      System.getProperty("java.io.tmpdir"), "mrunit-" + Math.random());
  private TaskAttemptContext taskAttemptContext;
  @SuppressWarnings("rawtypes")
  private RecordWriter recordWriter;
  @SuppressWarnings("rawtypes")
  private final InputFormat inputFormat;
  @SuppressWarnings("rawtypes")
  private final OutputFormat outputFormat;
  private final List<Pair<K, V>> outputs = new ArrayList<Pair<K, V>>();

  @SuppressWarnings("rawtypes")
  public MockMapreduceOutputFormat(Job outputFormatJob,
      Class<? extends OutputFormat> outputFormatClass,
      Class<? extends InputFormat> inputFormatClass, Job inputFormatJob,
      TaskAttemptContext taskAttemptContext)
      throws IOException {
    this.taskAttemptContext = taskAttemptContext;
    outputFormat = ReflectionUtils.newInstance(outputFormatClass,
        outputFormatJob.getConfiguration());
    inputFormat = ReflectionUtils.newInstance(inputFormatClass,
        inputFormatJob.getConfiguration());

    if (outputPath.exists()) {
      throw new IllegalStateException(
          "Generated the same random dir name twice: " + outputPath);
    }
    if (!outputPath.mkdir()) {
      throw new IOException("Failed to create output dir " + outputPath);
    }
    taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR, 
        new Path(outputPath.toString()).toString());
    taskAttemptContext.getConfiguration().set(FileInputFormat.INPUT_DIR, 
        new Path((outputPath + "/*/*/*/*")).toString());
  }
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void collect(K key, V value) throws IOException {
    try {
      if (recordWriter == null) {
        if(taskAttemptContext.getOutputKeyClass() == null) {
          when(taskAttemptContext.getOutputKeyClass()).thenReturn((Class)key.getClass());
        }
        if(taskAttemptContext.getOutputValueClass() == null) {
          when(taskAttemptContext.getOutputValueClass()).thenReturn((Class)value.getClass());
        }
        if(taskAttemptContext.getTaskAttemptID() == null) {
          when(taskAttemptContext.getTaskAttemptID()).thenReturn(TASK_ID);
        }
        recordWriter = outputFormat.getRecordWriter(taskAttemptContext);
      }
      recordWriter.write(key, value);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings({ "unchecked"})
  @Override
  public List<Pair<K, V>> getOutputs() throws IOException {
    try {
      recordWriter.close(taskAttemptContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    final Serialization serialization = new Serialization(
        taskAttemptContext.getConfiguration());
    try {
      List<InputSplit> inputSplits = inputFormat.getSplits(taskAttemptContext);
      for (InputSplit inputSplit : inputSplits) {
        RecordReader<K, V> recordReader = inputFormat.createRecordReader(
            inputSplit, taskAttemptContext);
        recordReader.initialize(inputSplit, taskAttemptContext);
        while (recordReader.nextKeyValue()) {
          outputs.add(new Pair<K, V>(serialization.copy(recordReader
              .getCurrentKey()), serialization.copy(recordReader
              .getCurrentValue())));
        }
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    FileUtil.fullyDelete(outputPath);
    return outputs;
  }
}