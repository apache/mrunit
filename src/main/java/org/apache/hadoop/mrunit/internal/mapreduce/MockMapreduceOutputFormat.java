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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
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
  private static final Class<?>[] TASK_ATTEMPT_CONTEXT_CLASSES = new Class<?>[] {
      Configuration.class, TaskAttemptID.class };
  private static final Class<?>[] JOB_CONTEXT_CLASSES = new Class<?>[] {
      Configuration.class, JobID.class };

  private final Job outputFormatJob;
  private final Job inputFormatJob;
  private final File outputPath = new File(
      System.getProperty("java.io.tmpdir"), "mrunit-" + Math.random());
  private TaskAttemptContext taskAttemptContext;
  private RecordWriter recordWriter;
  private final InputFormat inputFormat;
  private final OutputFormat outputFormat;
  private final List<Pair<K, V>> outputs = new ArrayList<Pair<K, V>>();

  public MockMapreduceOutputFormat(Job outputFormatJob,
      Class<? extends OutputFormat> outputFormatClass,
      Class<? extends InputFormat> inputFormatClass, Job inputFormatJob)
      throws IOException {
    this.outputFormatJob = outputFormatJob;
    this.inputFormatJob = inputFormatJob;

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
    FileOutputFormat.setOutputPath(outputFormatJob,
        new Path(outputPath.toString()));
  }

  private void setClassIfUnset(String name, Class<?> classType) {
    outputFormatJob.getConfiguration().setIfUnset(name, classType.getName());
  }

  private Object createObject(String primaryClassName,
      String secondaryClassName, Class<?>[] constructorParametersClasses,
      Object... constructorParameters) {
    try {
      Class<?> classType = Class.forName(primaryClassName);
      try {
        Constructor<?> constructor = classType
            .getConstructor(constructorParametersClasses);
        return constructor.newInstance(constructorParameters);
      } catch (SecurityException e) {
        throw new IllegalStateException(e);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException(e);
      } catch (InstantiationException e) {
        throw new IllegalStateException(e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    } catch (ClassNotFoundException e) {
      if (secondaryClassName == null) {
        throw new IllegalStateException(e);
      }
      return createObject(secondaryClassName, null,
          constructorParametersClasses, constructorParameters);
    }
  }

  @Override
  public void collect(K key, V value) throws IOException {
    try {
      if (recordWriter == null) {
        setClassIfUnset("mapred.output.key.class", key.getClass());
        setClassIfUnset("mapred.output.value.class", value.getClass());

        taskAttemptContext = (TaskAttemptContext) createObject(
            "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl",
            "org.apache.hadoop.mapreduce.TaskAttemptContext",
            TASK_ATTEMPT_CONTEXT_CLASSES, outputFormatJob.getConfiguration(),
            TASK_ID);
        recordWriter = outputFormat.getRecordWriter(taskAttemptContext);
      }

      recordWriter.write(key, value);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<Pair<K, V>> getOutputs() throws IOException {
    try {
      recordWriter.close(taskAttemptContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

    final Serialization serialization = new Serialization(
        inputFormatJob.getConfiguration());
    FileInputFormat.setInputPaths(inputFormatJob, outputPath + "/*/*/*/*");
    try {
      List<InputSplit> inputSplits = inputFormat
          .getSplits((JobContext) createObject(
              "org.apache.hadoop.mapreduce.task.JobContextImpl",
              "org.apache.hadoop.mapreduce.JobContext", JOB_CONTEXT_CLASSES,
              inputFormatJob.getConfiguration(), new JobID()));
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
