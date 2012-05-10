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
package org.apache.hadoop.mrunit.internal.mapred;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.util.ReflectionUtils;

public class MockMapredOutputFormat<K, V> implements OutputCollectable<K, V> {

  private static final String ATTEMPT = "attempt_000000000000_0000_m_000000_0";
  private static final TaskAttemptID TASK_ID = TaskAttemptID.forName(ATTEMPT);

  private final JobConf conf;
  private final File outputPath = new File(
      System.getProperty("java.io.tmpdir"), "mrunit-" + Math.random());
  private final File outputFile = new File(outputPath, "part-00000");
  private RecordWriter recordWriter;
  private final InputFormat inputFormat;
  private final OutputFormat outputFormat;
  private final List<Pair<K, V>> outputs = new ArrayList<Pair<K, V>>();

  public MockMapredOutputFormat(JobConf conf,
      Class<? extends OutputFormat> outputFormatClass,
      Class<? extends InputFormat> inputFormatClass) throws IOException {
    this.conf = conf;

    outputFormat = ReflectionUtils.newInstance(outputFormatClass, conf);
    inputFormat = ReflectionUtils.newInstance(inputFormatClass, conf);

    if (outputPath.exists()) {
      throw new IllegalStateException(
          "Generated the same random dir name twice: " + outputPath);
    }
    if (!outputPath.mkdir()) {
      throw new IOException("Failed to create output dir " + outputPath);
    }
    FileOutputFormat.setOutputPath(conf, new Path(outputPath.toString()));
    conf.set("mapred.task.id", TASK_ID.toString());
    FileSystem.getLocal(conf).mkdirs(
        new Path(outputPath.toString(), FileOutputCommitter.TEMP_DIR_NAME));
  }

  private void setClassIfUnset(String name, Class<?> classType) {
    conf.setIfUnset(name, classType.getName());
  }

  @Override
  public void collect(K key, V value) throws IOException {
    // only set if classes are unset to allow setting higher level class when
    // using multiple subtypes
    if (recordWriter == null) {
      setClassIfUnset("mapred.output.key.class", key.getClass());
      setClassIfUnset("mapred.output.value.class", value.getClass());

      recordWriter = outputFormat.getRecordWriter(FileSystem.getLocal(conf),
          conf, outputFile.getName(), Reporter.NULL);
    }

    recordWriter.write(key, value);
  }

  @Override
  public List<Pair<K, V>> getOutputs() throws IOException {
    recordWriter.close(Reporter.NULL);

    FileInputFormat.setInputPaths(conf, outputPath + "/*/*/*/*");
    for (InputSplit inputSplit : inputFormat.getSplits(conf, 1)) {
      final RecordReader<K, V> recordReader = inputFormat.getRecordReader(
          inputSplit, conf, Reporter.NULL);
      K key = recordReader.createKey();
      V value = recordReader.createValue();
      while (recordReader.next(key, value)) {
        outputs.add(new Pair<K, V>(key, value));
        key = recordReader.createKey();
        value = recordReader.createValue();
      }
    }
    FileUtil.fullyDelete(outputPath);
    return outputs;
  }
}
