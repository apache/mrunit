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
package org.apache.hadoop.mrunit.internal.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * MockMultipleOutput will mock the MultipleOutput.class.
 *
 * @param <KEYOUT>
 * @param <VALUEOUT>
 */
public class MockMultipleOutputs<K, V> extends MultipleOutputs<K, V> {

  private Map<String, MockRecordWriter> recordWriterMap = new HashMap<String, MockRecordWriter>();

  public MockMultipleOutputs(TaskInputOutputContext context) {
    super(context);
  }

  public boolean isEmpty() {
    return recordWriterMap.isEmpty();
  }

  public int getMultipleOutputsCount() {
    return recordWriterMap.size();
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value) throws IOException, InterruptedException {
    this.write(namedOutput, key, value, namedOutput);
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value, String baseOutputPath) throws IOException, InterruptedException {
    MockRecordWriter writer = recordWriterMap.get(namedOutput);
    if (writer == null) {
      writer = new MockRecordWriter<K, V>();
      recordWriterMap.put(namedOutput, writer);
    }
    writer.write(key, value);
  }

  @Override
  public void write(K key, V value, String baseOutputPath) throws IOException, InterruptedException {
    //TODO: Understand basedOutputPath
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public <K, V> List<Pair<K, V>> getMultipleOutputs(String outputName) {
    return recordWriterMap.get(outputName).getOutputs();
  }

  public List<String> getMultipleOutputsNames() {
    final List<String> names = new ArrayList<String>();
    names.addAll(recordWriterMap.keySet());
    return names;
  }

  class MockRecordWriter<K, V> extends RecordWriter<K, V> {

    private List<Pair<K, V>> record = new ArrayList<Pair<K, V>>();

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      record.add(new Pair(key, value));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    }

    public List<Pair<K, V>> getOutputs() {
      return record;
    }
  }

}
