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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * MockMultipleOutput will mock the MultipleOutput.class.
 * 
 * @param <K>
 * @param <V>
 */
public class MockMultipleOutputs<K, V> extends MultipleOutputs<K, V> {
  private Configuration configuration;
  private Map<String, MockOutputCollector> namedOutputCollectorMap;
  private Map<String, MockOutputCollector> pathCollectorMap;

  public MockMultipleOutputs(TaskInputOutputContext context) {
    super(context);
    configuration = new Configuration(context.getConfiguration());
    namedOutputCollectorMap = new HashMap<String, MockOutputCollector>();
    pathCollectorMap = new HashMap<String, MockOutputCollector>();
  }

  public boolean isNamedOutputsEmpty() {
    return namedOutputCollectorMap.isEmpty();
  }

  public int getMultipleOutputsCount() {
    return namedOutputCollectorMap.size();
  }

  public boolean isPathOutputsEmpty() {
    return pathCollectorMap.isEmpty();
  }

  public int getPathOutputsCount() {
    return pathCollectorMap.size();
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value)
      throws IOException, InterruptedException {
    this.write(namedOutput, key, value, namedOutput);
  }

  @Override
  public <K, V> void write(String namedOutput, K key, V value,
      String baseOutputPath) throws IOException, InterruptedException {
    MockOutputCollector collector = namedOutputCollectorMap.get(namedOutput);
    if (collector == null) {
      collector = new MockOutputCollector<K, V>(configuration);
      namedOutputCollectorMap.put(namedOutput, collector);
    }
    collector.collect(key, value);
  }

  @Override
  public void write(K key, V value, String baseOutputPath) throws IOException,
      InterruptedException {
    MockOutputCollector collector = pathCollectorMap.get(baseOutputPath);
    if (collector == null) {
      collector = new MockOutputCollector(configuration);
      pathCollectorMap.put(baseOutputPath, collector);
    }
    collector.collect(key, value);
  }

  public <K, V> List<Pair<K, V>> getMultipleOutputs(String outputName) {
    return namedOutputCollectorMap.get(outputName).getOutputs();
  }

  public List<String> getMultipleOutputsNames() {
    final List<String> names = new ArrayList<String>();
    names.addAll(namedOutputCollectorMap.keySet());
    return names;
  }

  public <K, V> List<Pair<K, V>> getPathOutputs(String outputName) {
    return pathCollectorMap.get(outputName).getOutputs();
  }

  public List<String> getOutputPaths() {
    final List<String> names = new ArrayList<String>();
    names.addAll(pathCollectorMap.keySet());
    return names;
  }

}
