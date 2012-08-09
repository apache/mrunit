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
package org.apache.hadoop.mrunit;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

class InputPathStoringMapper<VALUEIN, VALUEOUT> extends MapReduceBase implements
    Mapper<Text, VALUEIN, Text, VALUEOUT> {
  private Path mapInputPath;

  @Override
  public void map(Text key, VALUEIN value, OutputCollector<Text, VALUEOUT> output,
      Reporter reporter) throws IOException {
    if (reporter.getInputSplit() instanceof FileSplit) {
      mapInputPath = ((FileSplit) reporter.getInputSplit()).getPath();
    }
  }

  Path getMapInputPath() {
    return mapInputPath;
  }
}