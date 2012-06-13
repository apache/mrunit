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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class MockReporter implements Reporter {

  private final MockInputSplit inputSplit;
  private final Counters counters;

  public enum ReporterType {
    Mapper, Reducer
  }

  private final ReporterType typ;

  public MockReporter(final ReporterType kind, final Counters ctrs,
      final Path mapInputPath) {
    typ = kind;
    counters = ctrs;
    inputSplit = new MockInputSplit(mapInputPath);
  }

  @Override
  public InputSplit getInputSplit() {
    if (typ == ReporterType.Reducer) {
      throw new UnsupportedOperationException(
          "Reducer cannot call getInputSplit()");
    } else {
      return inputSplit;
    }
  }

  @Override
  public void incrCounter(final Enum<?> key, final long amount) {
    counters.incrCounter(key, amount);
  }

  @Override
  public void incrCounter(final String group, final String counter,
      final long amount) {
    counters.incrCounter(group, counter, amount);
  }

  @Override
  public void setStatus(final String status) {
    // do nothing.
  }

  @Override
  public void progress() {
    // do nothing.
  }

  @Override
  public Counter getCounter(final String group, final String name) {
    return counters.findCounter(group, name);
  }

  @Override
  public Counter getCounter(final Enum<?> key) {
    return counters.findCounter(key);
  }

  public float getProgress() {
    return 0;
  }
}
