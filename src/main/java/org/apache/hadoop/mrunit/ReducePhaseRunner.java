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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The class to manage starting the reduce phase is used for type genericity
 * reasons. This class is used in the run() method.
 */
class ReducePhaseRunner<INKEY, INVAL, OUTKEY, OUTVAL> {
  public static final Log LOG = LogFactory.getLog(ReducePhaseRunner.class);

  private final Configuration configuration;
  private final Counters counters;
  private Configuration outputSerializationConfiguration;
  private Class<? extends OutputFormat> outputFormatClass;
  private Class<? extends InputFormat> inputFormatClass;

  ReducePhaseRunner(Class<? extends InputFormat> inputFormatClass,
      Configuration configuration, Counters counters,
      Configuration outputSerializationConfiguration,
      Class<? extends OutputFormat> outputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    this.configuration = configuration;
    this.counters = counters;
    this.outputSerializationConfiguration = outputSerializationConfiguration;
    this.outputFormatClass = outputFormatClass;
  }

  public List<Pair<OUTKEY, OUTVAL>> runReduce(
      final List<Pair<INKEY, List<INVAL>>> inputs,
      final Reducer<INKEY, INVAL, OUTKEY, OUTVAL> reducer) throws IOException {

    final List<Pair<OUTKEY, OUTVAL>> reduceOutputs = new ArrayList<Pair<OUTKEY, OUTVAL>>();

    if (!inputs.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        final StringBuilder sb = new StringBuilder();
        for (Pair<INKEY, List<INVAL>> input : inputs) {
          TestDriver.formatValueList(input.getSecond(), sb);
          LOG.debug("Reducing input (" + input.getFirst() + ", " + sb + ")");
          sb.delete(0, sb.length());
        }
      }

      final ReduceDriver<INKEY, INVAL, OUTKEY, OUTVAL> reduceDriver = ReduceDriver
          .newReduceDriver(reducer).withCounters(counters)
          .withConfiguration(configuration).withAll(inputs);

      if (outputSerializationConfiguration != null) {
        reduceDriver
            .withOutputSerializationConfiguration(outputSerializationConfiguration);
      }

      if (outputFormatClass != null) {
        reduceDriver.withOutputFormat(outputFormatClass, inputFormatClass);
      }

      reduceOutputs.addAll(reduceDriver.run());
    }

    return reduceOutputs;
  }
}