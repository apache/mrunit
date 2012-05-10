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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

abstract class AbstractMockContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, CONTEXT extends TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> {

  protected final Counters counters;
  protected final Configuration conf;

  protected final CONTEXT context;
  private final OutputCollectable<KEYOUT, VALUEOUT> outputCollectable;

  public AbstractMockContextWrapper(final Counters counters,
      final Configuration conf,
      final OutputCollectable<KEYOUT, VALUEOUT> outputCollectable)
      throws IOException, InterruptedException {
    this.conf = conf;
    this.counters = counters;
    this.outputCollectable = outputCollectable;
    context = create();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void createCommon(
      final TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
      throws IOException, InterruptedException {
    when(context.getCounter((Enum) any())).thenAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(final InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        return counters.findCounter((Enum) args[0]);
      }
    });
    when(context.getCounter(anyString(), anyString())).thenAnswer(
        new Answer<Counter>() {
          @Override
          public Counter answer(final InvocationOnMock invocation) {
            final Object[] args = invocation.getArguments();
            return counters.findCounter((String) args[0], (String) args[1]);
          }
        });
    when(context.getConfiguration()).thenAnswer(new Answer<Configuration>() {
      @Override
      public Configuration answer(final InvocationOnMock invocation) {
        return conf;
      }
    });
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(final InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        try {
          outputCollectable.collect((KEYOUT) args[0], (VALUEOUT) args[1]);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    }).when(context).write((KEYOUT) any(), (VALUEOUT) any());
  }

  protected abstract CONTEXT create() throws IOException, InterruptedException;

  public CONTEXT getMockContext() {
    return context;
  }
}
