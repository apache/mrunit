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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.internal.output.MockOutputCreator;
import org.apache.hadoop.mrunit.internal.output.OutputCollectable;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

abstract class AbstractMockContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, CONTEXT 
extends TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> {

  protected CONTEXT context;
  protected final MockOutputCreator<KEYOUT, VALUEOUT> mockOutputCreator;
  protected OutputCollectable<KEYOUT, VALUEOUT> outputCollectable;

  public AbstractMockContextWrapper(final MockOutputCreator<KEYOUT, VALUEOUT> mockOutputCreator) {
    this.mockOutputCreator = mockOutputCreator;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  protected void createCommon(
      final TaskInputOutputContext context,
      final ContextDriver contextDriver,
      final MockOutputCreator mockOutputCreator) {
        
    when(context.getCounter((Enum) any())).thenAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(final InvocationOnMock invocation) {
        final Object[] args = invocation.getArguments();
        return contextDriver.getCounters().findCounter((Enum) args[0]);
      }
    });
    when(context.getCounter(anyString(), anyString())).thenAnswer(
        new Answer<Counter>() {
          @Override
          public Counter answer(final InvocationOnMock invocation) {
            final Object[] args = invocation.getArguments();
            return contextDriver.getCounters().findCounter((String) args[0], (String) args[1]);
          }
    });
    when(context.getConfiguration()).thenAnswer(new Answer<Configuration>() {
      @Override
      public Configuration answer(final InvocationOnMock invocation) {
        return contextDriver.getConfiguration();
      }
    });
    try {
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(final InvocationOnMock invocation) {
          final Object[] args = invocation.getArguments();
          try {
            if(outputCollectable == null) {
              outputCollectable = mockOutputCreator.createOutputCollectable(contextDriver.getConfiguration(), 
                  contextDriver.getOutputCopyingOrInputFormatConfiguration());
            }
            outputCollectable.collect((KEYOUT)args[0], (VALUEOUT)args[1]);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return null;
        }
      }).when(context).write(any(), any());
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract CONTEXT create() throws IOException, InterruptedException;

  public List<Pair<KEYOUT, VALUEOUT>> getOutputs() throws IOException {
    if(outputCollectable == null) {
      return new ArrayList<Pair<KEYOUT, VALUEOUT>>();
    }
    return outputCollectable.getOutputs();
  }
  
  public CONTEXT getMockContext() {
    return context;
  }
}
