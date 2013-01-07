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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mrunit.internal.output.MockOutputCreator;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * o.a.h.mapreduce.Mapper.map() expects to use a Mapper.Context object as a
 * parameter. We want to override the functionality of a lot of Context to have
 * it send the results back to us, etc. But since Mapper.Context is an inner
 * class of Mapper, we need to put any subclasses of Mapper.Context in a
 * subclass of Mapper.
 * 
 * This wrapper class exists for that purpose.
 */
public class MockMapContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends AbstractMockContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context> {

  protected static final Log LOG = LogFactory
      .getLog(MockMapContextWrapper.class);

  protected final List<Pair<KEYIN, VALUEIN>> inputs;
  protected final MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> driver;
  
  protected Pair<KEYIN, VALUEIN> currentKeyValue;
  
  public MockMapContextWrapper(
      final Configuration configuration,
      final List<Pair<KEYIN, VALUEIN>> inputs,
      final MockOutputCreator<KEYOUT, VALUEOUT> mockOutputCreator,
      final MapDriver<KEYIN, VALUEIN, KEYOUT, VALUEOUT> driver) {
    super(configuration, mockOutputCreator);
    this.inputs = inputs;
    this.driver = driver;
    context = create();

  }

  @Override
  @SuppressWarnings({ "unchecked" })
  protected Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context create() {
    final Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context = mock(org.apache.hadoop.mapreduce.Mapper.Context.class);

    createCommon(context, driver, mockOutputCreator);

    try {
      /*
       * In actual context code nextKeyValue() modifies the current state so we
       * can here as well.
       */
      when(context.nextKeyValue()).thenAnswer(new Answer<Boolean>() {
        @Override
        public Boolean answer(final InvocationOnMock invocation) {
          if (inputs.size() > 0) {
            currentKeyValue = inputs.remove(0);
            return true;
          } else {
            currentKeyValue = null;
            return false;
          }
        }
      });
      when(context.getCurrentKey()).thenAnswer(new Answer<KEYIN>() {
        @Override
        public KEYIN answer(final InvocationOnMock invocation) {
          return currentKeyValue.getFirst();
        }
      });
      when(context.getCurrentValue()).thenAnswer(new Answer<VALUEIN>() {
        @Override
        public VALUEIN answer(final InvocationOnMock invocation) {
          return currentKeyValue.getSecond();
        }
      });
      when(context.getTaskAttemptID()).thenAnswer(new Answer<TaskAttemptID>(){
        @Override
        public TaskAttemptID answer(InvocationOnMock invocation)
            throws Throwable {
          return TaskAttemptID.forName("attempt__0000_m_000000_0");
        }
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    when(context.getInputSplit()).thenAnswer(new Answer<InputSplit>() {
      @Override
      public InputSplit answer(InvocationOnMock invocation) throws Throwable {
        return new MockInputSplit(driver.getMapInputPath());
      }
    });
    return context;
  }
}
