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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * o.a.h.mapreduce.Reducer.reduce() expects to use a Reducer.Context object as a
 * parameter. We want to override the functionality of a lot of Context to have
 * it send the results back to us, etc. But since Reducer.Context is an inner
 * class of Reducer, we need to put any subclasses of Reducer.Context in a
 * subclass of Reducer.
 * 
 * This wrapper class exists for that purpose.
 */
public class MockReduceContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
    MockContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  protected static final Log LOG = LogFactory
      .getLog(MockReduceContextWrapper.class);
  protected final List<Pair<KEYIN, List<VALUEIN>>> inputs;
  protected Pair<KEYIN, List<VALUEIN>> currentKeyValue;
  protected final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context;

  public MockReduceContextWrapper(
      final List<Pair<KEYIN, List<VALUEIN>>> inputs, final Counters counters,
      final Configuration conf) throws IOException, InterruptedException {
    super(counters, conf);
    this.inputs = inputs;
    context = create();
  }

  @SuppressWarnings({ "unchecked" })
  private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context create()
      throws IOException, InterruptedException {

    final Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context = mock(org.apache.hadoop.mapreduce.Reducer.Context.class);
    createCommon(context);
    /*
     * In actual context code nextKeyValue() modifies the current state so we
     * can here as well.
     */
    when(context.nextKey()).thenAnswer(new Answer<Boolean>() {
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
    when(context.getValues()).thenAnswer(new Answer<Iterable<VALUEIN>>() {
      @Override
      public Iterable<VALUEIN> answer(final InvocationOnMock invocation) {
        return makeOneUseIterator(currentKeyValue.getSecond().iterator());
      }
    });
    return context;
  }

  /**
   * @return the outputs from the MockOutputCollector back to the test harness.
   */
  public List<Pair<KEYOUT, VALUEOUT>> getOutputs() {
    return outputs;
  }

  protected static <V> Iterable<V> makeOneUseIterator(final Iterator<V> parent) {
    return new Iterable<V>() {
      private final Iterator<V> iter = new Iterator<V>() {
        private boolean used;

        @Override
        public boolean hasNext() {
          if (used) {
            return false;
          }
          return parent.hasNext();
        }

        @Override
        public V next() {
          if (used) {
            throw new IllegalStateException();
          }
          return parent.next();
        }

        @Override
        public void remove() {
          throw new IllegalStateException();
        }
      };

      @Override
      public Iterator<V> iterator() {
        return iter;
      }
    };
  }

  public Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context getMockContext() {
    return context;
  }
}
