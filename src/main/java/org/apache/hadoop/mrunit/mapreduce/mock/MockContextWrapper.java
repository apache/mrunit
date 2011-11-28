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

package org.apache.hadoop.mrunit.mapreduce.mock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mrunit.types.Pair;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public abstract class MockContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  protected final Counters counters;
  protected final Configuration conf;
  protected final List<Pair<KEYOUT, VALUEOUT>> outputs = new ArrayList<Pair<KEYOUT, VALUEOUT>>();

  public MockContextWrapper(Counters counters, Configuration conf) {
    this.conf = conf;
    this.counters = counters;
  }
  
  protected void createCommon(TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException, InterruptedException {
    when(context.getCounter((Enum)any())).thenAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocation) {
        Object[] args = checkNotNull(invocation.getArguments());
        checkArgument(args.length == 1);
        return counters.findCounter((Enum)args[0]);
      }
    });
    when(context.getCounter(anyString(), anyString())).thenAnswer(new Answer<Counter>() {
      @Override
      public Counter answer(InvocationOnMock invocation) {
        Object[] args = checkNotNull(invocation.getArguments());
        checkArgument(args.length == 2);
        return counters.findCounter((String)checkNotNull(args[0]), (String)checkNotNull(args[1]));
      }
    });
    when(context.getConfiguration()).thenAnswer(new Answer<Configuration>() {
      @Override
      public Configuration answer(InvocationOnMock invocation) {
        return conf;
      }
    });
    doAnswer(new Answer<Object>() {
      public Object answer(InvocationOnMock invocation) {
          Object[] args = checkNotNull(invocation.getArguments());
          checkArgument(args.length == 2);
          outputs.add(new Pair(copy(args[0]), copy(args[1])));
          return null;
      }}).when(context).write((KEYOUT)any(), (VALUEOUT)any());

  }
  
  public static Object copy(Object orig) {
    if(orig instanceof Writable) {
      if(orig instanceof NullWritable) {
        return orig;
      }
      try {
        Writable original =  (Writable) orig;
        DataOutputBuffer out = new DataOutputBuffer();
        original.write(out);
        DataInputBuffer in = new DataInputBuffer();
        byte[] buff = out.getData();
        in.reset(buff, buff.length);
        Writable copy;
        copy = original.getClass().newInstance();
        copy.readFields(in);
        return copy;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return orig;
  }

}

