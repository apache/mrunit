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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mrunit.mock.MockOutputCollector;
import org.apache.hadoop.mrunit.types.Pair;

/**
 * o.a.h.mapreduce.Reducer.reduce() expects to use a Reducer.Context
 * object as a parameter. We want to override the functionality
 * of a lot of Context to have it send the results back to us, etc.
 * But since Reducer.Context is an inner class of Reducer, we need to
 * put any subclasses of Reducer.Context in a subclass of Reducer.
 *
 * This wrapper class exists for that purpose.
 */
public class MockReduceContextWrapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  public static final Log LOG = LogFactory.getLog(MockReduceContextWrapper.class);

  /**
   * Mock context instance that provides input to and receives output from
   * the Mapper instance under test.
   */
  public class MockReduceContext extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    // The iterator over the input key, list(val).
    private Iterator<Pair<KEYIN, List<VALUEIN>>> inputIter;

    // The current key and list of values.
    private KEYIN curKey;
    private InspectableIterable curValueIterable;

    private MockOutputCollector<KEYOUT, VALUEOUT> output;

    public MockReduceContext(final List<Pair<KEYIN, List<VALUEIN>>> in, final Counters counters, Configuration conf)
        throws IOException, InterruptedException {

      super(conf,
            new TaskAttemptID("mrunit-jt", 0, false, 0, 0),
            new MockRawKeyValueIterator(), null, null, null,
            new MockOutputCommitter(), new MockReporter(counters), null,
            (Class) Text.class, (Class) Text.class);
      this.inputIter = in.iterator();
      this.output = new MockOutputCollector<KEYOUT, VALUEOUT>(getConfiguration());
    }


    /**
     * A private iterable/iterator implementation that wraps around the 
     * underlying iterable/iterator used by the input value list. This
     * memorizes the last value we saw so that we can return it in getCurrentValue().
     */
    private class InspectableIterable implements Iterable<VALUEIN> {
      private Iterable<VALUEIN> base;
      private VALUEIN lastVal;
      private boolean used;

      public InspectableIterable(final Iterable<VALUEIN> baseCollection) {
        this.base = baseCollection;
        this.used = false;
      }

      public Iterator<VALUEIN> iterator() {
        /*
         * The iterator() method is called by the runtime to get an iterator
         * over an Iteratable. If we always returned a new InspectableIterator,
         * successive for each loops would pass over the data multiple times
         * which is not Hadoop's normal behavior.
         */
        if (used) {
          return new NullIterator();
        } else {
          used = true;
          return new InspectableIterator(this.base.iterator());
        }
      }

      public VALUEIN getLastVal() {
        return lastVal;
      }

      private class InspectableIterator 
          extends ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.ValueIterator
          implements Iterator<VALUEIN> {
        private Iterator<VALUEIN> iter;
        public InspectableIterator(final Iterator<VALUEIN> baseIter) {
          iter = baseIter;
        }

        public VALUEIN next() {
          InspectableIterable.this.lastVal = iter.next();
          return InspectableIterable.this.lastVal;
        }

        public boolean hasNext() {
          return iter.hasNext();
        }

        public void remove() {
          iter.remove();
        }
      }
    }

    /*
     * An iterator implementation that never returns data. This is to preserve
     * Hadoop's behavior where iterating over the same reducer data more than
     * once yields no data.
     */
    private class NullIterator extends
        ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.ValueIterator implements
        Iterator<VALUEIN> {

      public VALUEIN next() {
        return null;
      }

      public boolean hasNext() {
        return false;
      }

      public void remove() {
      }
    }

    @Override
    public boolean nextKey() {
      if (inputIter.hasNext()) {
        // Advance to the next key and list of values
        Pair<KEYIN, List<VALUEIN>> p = inputIter.next();
        curKey = p.getFirst();

        // Reset the value iterator
        curValueIterable = new InspectableIterable(p.getSecond());
        return true;
      } else {
        return false;
      }
    }

    @Override
    public boolean nextKeyValue() {
      return nextKey();
    }

    @Override
    public KEYIN getCurrentKey() {
      return curKey;
    }

    @Override
    public VALUEIN getCurrentValue() {
      return curValueIterable.getLastVal();
    }

    @Override
    public Iterable<VALUEIN> getValues() {
      return curValueIterable;
    }

    public void write(KEYOUT key, VALUEOUT value) throws IOException {
      output.collect(key, value);
    }

    @Override
    /** This method does nothing in the mock version. */
    public void progress() {
    }

    @Override
    /** This method does nothing in the mock version. */
    public void setStatus(String status) {
    }

    /**
     * @return the outputs from the MockOutputCollector back to
     * the test harness.
     */
    public List<Pair<KEYOUT, VALUEOUT>> getOutputs() {
      return output.getOutputs();
    }
  }

  public MockReduceContext getMockContext(List<Pair<KEYIN, List<VALUEIN>>> inputs,
      Counters counters, Configuration conf)
      throws IOException, InterruptedException {
    return new MockReduceContext(inputs, counters, conf);
  }
}
