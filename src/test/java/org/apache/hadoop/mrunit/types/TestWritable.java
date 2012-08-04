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
package org.apache.hadoop.mrunit.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TestWritable implements WritableComparable<TestWritable> {

  private Text value = new Text();

  public TestWritable() {

  }

  public TestWritable(String value) {
    this.value.set(value);
  }

  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(TestWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      TestWritable a1 = (TestWritable) a;
      TestWritable b1 = (TestWritable) b;
      return -(a1.value.compareTo(b1.value));
    }
  }

  public static class SingleGroupComparator extends WritableComparator {
    public SingleGroupComparator() {
      super(TestWritable.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
      // force all instances into the same group
      return 0;
    }
  }

  public String getValue() {
    return value.toString();
  }

  public void setText(String text) {
    this.value.set(text);
  }

  public void readFields(DataInput arg0) throws IOException {
    value.readFields(arg0);
  }

  public void write(DataOutput arg0) throws IOException {
    value.write(arg0);
  }

  public int compareTo(TestWritable o) {
    return value.compareTo(o.value);
  }

  @Override
  public String toString() {
    return value.toString();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((value == null) ? 0 : value.hashCode());
    return result;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TestWritable other = (TestWritable) obj;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equals(other.value))
      return false;
    return true;
  }

  static {
    WritableComparator.define(TestWritable.class, new Comparator());
  }

}
