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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.counters.CounterWrapper;
import org.apache.hadoop.mrunit.types.Pair;

public abstract class TestDriver<K1, V1, K2, V2> {

  public static final Log LOG = LogFactory.getLog(TestDriver.class);

  protected List<Pair<K2, V2>> expectedOutputs;

  protected List<Pair<Enum, Long>> expectedEnumCounters;
  protected List<Pair<Pair<String, String>, Long>> expectedStringCounters;

  protected Configuration configuration;

  protected CounterWrapper counterWrapper;

  public TestDriver() {
    expectedOutputs = new ArrayList<Pair<K2, V2>>();
    expectedEnumCounters = new ArrayList<Pair<Enum, Long>>();
    expectedStringCounters = new ArrayList<Pair<Pair<String, String>, Long>>();
    configuration = new Configuration();
  }

  /**
   * @return the list of (k, v) pairs expected as output from this driver
   */
  public List<Pair<K2, V2>> getExpectedOutputs() {
    return expectedOutputs;
  }

  /**
   * Clears the list of outputs expected from this driver
   */
  public void resetOutput() {
    expectedOutputs.clear();
  }

  /**
   * @return expected counters from this driver
   */
  public List<Pair<Enum, Long>> getExpectedEnumCounters() {
    return expectedEnumCounters;
  }

  /**
   * @return expected counters from this driver
   */
  public List<Pair<Pair<String, String>, Long>> getExpectedStringCounters() {
    return expectedStringCounters;
  }

  /**
   * Clears the list of expected counters from this driver
   */
  public void resetExpectedCounters() {
    expectedEnumCounters.clear();
    expectedStringCounters.clear();
  }

  /**
   * Register expected enumeration based counter value
   * 
   * @param e
   *          Enumeration based counter
   * @param expectedValue
   *          Expected value
   * @return
   */
  public TestDriver<K1, V1, K2, V2> withCounter(Enum e, long expectedValue) {
    expectedEnumCounters.add(new Pair<Enum, Long>(e, expectedValue));
    return this;
  }

  /**
   * Register expected name based counter value
   * 
   * @param group
   *          Counter group
   * @param name
   *          Counter name
   * @param expectedValue
   *          Expected value
   * @return
   */
  public TestDriver<K1, V1, K2, V2> withCounter(String group, String name,
      long expectedValue) {
    expectedStringCounters.add(new Pair<Pair<String, String>, Long>(
        new Pair<String, String>(group, name), expectedValue));
    return this;
  }

  /**
   * Runs the test but returns the result set instead of validating it (ignores
   * any addOutput(), etc calls made before this)
   * 
   * @return the list of (k, v) pairs returned as output from the test
   */
  public abstract List<Pair<K2, V2>> run() throws IOException;

  /**
   * Runs the test and validates the results
   */
  public abstract void runTest();

  /**
   * Split "key \t val" into Pair(Text(key), Text(val))
   * 
   * @param tabSeparatedPair
   */
  public static Pair<Text, Text> parseTabbedPair(final String tabSeparatedPair) {

    String key, val;

    if (null == tabSeparatedPair) {
      return null;
    }

    final int split = tabSeparatedPair.indexOf('\t');
    if (-1 == split) {
      return null;
    }

    key = tabSeparatedPair.substring(0, split);
    val = tabSeparatedPair.substring(split + 1);

    return new Pair<Text, Text>(new Text(key), new Text(val));
  }

  /**
   * Split "val,val,val,val..." into a List of Text(val) objects.
   * 
   * @param commaDelimList
   *          A list of values separated by commas
   */
  protected static List<Text> parseCommaDelimitedList(
      final String commaDelimList) {
    final ArrayList<Text> outList = new ArrayList<Text>();

    if (null == commaDelimList) {
      return null;
    }

    final int len = commaDelimList.length();
    int curPos = 0;
    int curComma = commaDelimList.indexOf(',');
    if (curComma == -1) {
      curComma = len;
    }

    while (curPos < len) {
      outList.add(new Text(commaDelimList.substring(curPos, curComma).trim()));
      curPos = curComma + 1;
      curComma = commaDelimList.indexOf(',', curPos);
      if (curComma == -1) {
        curComma = len;
      }
    }

    return outList;
  }

  /**
   * check the outputs against the expected inputs in record
   * 
   * @param outputs
   *          The actual output (k, v) pairs
   */
  protected void validate(final List<Pair<K2, V2>> outputs) {

    boolean success = true;
    final List<String> errors = new ArrayList<String>();
    // were we supposed to get output in the first place?
    // return false if we don't.
    if (expectedOutputs.size() == 0 && outputs.size() > 0) {
      final String msg = "Expected no outputs; got " + outputs.size()
          + " outputs.";
      LOG.error(msg);
      errors.add(msg);
      success = false;
    }

    // make sure all actual outputs are in the expected set,
    // and at the proper position.
    for (int i = 0; i < outputs.size(); i++) {
      final Pair<K2, V2> actual = outputs.get(i);
      success = lookupExpectedValue(actual, i, errors) && success;
    }

    // make sure all expected outputs were accounted for.
    if (expectedOutputs.size() != outputs.size() || !success) {
      // something is unaccounted for. Figure out what.

      final ArrayList<Pair<K2, V2>> actuals = new ArrayList<Pair<K2, V2>>();
      actuals.addAll(outputs);

      for (int i = 0; i < expectedOutputs.size(); i++) {
        final Pair<K2, V2> expected = expectedOutputs.get(i);

        String expectedStr = "(null)";
        if (null != expected) {
          expectedStr = expected.toString();
        }

        boolean found = false;
        for (int j = 0; j < actuals.size() && !found; j++) {
          final Pair<K2, V2> actual = actuals.get(j);

          if (actual.equals(expected)) {
            // don't match against this actual output again
            actuals.remove(j);

            found = true;
          } else if (actual.getFirst().getClass() != expected.getFirst()
              .getClass()) {
            final String msg = "Missing expected output " + expectedStr
                + ": Mismatch in key class: expected: "
                + expected.getFirst().getClass() + " " + "actual: "
                + actual.getFirst().getClass();
            LOG.error(msg);
            errors.add(msg);

            found = true;
          } else if (actual.getSecond().getClass() != expected.getSecond()
              .getClass()) {
            final String msg = "Missing expected output " + expectedStr
                + ": Mismatch in value class: expected: "
                + expected.getSecond().getClass() + " " + "actual: "
                + actual.getSecond().getClass();
            LOG.error(msg);
            errors.add(msg);

            found = true;
          }
        }

        if (!found) {
          final String msg = "Missing expected output " + expectedStr
              + " at position " + i + ".";
          LOG.error(msg);
          errors.add(msg);
        }
      }

      success = false;
    }

    if (!success) {
      final StringBuilder buffer = new StringBuilder();
      buffer.append(errors.size()).append(" Error(s): ");
      formatValueList(errors, buffer);
      fail(buffer.toString());
    }
  }

  /**
   * Check that passed counter do contain all expected counters with proper
   * values.
   * 
   * @param counterWrapper
   */
  protected void validate(CounterWrapper counterWrapper) {
    boolean success = true;
    List<String> errors = new ArrayList<String>();

    // Firstly check enumeration based counters
    for (Pair<Enum, Long> expected : expectedEnumCounters) {
      long actualValue = counterWrapper.findCounterValue(expected.getFirst());

      if (actualValue != expected.getSecond()) {
        String msg = "Counter "
            + expected.getFirst().getDeclaringClass().getCanonicalName() + "."
            + expected.getFirst().toString() + " have value " + actualValue
            + " instead of expected " + expected.getSecond();
        LOG.error(msg);
        errors.add(msg);

        success = false;
      }
    }

    // Second string based counters
    for (Pair<Pair<String, String>, Long> expected : expectedStringCounters) {
      Pair<String, String> counter = expected.getFirst();

      long actualValue = counterWrapper.findCounterValue(counter.getFirst(),
          counter.getSecond());

      if (actualValue != expected.getSecond()) {
        String msg = "Counter with category " + counter.getFirst()
            + " and name " + counter.getSecond() + " have value " + actualValue
            + " instead of expected " + expected.getSecond();
        LOG.error(msg);
        errors.add(msg);

        success = false;
      }
    }

    if (!success) {
      StringBuilder buffer = new StringBuilder();
      buffer.append(errors.size()).append(" Error(s): ");
      formatValueList(errors, buffer);
      fail(buffer.toString());
    }
  }

  /**
   * Part of the validation system.
   * 
   * @param actualVal
   *          A (k, v) pair we got from the Mapper
   * @param actualPos
   *          The position of this pair in the actual output
   * @return true if the expected val at 'actualPos' in the expected list equals
   *         actualVal
   */
  private boolean lookupExpectedValue(final Pair<K2, V2> actualVal,
      final int actualPos, final List<String> errors) {

    // first: Do we have the success condition?
    if (expectedOutputs.size() > actualPos
        && expectedOutputs.get(actualPos).equals(actualVal)) {
      LOG.debug("Matched expected output " + actualVal.toString()
          + " at position " + actualPos);
      return true;
    }

    // second: can we find this output somewhere else in
    // the expected list?
    boolean foundSomewhere = false;

    for (int i = 0; i < expectedOutputs.size() && !foundSomewhere; i++) {
      final Pair<K2, V2> expected = expectedOutputs.get(i);

      if (expected.equals(actualVal)) {
        final String msg = "Matched expected output " + actualVal.toString()
            + " but at incorrect position " + actualPos
            + " (expected position " + i + ")";
        LOG.error(msg);
        errors.add(msg);

        foundSomewhere = true;
      } else if (actualVal.getFirst().getClass() != expected.getFirst()
          .getClass()) {
        final String msg = "Received unexpected output " + actualVal.toString()
            + ": Mismatch in key class: expected: "
            + expected.getFirst().getClass() + " " + "actual: "
            + actualVal.getFirst().getClass();
        LOG.error(msg);
        errors.add(msg);

        foundSomewhere = true;
      } else if (actualVal.getSecond().getClass() != expected.getSecond()
          .getClass()) {
        final String msg = "Received unexpected output " + actualVal.toString()
            + ": Mismatch in value class: expected: "
            + expected.getSecond().getClass() + " " + "actual: "
            + actualVal.getSecond().getClass();
        LOG.error(msg);
        errors.add(msg);

        foundSomewhere = true;
      }
    }

    if (!foundSomewhere) {
      final String msg = "Received unexpected output " + actualVal.toString();
      LOG.error(msg);
      errors.add(msg);
    }

    return false;
  }

  protected static void formatValueList(final List<?> values,
      final StringBuilder sb) {
    sb.append("(");

    if (null != values) {
      boolean first = true;

      for (final Object val : values) {
        if (!first) {
          sb.append(", ");
        }

        first = false;
        sb.append(val.toString());
      }
    }

    sb.append(")");
  }

  /**
   * @return The configuration object that will given to the mapper and/or
   *         reducer associated with the driver (new API only)
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper and/or
   *          reducer associated with the driver (new API only)
   */
  public void setConfiguration(final Configuration configuration) {
    this.configuration = configuration;
  }
}
