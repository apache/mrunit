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

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public TestDriver<K1, V1, K2, V2> withCounter(final Enum e,
      final long expectedValue) {
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
  public TestDriver<K1, V1, K2, V2> withCounter(final String group,
      final String name, final long expectedValue) {
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
  public void runTest() {
    runTest(true);
  }

  /**
   * Runs the test and validates the results
   * 
   * @param orderMatters
   *          Whether or not output ordering is important
   */
  public abstract void runTest(boolean orderMatters);

  /**
   * Split "key \t val" into Pair(Text(key), Text(val))
   * 
   * @param tabSeparatedPair
   * @return
   */
  public static Pair<Text, Text> parseTabbedPair(final String tabSeparatedPair) {
    final int split = tabSeparatedPair.indexOf('\t');
    if (split == -1) {
      throw new IllegalArgumentException("String pair missing a tab separator");
    }
    return new Pair<Text, Text>(new Text(tabSeparatedPair.substring(0, split)),
        new Text(tabSeparatedPair.substring(split + 1)));
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
   * @param orderMatters
   *          Whether or not output ordering is important when validating test
   *          result
   */
  protected void validate(final List<Pair<K2, V2>> outputs,
      final boolean orderMatters) {

    final List<String> errors = new ArrayList<String>();

    // were we supposed to get output in the first place?
    if (expectedOutputs.isEmpty() && !outputs.isEmpty()) {
      logError(errors,
          String.format("Expected no outputs; got %d outputs.", outputs.size()));
    }

    final Map<Pair<K2, V2>, List<Integer>> expectedPositions = buildPositionMap(expectedOutputs);
    final Map<Pair<K2, V2>, List<Integer>> actualPositions = buildPositionMap(outputs);

    for (final Pair<K2, V2> output : expectedPositions.keySet()) {
      final List<Integer> expectedPositionList = expectedPositions.get(output);
      final List<Integer> actualPositionList = actualPositions.get(output);
      if (actualPositionList != null) {
        // the expected value has been seen - check positions
        final int expectedPositionsCount = expectedPositionList.size();
        final int actualPositionsCount = actualPositionList.size();
        if (orderMatters) {
          // order is important, so the positions must match exactly
          if (expectedPositionList.equals(actualPositionList)) {
            LOG.debug(String.format("Matched expected output %s at "
                + "positions %s", output, expectedPositionList.toString()));
          } else {
            int i = 0;
            while (expectedPositionsCount > i || actualPositionsCount > i) {
              if (expectedPositionsCount > i && actualPositionsCount > i) {
                final int expectedPosition = expectedPositionList.get(i);
                final int actualPosition = actualPositionList.get(i);
                if (expectedPosition == actualPosition) {
                  LOG.debug(String.format("Matched expected output %s at "
                      + "position %d", output, expectedPosition));
                } else {
                  logError(errors, String.format(
                      "Matched expected output %s but at "
                          + "incorrect position %d (expected position %d)",
                      output, actualPosition, expectedPosition));
                }
              } else if (expectedPositionsCount > i) {
                // not ok, value wasn't seen enough times
                logError(errors, String.format(
                    "Missing expected output %s at position %d.", output,
                    expectedPositionList.get(i)));
              } else {
                // not ok, value seen too many times
                logError(errors, String.format(
                    "Received unexpected output %s at position %d.", output,
                    actualPositionList.get(i)));
              }
              i++;
            }
          }
        } else {
          // order is unimportant, just check that the count of times seen match
          if (expectedPositionsCount == actualPositionsCount) {
            // ok, counts match
            LOG.debug(String.format("Matched expected output %s in "
                + "%d positions", output, expectedPositionsCount));
          } else if (expectedPositionsCount > actualPositionsCount) {
            // not ok, value wasn't seen enough times
            for (int i = 0; i < expectedPositionsCount - actualPositionsCount; i++) {
              logError(errors,
                  String.format("Missing expected output %s", output));
            }
          } else {
            // not ok, value seen too many times
            for (int i = 0; i < actualPositionsCount - expectedPositionsCount; i++) {
              logError(errors,
                  String.format("Received unexpected output %s", output));
            }
          }
        }
        actualPositions.remove(output);
      } else {
        // the expected value was not found anywhere - output errors
        checkTypesAndLogError(outputs, output, expectedPositionList,
            orderMatters, errors, "Missing expected output");
      }
    }

    for (final Pair<K2, V2> output : actualPositions.keySet()) {
      // anything left in actual set is unexpected
      checkTypesAndLogError(outputs, output, actualPositions.get(output),
          orderMatters, errors, "Received unexpected output");
    }

    if (!errors.isEmpty()) {
      final StringBuilder buffer = new StringBuilder();
      buffer.append(errors.size()).append(" Error(s): ");
      formatValueList(errors, buffer);
      fail(buffer.toString());
    }

  }

  private void checkTypesAndLogError(final List<Pair<K2, V2>> outputs,
      final Pair<K2, V2> output, final List<Integer> positions,
      final boolean orderMatters, final List<String> errors,
      final String errorString) {
    for (final int pos : positions) {
      String msg = null;
      if (expectedOutputs.size() > pos && outputs.size() > pos) {
        final Pair<K2, V2> actual = outputs.get(pos);
        final Pair<K2, V2> expected = expectedOutputs.get(pos);
        final Class<?> actualKeyClass = actual.getFirst().getClass();
        final Class<?> actualValueClass = actual.getSecond().getClass();
        final Class<?> expectedKeyClass = expected.getFirst().getClass();
        final Class<?> expectedValueClass = expected.getSecond().getClass();
        if (actualKeyClass != expectedKeyClass) {
          msg = String.format(
              "%s %s: Mismatch in key class: expected: %s actual: %s",
              errorString, output, expectedKeyClass, actualKeyClass);
        } else if (actualValueClass != expectedValueClass) {
          msg = String.format(
              "%s %s: Mismatch in value class: expected: %s actual: %s",
              errorString, output, expectedValueClass, actualValueClass);
        }
      }
      if (msg == null) {
        if (orderMatters) {
          msg = String
              .format("%s %s at position %d.", errorString, output, pos);
        } else {
          msg = String.format("%s %s", errorString, output);
        }
      }
      logError(errors, msg);
    }
  }

  private Map<Pair<K2, V2>, List<Integer>> buildPositionMap(
      final List<Pair<K2, V2>> values) {
    final Map<Pair<K2, V2>, List<Integer>> valuePositions = new HashMap<Pair<K2, V2>, List<Integer>>();
    for (int i = 0; i < values.size(); i++) {
      final Pair<K2, V2> output = values.get(i);
      List<Integer> positions;
      if (valuePositions.containsKey(output)) {
        positions = valuePositions.get(output);
      } else {
        positions = new ArrayList<Integer>();
        valuePositions.put(output, positions);
      }
      positions.add(i);
    }
    return valuePositions;
  }

  private void logError(final List<String> errors, final String msg) {
    LOG.error(msg);
    errors.add(msg);
  }

  /**
   * Check that passed counter do contain all expected counters with proper
   * values.
   * 
   * @param counterWrapper
   */
  protected void validate(final CounterWrapper counterWrapper) {
    boolean success = true;
    final List<String> errors = new ArrayList<String>();

    // Firstly check enumeration based counters
    for (final Pair<Enum, Long> expected : expectedEnumCounters) {
      final long actualValue = counterWrapper.findCounterValue(expected
          .getFirst());

      if (actualValue != expected.getSecond()) {
        final String msg = "Counter "
            + expected.getFirst().getDeclaringClass().getCanonicalName() + "."
            + expected.getFirst().toString() + " have value " + actualValue
            + " instead of expected " + expected.getSecond();
        logError(errors, msg);

        success = false;
      }
    }

    // Second string based counters
    for (final Pair<Pair<String, String>, Long> expected : expectedStringCounters) {
      final Pair<String, String> counter = expected.getFirst();

      final long actualValue = counterWrapper.findCounterValue(
          counter.getFirst(), counter.getSecond());

      if (actualValue != expected.getSecond()) {
        final String msg = "Counter with category " + counter.getFirst()
            + " and name " + counter.getSecond() + " have value " + actualValue
            + " instead of expected " + expected.getSecond();
        logError(errors, msg);

        success = false;
      }
    }

    if (!success) {
      final StringBuilder buffer = new StringBuilder();
      buffer.append(errors.size()).append(" Error(s): ");
      formatValueList(errors, buffer);
      fail(buffer.toString());
    }
  }

  protected static void formatValueList(final List<?> values,
      final StringBuilder sb) {
    sb.append("(");

    boolean first = true;
    for (final Object val : values) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(val);
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
    this.configuration = returnNonNull(configuration);
  }
}
