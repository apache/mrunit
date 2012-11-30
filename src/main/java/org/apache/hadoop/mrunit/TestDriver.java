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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.io.Serialization;
import org.apache.hadoop.mrunit.internal.util.DistCacheUtils;
import org.apache.hadoop.mrunit.internal.util.Errors;
import org.apache.hadoop.mrunit.internal.util.StringUtils;
import org.apache.hadoop.mrunit.types.Pair;

public abstract class TestDriver<K1, V1, K2, V2, T extends TestDriver<K1, V1, K2, V2, T>> {

  public static final Log LOG = LogFactory.getLog(TestDriver.class);

  protected List<Pair<K2, V2>> expectedOutputs;

  private boolean strictCountersChecking = false;
  protected List<Pair<Enum<?>, Long>> expectedEnumCounters;
  protected List<Pair<Pair<String, String>, Long>> expectedStringCounters;
  protected Configuration configuration;
  private Configuration outputSerializationConfiguration;
  private File tmpDistCacheDir;
  protected CounterWrapper counterWrapper;

  protected Serialization serialization;

  public TestDriver() {
    expectedOutputs = new ArrayList<Pair<K2, V2>>();
    expectedEnumCounters = new ArrayList<Pair<Enum<?>, Long>>();
    expectedStringCounters = new ArrayList<Pair<Pair<String, String>, Long>>();
    configuration = new Configuration();
    serialization = new Serialization(configuration);
  }

  /**
   * Adds output (k, v)* pairs we expect
   * 
   * @param outputRecords
   *          The (k, v)* pairs to add
   */
  public void addAllOutput(final List<Pair<K2, V2>> outputRecords) {
    for (Pair<K2, V2> output : outputRecords) {
      addOutput(output);
    }
  }

  /**
   * Functions like addAllOutput() but returns self for fluent programming style
   * 
   * @param outputRecords
   * @return this
   */
  public T withAllOutput(
      final List<Pair<K2, V2>> outputRecords) {
    addAllOutput(outputRecords);
    return thisAsTestDriver();
  }

  /**
   * Adds an output (k, v) pair we expect
   * 
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(final Pair<K2, V2> outputRecord) {
    addOutput(outputRecord.getFirst(), outputRecord.getSecond());
  }

  /**
   * Adds a (k, v) pair we expect as output
   * @param key the key
   * @param val the value
   */
  public void addOutput(final K2 key, final V2 val) {
    expectedOutputs.add(copyPair(key, val));
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public T withOutput(final Pair<K2, V2> outputRecord) {
    addOutput(outputRecord);
    return thisAsTestDriver();
  }

  /**
   * Works like addOutput() but returns self for fluent programming style
   * 
   * @return this
   */
  public T withOutput(final K2 key, final V2 val) {
    addOutput(key, val);
    return thisAsTestDriver();
  }

  /**
   * Expects an input of the form "key \t val" Forces the output types to
   * Text.
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addOutputFromString(final String output) {
    addOutput((Pair<K2, V2>) parseTabbedPair(output));
  }

  /**
   * Identical to addOutputFromString, but with a fluent programming style
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @return this
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  public T withOutputFromString(final String output) {
    addOutputFromString(output);
    return thisAsTestDriver();
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
  public List<Pair<Enum<?>, Long>> getExpectedEnumCounters() {
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
  
  @SuppressWarnings("unchecked")
  protected T thisAsTestDriver() {
    return (T) this;
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
  public T withCounter(final Enum<?> e,
      final long expectedValue) {
    expectedEnumCounters.add(new Pair<Enum<?>, Long>(e, expectedValue));
    return thisAsTestDriver();
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
  public T withCounter(final String group,
      final String name, final long expectedValue) {
    expectedStringCounters.add(new Pair<Pair<String, String>, Long>(
        new Pair<String, String>(group, name), expectedValue));
    return thisAsTestDriver();
  }

  /**
   * Change counter checking. After this method is called, the test will fail if
   * an actual counter is not matched by an expected counter. By default, the
   * test only check that every expected counter is there.
   * 
   * This mode allows you to ensure that no unexpected counters has been
   * declared.
   */
  public T withStrictCounterChecking() {
    strictCountersChecking = true;
    return thisAsTestDriver();
  }

  /**
   * @return The configuration object that will given to the mapper and/or
   *         reducer associated with the driver
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper and/or
   *          reducer associated with the driver
   */
  public void setConfiguration(final Configuration configuration) {
    this.configuration = returnNonNull(configuration);
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper associated
   *          with the driver
   * @return this object for fluent coding
   */
  public T withConfiguration(
      final Configuration configuration) {
    setConfiguration(configuration);
    return thisAsTestDriver();
  }

  /**
   * Get the {@link Configuration} to use when copying output for use with run*
   * methods or for the InputFormat when reading output back in when setting a
   * real OutputFormat.
   * 
   * @return outputSerializationConfiguration, null when no
   *         outputSerializationConfiguration is set
   */
  public Configuration getOutputSerializationConfiguration() {
    return outputSerializationConfiguration;
  }

  /**
   * Set the {@link Configuration} to use when copying output for use with run*
   * methods or for the InputFormat when reading output back in when setting a
   * real OutputFormat. When this configuration is not set, MRUnit will use the
   * configuration set with {@link #withConfiguration(Configuration)} or
   * {@link #setConfiguration(Configuration)}
   * 
   * @param configuration
   */
  public void setOutputSerializationConfiguration(
      final Configuration configuration) {
    this.outputSerializationConfiguration = returnNonNull(configuration);
  }

  /**
   * Set the {@link Configuration} to use when copying output for use with run*
   * methods or for the InputFormat when reading output back in when setting a
   * real OutputFormat. When this configuration is not set, MRUnit will use the
   * configuration set with {@link #withConfiguration(Configuration)} or
   * {@link #setConfiguration(Configuration)}
   * 
   * @param configuration
   * @return this for fluent style
   */
  public T withOutputSerializationConfiguration(
      Configuration configuration) {
    setOutputSerializationConfiguration(configuration);
    return thisAsTestDriver();
  }

  /**
   * Adds a file to be put on the distributed cache. 
   * The path may be relative and will try to be resolved from
   * the classpath of the test. 
   *  
   * @param path path to the file
   */
  public void addCacheFile(String path) {
    addCacheFile(DistCacheUtils.findResource(path));
  }

  /**
   * Adds a file to be put on the distributed cache.
   * @param uri uri of the file
   */
  public void addCacheFile(URI uri) {
    DistributedCache.addCacheFile(uri, getConfiguration());
  }

  /**
   * Set the list of files to put on the distributed cache
   * @param files list of URIs
   */
  public void setCacheFiles(URI[] files) {
    DistributedCache.setCacheFiles(files, getConfiguration());
  }

  /**
   * Adds an archive to be put on the distributed cache. 
   * The path may be relative and will try to be resolved from
   * the classpath of the test. 
   *  
   * @param path path to the archive
   */
  public void addCacheArchive(String path) {
    addCacheArchive(DistCacheUtils.findResource(path));
  }

  /**
   * Adds an archive to be put on the distributed cache.
   * @param uri uri of the archive
   */
  public void addCacheArchive(URI uri) {
    DistributedCache.addCacheArchive(uri, getConfiguration());
  }

  /**
   * Set the list of archives to put on the distributed cache
   * @param archives list of URIs
   */
  public void setCacheArchives(URI[] archives) {
    DistributedCache.setCacheArchives(archives, getConfiguration());
  }

  /**
   * Adds a file to be put on the distributed cache. 
   * The path may be relative and will try to be resolved from
   * the classpath of the test. 
   *  
   * @param file path to the file
   * @return the driver
   */
  public T withCacheFile(String file) {
    addCacheFile(file);
    return thisAsTestDriver();
  }

  /**
   * Adds a file to be put on the distributed cache.
   * @param file uri of the file
   * @return the driver
   */
  public T withCacheFile(URI file) {
    addCacheFile(file);
    return thisAsTestDriver();
  }

  /**
   * Adds an archive to be put on the distributed cache. 
   * The path may be relative and will try to be resolved from
   * the classpath of the test. 
   *  
   * @param archive path to the archive
   * @return the driver
   */
  public T withCacheArchive(String archive) {
    addCacheArchive(archive);
    return thisAsTestDriver();
  }

  /**
   * Adds an archive to be put on the distributed cache.
   * @param file uri of the archive
   * @return the driver
   */
  public T withCacheArchive(URI archive) {
    addCacheArchive(archive);
    return thisAsTestDriver();
  }

  /**
   * Runs the test but returns the result set instead of validating it (ignores
   * any addOutput(), etc calls made before this). 
   * 
   * Also optionally performs counter validation. 
   * 
   * @param validateCounters whether to run automatic counter validation
   * @return the list of (k, v) pairs returned as output from the test
   */
  public List<Pair<K2, V2>> run(boolean validateCounters) throws IOException {
    final List<Pair<K2, V2>> outputs = run();
    if (validateCounters) {
      validate(counterWrapper);
    }
    return outputs;
  }

  /**
   * Initialises the test distributed cache if required. This
   * process is referred to as "localizing" by Hadoop, but since
   * this is a unit test all files/archives are already local. 
   * 
   * Cached files are not moved but cached archives are extracted 
   * into a temporary directory. 
   * 
   * @throws IOException
   */
  protected void initDistributedCache() throws IOException {

    Configuration conf = getConfiguration();

    if (isDistributedCacheInitialised(conf)) {
      return;
    }

    List<Path> localArchives = new ArrayList<Path>();
    List<Path> localFiles = new ArrayList<Path>();

    if (DistributedCache.getCacheFiles(conf) != null) {
      for (URI uri: DistributedCache.getCacheFiles(conf)) {
        Path filePath = new Path(uri.getPath());
        localFiles.add(filePath);
      }
      if (!localFiles.isEmpty()) {
        DistCacheUtils.addLocalFiles(conf, 
            DistCacheUtils.stringifyPathList(localFiles));
      }
    }
    if (DistributedCache.getCacheArchives(conf) != null) {
      for (URI uri: DistributedCache.getCacheArchives(conf)) {
        Path archivePath = new Path(uri.getPath());
        if (tmpDistCacheDir == null) {
          tmpDistCacheDir = DistCacheUtils.createTempDirectory();
        }
        localArchives.add(DistCacheUtils.extractArchiveToTemp(
            archivePath, tmpDistCacheDir));
      }
      if (!localArchives.isEmpty()) {
        DistCacheUtils.addLocalArchives(conf, 
            DistCacheUtils.stringifyPathList(localArchives));
      }
    }
  }

  /**
   * Checks whether the distributed cache has been "localized", i.e.
   * archives extracted and paths moved so that they can be accessed
   * through {@link DistributedCache#getLocalCacheArchives()} and 
   * {@link DistributedCache#getLocalCacheFiles()}
   * 
   * @param conf the configuration
   * @return true if the cache is initialised
   * @throws IOException
   */
  private boolean isDistributedCacheInitialised(Configuration conf) 
      throws IOException {
    return DistributedCache.getLocalCacheArchives(conf) != null ||
        DistributedCache.getLocalCacheFiles(conf) != null;
  }

  /**
   * Cleans up the distributed cache test by deleting the 
   * temporary directory and any extracted cache archives
   * contained within
   * 
   * @throws IOException 
   *  if the local fs handle cannot be retrieved 
   */
  protected void cleanupDistributedCache() throws IOException {
    if (tmpDistCacheDir != null) {
      FileSystem fs = FileSystem.getLocal(getConfiguration());
      LOG.debug("Deleting " + tmpDistCacheDir.toURI());
      fs.delete(new Path(tmpDistCacheDir.toURI()), true);
    }
    tmpDistCacheDir = null;
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
  public void runTest() throws IOException {
    runTest(true);
  }

  /**
   * Runs the test and validates the results
   * 
   * @param orderMatters
   *          Whether or not output ordering is important
   */
  public void runTest(final boolean orderMatters) throws IOException {
    if (LOG.isDebugEnabled()) {
      printPreTestDebugLog();
    }
    final List<Pair<K2, V2>> outputs = run();
    validate(outputs, orderMatters);
    validate(counterWrapper);
  }

  /**
   * Overridable hook for printing pre-test debug information
   */
  protected void printPreTestDebugLog() {
    //
  }

  /**
   * Split "key \t val" into Pair(Text(key), Text(val))
   * 
   * @param tabSeparatedPair
   * @return
   */
  public static Pair<Text, Text> parseTabbedPair(final String tabSeparatedPair) {
    return StringUtils.parseTabbedPair(tabSeparatedPair);
  }

  /**
   * Split "val,val,val,val..." into a List of Text(val) objects.
   * 
   * @param commaDelimList
   *          A list of values separated by commas
   */
  protected static List<Text> parseCommaDelimitedList(
      final String commaDelimList) {
    return StringUtils.parseCommaDelimitedList(commaDelimList);
  }

  protected <E> E copy(E object) {
    return serialization.copyWithConf(object, configuration);
  }

  protected <S, E> Pair<S, E> copyPair(S first, E second) {
    return new Pair<S, E>(copy(first), copy(second));
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

    final Errors errors = new Errors(LOG);

    if (!outputs.isEmpty()) {
      // were we supposed to get output in the first place?
      if (expectedOutputs.isEmpty()) {
        errors.record("Expected no outputs; got %d outputs.", outputs.size());
      }
      // check that user's key and value writables implement equals, hashCode, toString
      checkOverrides(outputs.get(0));
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
                  errors.record("Matched expected output %s but at "
                      + "incorrect position %d (expected position %d)", output,
                      actualPosition, expectedPosition);
                }
              } else if (expectedPositionsCount > i) {
                // not ok, value wasn't seen enough times
                errors.record("Missing expected output %s at position %d.",
                    output, expectedPositionList.get(i));
              } else {
                // not ok, value seen too many times
                errors.record("Received unexpected output %s at position %d.",
                    output, actualPositionList.get(i));
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
              errors.record("Missing expected output %s", output);
            }
          } else {
            // not ok, value seen too many times
            for (int i = 0; i < actualPositionsCount - expectedPositionsCount; i++) {
              errors.record("Received unexpected output %s", output);
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
    
    errors.assertNone();
  }

  private void checkOverrides(final Pair<K2,V2> outputPair) {
    checkOverride(outputPair.getFirst().getClass());
    checkOverride(outputPair.getSecond().getClass());
  }

  private void checkOverride(final Class<?> clazz) {
    try {
      if (clazz.getMethod("equals", Object.class).getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName() + ".equals(Object) " +
            "is not being overridden - tests may fail!");
      }
      if (clazz.getMethod("hashCode").getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName() + ".hashCode() " +
            "is not being overridden - tests may fail!");
      }
      if (clazz.getMethod("toString").getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName() + ".toString() " +
            "is not being overridden - test failures may be difficult to diagnose.");
        LOG.warn("Consider executing test using run() to access outputs");
      }
    } catch (SecurityException e) {
      LOG.error(e);
    } catch (NoSuchMethodException e) {
      LOG.error(e);
    }
  }

  private void checkTypesAndLogError(final List<Pair<K2, V2>> outputs,
      final Pair<K2, V2> output, final List<Integer> positions,
      final boolean orderMatters, final Errors errors,
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
      errors.record(msg);
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

  
  /**
   * Check counters.
   */
  protected void validate(final CounterWrapper counterWrapper) {
    validateExpectedAgainstActual(counterWrapper);
    validateActualAgainstExpected(counterWrapper);
  }

  /**
   * Same as {@link CounterWrapper#findCounterValues()} but for expectations.
   */
  private Collection<Pair<String, String>> findExpectedCounterValues() {
    Collection<Pair<String, String>> results = new ArrayList<Pair<String, String>>();
    for (Pair<Pair<String, String>,Long> counterAndCount : expectedStringCounters) {
      results.add(counterAndCount.getFirst());
    }
    for (Pair<Enum<?>,Long> counterAndCount : expectedEnumCounters) {
      Enum<?> first = counterAndCount.getFirst();
      String groupName = first.getDeclaringClass().getName();
      String counterName = first.name();
      results.add(new Pair<String, String>(groupName, counterName));
    }
    return results;
  }

  /**
   * Check that provided actual counters contain all expected counters with proper
   * values.
   * 
   * @param counterWrapper
   */
  private void validateExpectedAgainstActual(
      final CounterWrapper counterWrapper) {
    final Errors errors = new Errors(LOG);
  
    // Firstly check enumeration based counters
    for (final Pair<Enum<?>, Long> expected : expectedEnumCounters) {
      final long actualValue = counterWrapper.findCounterValue(expected
          .getFirst());
  
      if (actualValue != expected.getSecond()) {
        errors.record("Counter %s.%s has value %d instead of expected %d",
            expected.getFirst().getDeclaringClass().getCanonicalName(),
            expected.getFirst().toString(), actualValue, expected.getSecond());
      }
    }
  
    // Second string based counters
    for (final Pair<Pair<String, String>, Long> expected : expectedStringCounters) {
      final Pair<String, String> counter = expected.getFirst();
  
      final long actualValue = counterWrapper.findCounterValue(
          counter.getFirst(), counter.getSecond());
  
      if (actualValue != expected.getSecond()) {
        errors
            .record(
                "Counter with category %s and name %s has value %d instead of expected %d",
                counter.getFirst(), counter.getSecond(), actualValue,
                expected.getSecond());
      }
    }
    
    errors.assertNone();
  }

  /**
   * Check that provided actual counters are all expected.
   * 
   * @param counterWrapper
   */
  private void validateActualAgainstExpected(final CounterWrapper counterWrapper) {
    if (strictCountersChecking) {
      final Errors errors = new Errors(LOG);
      Collection<Pair<String, String>> unmatchedCounters = counterWrapper.findCounterValues();
      Collection<Pair<String, String>> findExpectedCounterValues = findExpectedCounterValues();
      unmatchedCounters.removeAll(findExpectedCounterValues);
      if(!unmatchedCounters.isEmpty()) {
        for (Pair<String, String> unmatcherCounter : unmatchedCounters) {
          errors
              .record(
                  "Actual counter (\"%s\",\"%s\") was not found in expected counters",
                  unmatcherCounter.getFirst(), unmatcherCounter.getSecond());
        }
      }
      errors.assertNone();
    }
  }

  protected static void formatValueList(final List<?> values,
      final StringBuilder sb) {
    StringUtils.formatValueList(values, sb);
  }
}
