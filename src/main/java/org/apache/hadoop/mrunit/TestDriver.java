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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.internal.counters.CounterWrapper;
import org.apache.hadoop.mrunit.internal.io.Serialization;
import org.apache.hadoop.mrunit.internal.output.MockMultipleOutputs;
import org.apache.hadoop.mrunit.internal.util.DistCacheUtils;
import org.apache.hadoop.mrunit.internal.util.Errors;
import org.apache.hadoop.mrunit.internal.util.PairEquality;
import org.apache.hadoop.mrunit.internal.util.StringUtils;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.apache.hadoop.mrunit.internal.util.ArgumentChecker.returnNonNull;

public abstract class TestDriver<K, V, T extends TestDriver<K, V, T>> {

  public static final Log LOG = LogFactory.getLog(TestDriver.class);

  protected List<Pair<K, V>> expectedOutputs;

  private boolean strictCountersChecking = false;
  protected List<Pair<Enum<?>, Long>> expectedEnumCounters;
  protected List<Pair<Pair<String, String>, Long>> expectedStringCounters;
  /**
   * Configuration object, do not use directly, always use the the getter as it
   * lazily creates the object in the case the setConfiguration() method will be
   * used by the user.
   */
  private Configuration configuration;
  /**
   * Serialization object, do not use directly, always use the the getter as it
   * lazily creates the object in the case the setConfiguration() method will be
   * used by the user.
   */
  private Serialization serialization;

  private Configuration outputSerializationConfiguration;
  private Comparator<K> keyComparator;
  private Comparator<V> valueComparator;
  private File tmpDistCacheDir;
  protected CounterWrapper counterWrapper;
  protected MockMultipleOutputs mos;
  protected Map<String, List<Pair<?, ?>>> expectedMultipleOutputs;
  protected Map<String, List<Pair<?, ?>>> expectedPathOutputs;
  private boolean hasRun = false;

  public TestDriver() {
    expectedOutputs = new ArrayList<Pair<K, V>>();
    expectedEnumCounters = new ArrayList<Pair<Enum<?>, Long>>();
    expectedStringCounters = new ArrayList<Pair<Pair<String, String>, Long>>();
    expectedMultipleOutputs = new HashMap<String, List<Pair<?, ?>>>();
    expectedPathOutputs = new HashMap<String, List<Pair<?, ?>>>();
  }

  /**
   * Check to see if this driver is being reused
   * 
   * @return boolean - true if run() has been called more than once
   */
  protected boolean driverReused() {
    return this.hasRun;
  }

  /**
   * Set to true when run() is called to prevent driver reuse
   */
  protected void setUsedOnceStatus() {
    this.hasRun = true;
  }

  /**
   * Adds output (k, v)* pairs we expect
   * 
   * @param outputRecords
   *          The (k, v)* pairs to add
   */
  public void addAllOutput(final List<Pair<K, V>> outputRecords) {
    for (Pair<K, V> output : outputRecords) {
      addOutput(output);
    }
  }

  /**
   * Functions like addAllOutput() but returns self for fluent programming style
   * 
   * @param outputRecords
   * @return this
   */
  public T withAllOutput(final List<Pair<K, V>> outputRecords) {
    addAllOutput(outputRecords);
    return thisAsTestDriver();
  }

  /**
   * Adds an output (k, v) pair we expect
   * 
   * @param outputRecord
   *          The (k, v) pair to add
   */
  public void addOutput(final Pair<K, V> outputRecord) {
    addOutput(outputRecord.getFirst(), outputRecord.getSecond());
  }

  /**
   * Adds a (k, v) pair we expect as output
   * 
   * @param key
   *          the key
   * @param val
   *          the value
   */
  public void addOutput(final K key, final V val) {
    expectedOutputs.add(copyPair(key, val));
  }

  /**
   * Works like addOutput(), but returns self for fluent style
   * 
   * @param outputRecord
   * @return this
   */
  public T withOutput(final Pair<K, V> outputRecord) {
    addOutput(outputRecord);
    return thisAsTestDriver();
  }

  /**
   * Works like addOutput() but returns self for fluent programming style
   * 
   * @return this
   */
  public T withOutput(final K key, final V val) {
    addOutput(key, val);
    return thisAsTestDriver();
  }

  /**
   * Expects an input of the form "key \t val" Forces the output types to Text.
   * 
   * @param output
   *          A string of the form "key \t val". Trims any whitespace.
   * @deprecated No replacement due to lack of type safety and incompatibility
   *             with non Text Writables
   */
  @Deprecated
  @SuppressWarnings("unchecked")
  public void addOutputFromString(final String output) {
    addOutput((Pair<K, V>) parseTabbedPair(output));
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
  public List<Pair<K, V>> getExpectedOutputs() {
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
   * @return this
   */
  public T withCounter(final Enum<?> e, final long expectedValue) {
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
   * @return this
   */
  public T withCounter(final String group, final String name,
      final long expectedValue) {
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
    if (configuration == null) {
      configuration = new Configuration();
    }
    return configuration;
  }

  /**
   * @return The comparator for output keys or null of none has been set
   */
  public Comparator<K> getKeyComparator() {
    return this.keyComparator;
  }

  /**
   * @return The comparator for output values or null of none has been set
   */
  public Comparator<V> getValueComparator() {
    return this.valueComparator;
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper and/or
   *          reducer associated with the driver. This method should only be
   *          called directly after the constructor as the internal state of the
   *          driver depends on the configuration object
   * @deprecated Use getConfiguration() to set configuration items as opposed to
   *             overriding the entire configuration object as it's used
   *             internally.
   */
  @Deprecated
  public void setConfiguration(final Configuration configuration) {
    this.configuration = returnNonNull(configuration);
  }

  /**
   * @param configuration
   *          The configuration object that will given to the mapper associated
   *          with the driver. This method should only be called directly after
   *          the constructor as the internal state of the driver depends on the
   *          configuration object
   * @deprecated Use getConfiguration() to set configuration items as opposed to
   *             overriding the entire configuration object as it's used
   *             internally.
   * @return this object for fluent coding
   */
  @Deprecated
  public T withConfiguration(final Configuration configuration) {
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
  public T withOutputSerializationConfiguration(Configuration configuration) {
    setOutputSerializationConfiguration(configuration);
    return thisAsTestDriver();
  }

  /**
   * Adds a file to be put on the distributed cache. The path may be relative
   * and will try to be resolved from the classpath of the test.
   * 
   * @param path
   *          path to the file
   */
  public void addCacheFile(String path) {
    addCacheFile(DistCacheUtils.findResource(path));
  }

  /**
   * Adds a file to be put on the distributed cache.
   * 
   * @param uri
   *          uri of the file
   */
  public void addCacheFile(URI uri) {
    DistributedCache.addCacheFile(uri, getConfiguration());
  }

  /**
   * Set the list of files to put on the distributed cache
   * 
   * @param files
   *          list of URIs
   */
  public void setCacheFiles(URI[] files) {
    DistributedCache.setCacheFiles(files, getConfiguration());
  }

  /**
   * Set the output key comparator
   * 
   * @param keyComparator
   *          the key comparator
   */
  public void setKeyComparator(Comparator<K> keyComparator) {
    this.keyComparator = keyComparator;
  }

  /**
   * Set the output value comparator
   * 
   * @param valueComparator
   *          the value comparator
   */
  public void setValueComparator(Comparator<V> valueComparator) {
    this.valueComparator = valueComparator;
  }

  /**
   * Adds an archive to be put on the distributed cache. The path may be
   * relative and will try to be resolved from the classpath of the test.
   * 
   * @param path
   *          path to the archive
   */
  public void addCacheArchive(String path) {
    addCacheArchive(DistCacheUtils.findResource(path));
  }

  /**
   * Adds an archive to be put on the distributed cache.
   * 
   * @param uri
   *          uri of the archive
   */
  public void addCacheArchive(URI uri) {
    DistributedCache.addCacheArchive(uri, getConfiguration());
  }

  /**
   * Set the list of archives to put on the distributed cache
   * 
   * @param archives
   *          list of URIs
   */
  public void setCacheArchives(URI[] archives) {
    DistributedCache.setCacheArchives(archives, getConfiguration());
  }

  /**
   * Adds a file to be put on the distributed cache. The path may be relative
   * and will try to be resolved from the classpath of the test.
   * 
   * @param file
   *          path to the file
   * @return the driver
   */
  public T withCacheFile(String file) {
    addCacheFile(file);
    return thisAsTestDriver();
  }

  /**
   * Adds a file to be put on the distributed cache.
   * 
   * @param file
   *          uri of the file
   * @return the driver
   */
  public T withCacheFile(URI file) {
    addCacheFile(file);
    return thisAsTestDriver();
  }

  /**
   * Adds an archive to be put on the distributed cache. The path may be
   * relative and will try to be resolved from the classpath of the test.
   * 
   * @param archive
   *          path to the archive
   * @return the driver
   */
  public T withCacheArchive(String archive) {
    addCacheArchive(archive);
    return thisAsTestDriver();
  }

  /**
   * Adds an archive to be put on the distributed cache.
   * 
   * @param archive
   *          uri of the archive
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
   * @param validateCounters
   *          whether to run automatic counter validation
   * @return the list of (k, v) pairs returned as output from the test
   */
  public List<Pair<K, V>> run(boolean validateCounters) throws IOException {
    final List<Pair<K, V>> outputs = run();
    if (validateCounters) {
      validate(counterWrapper);
    }
    return outputs;
  }

  private Serialization getSerialization() {
    if (serialization == null) {
      serialization = new Serialization(getConfiguration());
    }
    return serialization;
  }

  /**
   * Initialises the test distributed cache if required. This process is
   * referred to as "localizing" by Hadoop, but since this is a unit test all
   * files/archives are already local.
   * 
   * Cached files are not moved but cached archives are extracted into a
   * temporary directory.
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
      for (URI uri : DistributedCache.getCacheFiles(conf)) {
        Path filePath = new Path(uri.getPath());
        localFiles.add(filePath);
      }
      if (!localFiles.isEmpty()) {
        DistCacheUtils.addLocalFiles(conf,
            DistCacheUtils.stringifyPathList(localFiles));
      }
    }
    if (DistributedCache.getCacheArchives(conf) != null) {
      for (URI uri : DistributedCache.getCacheArchives(conf)) {
        Path archivePath = new Path(uri.getPath());
        if (tmpDistCacheDir == null) {
          tmpDistCacheDir = DistCacheUtils.createTempDirectory();
        }
        localArchives.add(DistCacheUtils.extractArchiveToTemp(archivePath,
            tmpDistCacheDir));
      }
      if (!localArchives.isEmpty()) {
        DistCacheUtils.addLocalArchives(conf,
            DistCacheUtils.stringifyPathList(localArchives));
      }
    }
  }

  /**
   * Checks whether the distributed cache has been "localized", i.e. archives
   * extracted and paths moved so that they can be accessed through
   * {@link DistributedCache#getLocalCacheArchives()} and
   * {@link DistributedCache#getLocalCacheFiles()}
   * 
   * @param conf
   *          the configuration
   * @return true if the cache is initialised
   * @throws IOException
   */
  private boolean isDistributedCacheInitialised(Configuration conf)
      throws IOException {
    return DistributedCache.getLocalCacheArchives(conf) != null
        || DistributedCache.getLocalCacheFiles(conf) != null;
  }

  /**
   * Cleans up the distributed cache test by deleting the temporary directory
   * and any extracted cache archives contained within
   * 
   * @throws IOException
   *           if the local fs handle cannot be retrieved
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
  public abstract List<Pair<K, V>> run() throws IOException;

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
    final List<Pair<K, V>> outputs = run();
    validate(outputs, orderMatters);
    validate(counterWrapper);
    validate(mos);
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
   * @return (k,v)
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
    return getSerialization().copyWithConf(object, getConfiguration());
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
  protected void validate(final List<Pair<K, V>> outputs,
      final boolean orderMatters) {
    // expected nothing and got nothing, everything is fine
    if (outputs.isEmpty() && expectedOutputs.isEmpty()) {
      return;
    }

    final Errors errors = new Errors(LOG);
    // expected nothing but got something
    if (!outputs.isEmpty() && expectedOutputs.isEmpty()) {
      errors.record("Expected no output; got %d output(s).", outputs.size());
      errors.assertNone();
    }
    // expected something but got nothing
    if (outputs.isEmpty() && !expectedOutputs.isEmpty()) {
      errors.record("Expected %d output(s); got no output.",
          expectedOutputs.size());
      errors.assertNone();
    }

    // now, the smart test needs to be done
    // check that user's key and value writables implement equals, hashCode,
    // toString
    checkOverrides(outputs, expectedOutputs);

    final PairEquality<K, V> equality = new PairEquality<K, V>(keyComparator,
        valueComparator);
    if (orderMatters) {
      validateWithOrder(outputs, errors, equality);
    } else {
      validateWithoutOrder(outputs, errors, equality);
    }

    // if there are errors, it might be due to types and not clear from the
    // message
    if (!errors.isEmpty()) {
      Class<?> outputKeyClass = null;
      Class<?> outputValueClass = null;
      Class<?> expectedKeyClass = null;
      Class<?> expectedValueClass = null;

      for (Pair<K, V> output : outputs) {
        if (output.getFirst() != null) {
          outputKeyClass = output.getFirst().getClass();
        }
        if (output.getSecond() != null) {
          outputValueClass = output.getSecond().getClass();
        }
        if (outputKeyClass != null && outputValueClass != null) {
          break;
        }
      }

      for (Pair<K, V> expected : expectedOutputs) {
        if (expected.getFirst() != null) {
          expectedKeyClass = expected.getFirst().getClass();
        }
        if (expected.getSecond() != null) {
          expectedValueClass = expected.getSecond().getClass();
        }
        if (expectedKeyClass != null && expectedValueClass != null) {
          break;
        }
      }

      if (outputKeyClass != null && expectedKeyClass != null
          && !outputKeyClass.equals(expectedKeyClass)) {
        errors.record("Mismatch in key class: expected: %s actual: %s",
            expectedKeyClass, outputKeyClass);
      }

      if (outputValueClass != null && expectedValueClass != null
          && !outputValueClass.equals(expectedValueClass)) {
        errors.record("Mismatch in value class: expected: %s actual: %s",
            expectedValueClass, outputValueClass);
      }
    }
    errors.assertNone();
  }

  private void validateWithoutOrder(final List<Pair<K, V>> outputs,
      final Errors errors, final PairEquality<K, V> equality) {
    Set<Integer> verifiedExpecteds = new HashSet<Integer>();
    Set<Integer> unverifiedOutputs = new HashSet<Integer>();
    for (int i = 0; i < outputs.size(); i++) {
      Pair<K, V> output = outputs.get(i);
      boolean found = false;
      for (int j = 0; j < expectedOutputs.size(); j++) {
        if (verifiedExpecteds.contains(j)) {
          continue;
        }
        Pair<K, V> expected = expectedOutputs.get(j);
        if (equality.isTrueFor(output, expected)) {
          found = true;
          verifiedExpecteds.add(j);
          LOG.debug(String.format("Matched expected output %s no %d at "
              + "position %d", output, j, i));
          break;
        }
      }
      if (!found) {
        unverifiedOutputs.add(i);
      }
    }
    for (int j = 0; j < expectedOutputs.size(); j++) {
      if (!verifiedExpecteds.contains(j)) {
        errors.record("Missing expected output %s", expectedOutputs.get(j));
      }
    }
    for (int i = 0; i < outputs.size(); i++) {
      if (unverifiedOutputs.contains(i)) {
        errors.record("Received unexpected output %s", outputs.get(i));
      }
    }
  }

  private void validateWithOrder(final List<Pair<K, V>> outputs,
      final Errors errors, final PairEquality<K, V> equality) {
    int i = 0;
    for (i = 0; i < Math.min(outputs.size(), expectedOutputs.size()); i++) {
      Pair<K, V> output = outputs.get(i);
      Pair<K, V> expected = expectedOutputs.get(i);
      if (equality.isTrueFor(output, expected)) {
        LOG.debug(String.format("Matched expected output %s at "
            + "position %d", expected, i));
      } else {
        errors.record("Missing expected output %s at position %d, got %s.",
            expected, i, output);
      }
    }
    for (int j = i; j < outputs.size(); j++) {
      errors.record("Received unexpected output %s at position %d.",
          outputs.get(j), j);
    }
    for (int j = i; j < expectedOutputs.size(); j++) {
      errors.record("Missing expected output %s at position %d.",
          expectedOutputs.get(j), j);
    }
  }

  private void checkOverrides(final List<Pair<K, V>> outputPairs,
      final List<Pair<K, V>> expectedOutputPairs) {
    Class<?> keyClass = null;
    Class<?> valueClass = null;
    // key or value could be null, try to find a class
    for (Pair<K, V> pair : outputPairs) {
      if (keyClass == null && pair.getFirst() != null) {
        keyClass = pair.getFirst().getClass();
      }
      if (valueClass == null && pair.getSecond() != null) {
        valueClass = pair.getSecond().getClass();
      }
    }
    for (Pair<K, V> pair : expectedOutputPairs) {
      if (keyClass == null && pair.getFirst() != null) {
        keyClass = pair.getFirst().getClass();
      }
      if (valueClass == null && pair.getSecond() != null) {
        valueClass = pair.getSecond().getClass();
      }
    }
    checkOverride(keyClass);
    checkOverride(valueClass);
  }

  private void checkOverride(final Class<?> clazz) {
    if (clazz == null) {
      return;
    }
    try {
      if (clazz.getMethod("equals", Object.class).getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName() + ".equals(Object) "
            + "is not being overridden - tests may fail!");
      }
      if (clazz.getMethod("hashCode").getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName() + ".hashCode() "
            + "is not being overridden - tests may fail!");
      }
      if (clazz.getMethod("toString").getDeclaringClass() != clazz) {
        LOG.warn(clazz.getCanonicalName()
            + ".toString() "
            + "is not being overridden - test failures may be difficult to diagnose.");
        LOG.warn("Consider executing test using run() to access outputs");
      }
    } catch (SecurityException e) {
      LOG.error(e);
    } catch (NoSuchMethodException e) {
      LOG.error(e);
    }
  }

  private Map<Pair<K, V>, List<Integer>> buildPositionMap(
      final List<Pair<K, V>> values, Comparator<Pair<K, V>> comparator) {
    final Map<Pair<K, V>, List<Integer>> valuePositions = new TreeMap<Pair<K, V>, List<Integer>>(
        comparator);
    for (int i = 0; i < values.size(); i++) {
      final Pair<K, V> output = values.get(i);
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
   * Check Multiple Outputs.
   */

  protected void validateOutputList(String name, Errors errors,
      Map<String, List<Pair<?, ?>>> actuals,
      Map<String, List<Pair<?, ?>>> expects) {

    List<String> removeList = new ArrayList<String>();

    for (String key : expects.keySet()) {
      removeList.add(key);
      List<Pair<?, ?>> expectedValues = expects.get(key);
      List<Pair<?, ?>> actualValues = actuals.get(key);

      if (actualValues == null) {
        errors.record("Missing expected outputs for %s '%s'", name, key);
        actualValues = new ArrayList();
      }

      int expectedSize = expectedValues.size();
      int actualSize = actualValues.size();
      int i = 0;

      while (expectedSize > i || actualSize > i) {
        if (expectedSize > i && actualSize > i) {
          Pair<?, ?> expected = expectedValues.get(i);
          Pair<?, ?> actual = actualValues.get(i);

          if (!expected.equals(actual)) {
            errors.record(
                "Expected output %s for %s '%s' at position %d, but found %s",
                expected.toString(), name, key, i, actual.toString());
          }
        } else if (expectedSize > i) {
          Pair<?, ?> expected = expectedValues.get(i);
          errors.record(
              "Missing expected output %s for %s '%s' at position %d.",
              expected.toString(), name, key, i);
        } else {
          Pair<?, ?> actual = actualValues.get(i);
          errors.record(
              "Received unexpected output %s for %s '%s' at position %d.",
              actual.toString(), name, key, i);
        }
        i++;
      }
    }

    for (String processedOutput : removeList) {
      actuals.remove(processedOutput);
    }

    // The rest of values in actuals, if any
    for (String key : actuals.keySet()) {
      List<Pair<?, ?>> actualValues = actuals.get(key);
      for (Pair pair : actualValues) {
        errors.record("Received unexpected output %s for unexpected %s '%s'",
            pair.toString(), name, key);
      }
    }
  }

  protected void validate(final MockMultipleOutputs mos) {
    final Errors errors = new Errors(LOG);

    if (mos != null && !mos.isNamedOutputsEmpty()
        && expectedMultipleOutputs.isEmpty()) {
      errors.record(
          "Expected no multiple outputs; got %d named MultipleOutputs.",
          mos.getMultipleOutputsCount());
    }

    Map<String, List<Pair<?, ?>>> actuals = buildActualMultipleOutputs(mos);
    Map<String, List<Pair<?, ?>>> expects = buildExpectedMultipleOutputs();

    validateOutputList("namedOutput", errors, actuals, expects);

    actuals.clear();
    expects.clear();

    if (mos != null && !mos.isPathOutputsEmpty()
        && expectedPathOutputs.isEmpty()) {
      errors.record("Expected no pathOutputs; got %d pathOutputs.",
          mos.getPathOutputsCount());
    }

    actuals = buildActualPathOutputs(mos);
    expects = buildExpectedPathOutputs();

    validateOutputList("PathOutput", errors, actuals, expects);

    errors.assertNone();
  }

  private Map<String, List<Pair<?, ?>>> buildActualMultipleOutputs(
      MockMultipleOutputs mos) {
    HashMap<String, List<Pair<?, ?>>> actuals = new HashMap<String, List<Pair<?, ?>>>();
    if (mos != null) {
      List<String> multipleOutputsNames = mos.getMultipleOutputsNames();
      for (String name : multipleOutputsNames) {
        actuals.put(name, mos.getMultipleOutputs(name));
      }
    }
    return actuals;
  }

  private Map<String, List<Pair<?, ?>>> buildExpectedMultipleOutputs() {
    HashMap<String, List<Pair<?, ?>>> result = new HashMap<String, List<Pair<?, ?>>>();
    for (String name : expectedMultipleOutputs.keySet()) {
      result.put(name, expectedMultipleOutputs.get(name));
    }
    return result;
  }

  private Map<String, List<Pair<?, ?>>> buildActualPathOutputs(
      MockMultipleOutputs mos) {
    HashMap<String, List<Pair<?, ?>>> actuals = new HashMap<String, List<Pair<?, ?>>>();
    if (mos != null) {
      List<String> outputPaths = mos.getOutputPaths();
      for (String path : outputPaths) {
        actuals.put(path, mos.getPathOutputs(path));
      }
    }
    return actuals;
  }

  private Map<String, List<Pair<?, ?>>> buildExpectedPathOutputs() {
    HashMap<String, List<Pair<?, ?>>> result = new HashMap<String, List<Pair<?, ?>>>();
    for (String name : expectedPathOutputs.keySet()) {
      result.put(name, expectedPathOutputs.get(name));
    }
    return result;
  }

  /**
   * Same as {@link CounterWrapper#findCounterValues()} but for expectations.
   */
  private Collection<Pair<String, String>> findExpectedCounterValues() {
    Collection<Pair<String, String>> results = new ArrayList<Pair<String, String>>();
    for (Pair<Pair<String, String>, Long> counterAndCount : expectedStringCounters) {
      results.add(counterAndCount.getFirst());
    }
    for (Pair<Enum<?>, Long> counterAndCount : expectedEnumCounters) {
      Enum<?> first = counterAndCount.getFirst();
      String groupName = first.getDeclaringClass().getName();
      String counterName = first.name();
      results.add(new Pair<String, String>(groupName, counterName));
    }
    return results;
  }

  /**
   * Check that provided actual counters contain all expected counters with
   * proper values.
   * 
   * @param counterWrapper
   */
  private void validateExpectedAgainstActual(final CounterWrapper counterWrapper) {
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
      Collection<Pair<String, String>> unmatchedCounters = counterWrapper
          .findCounterValues();
      Collection<Pair<String, String>> findExpectedCounterValues = findExpectedCounterValues();
      unmatchedCounters.removeAll(findExpectedCounterValues);
      if (!unmatchedCounters.isEmpty()) {
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

  public static void formatValueList(final List<?> values,
      final StringBuilder sb) {
    StringUtils.formatValueList(values, sb);
  }

  protected static <KEYIN, VALUEIN> void formatPairList(
      final List<Pair<KEYIN, VALUEIN>> pairs, final StringBuilder sb) {
    StringUtils.formatPairList(pairs, sb);
  }

  /**
   * Adds an output (k, v) pair we expect as Multiple output
   * 
   * @param namedOutput
   * @param outputRecord
   */
  public <K, V> void addMultiOutput(String namedOutput,
      final Pair<K, V> outputRecord) {
    addMultiOutput(namedOutput, outputRecord.getFirst(),
        outputRecord.getSecond());
  }

  /**
   * add a (k, v) pair we expect as Multiple output
   * 
   * @param namedOutput
   * @param key
   * @param val
   */
  public <K, V> void addMultiOutput(final String namedOutput, final K key,
      final V val) {
    List<Pair<?, ?>> outputs = expectedMultipleOutputs.get(namedOutput);
    if (outputs == null) {
      outputs = new ArrayList<Pair<?, ?>>();
      expectedMultipleOutputs.put(namedOutput, outputs);
    }
    outputs.add(new Pair<K, V>(key, val));
  }

  /**
   * works like addMultiOutput() but returns self for fluent programming style
   * 
   * @param namedOutput
   * @param key
   * @param value
   * @return this
   */
  public <K extends Comparable, V extends Comparable> T withMultiOutput(
      final String namedOutput, final K key, final V value) {
    addMultiOutput(namedOutput, key, value);
    return thisAsTestDriver();
  }

  /**
   * Works like addMultiOutput(), but returns self for fluent programming style
   * 
   * @param namedOutput
   * @param outputRecord
   * @return this
   */
  public <K, V> T withMultiOutput(String namedOutput,
      final Pair<K, V> outputRecord) {
    addMultiOutput(namedOutput, outputRecord);
    return thisAsTestDriver();
  }

  public <K, V> T withPathOutput(final K key, final V value, final String path) {
    return withPathOutput(new Pair<K, V>(key, value), path);
  }

  public <K, V> T withPathOutput(final Pair<K, V> outputRecord, String path) {
    List<Pair<?, ?>> list = expectedPathOutputs.get(path);
    if (list == null) {
      list = new ArrayList<Pair<?, ?>>();
      expectedPathOutputs.put(path, list);
    }
    list.add(outputRecord);
    return thisAsTestDriver();
  }
}
