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

import org.hamcrest.Matcher;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Delegating class to add convenience methods to {@link ExpectedException}
 */
public class ExpectedSuppliedException implements TestRule {

  private final ExpectedException expectedException = ExpectedException.none();

  private ExpectedSuppliedException() {
  }

  public static ExpectedSuppliedException none() {
    return new ExpectedSuppliedException();
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return expectedException.apply(base, description);
  }

  public void expect(final Matcher<?> matcher) {
    expectedException.expect(matcher);
  }

  public void expect(final Class<? extends Throwable> type) {
    expectedException.expect(type);
  }

  public void expectMessage(final String substring) {
    expectedException.expectMessage(substring);
  }

  public void expectMessage(final Class<? extends Throwable> type,
      final String substring) {
    expect(type);
    expectMessage(substring);
  }

  public void expectAssertionErrorMessage(final String substring) {
    expectMessage(AssertionError.class, substring);
  }

  public void expectMessage(final Matcher<String> matcher) {
    expectedException.expectMessage(matcher);
  }

  @Override
  public int hashCode() {
    return expectedException.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return expectedException.equals(obj);
  }

  @Override
  public String toString() {
    return expectedException.toString();
  }

}
