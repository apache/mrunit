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
package org.apache.hadoop.mrunit.internal.util;

import static org.mockito.Mockito.verify;

import org.apache.commons.logging.Log;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class TestErrors {
  private Log log = Mockito.mock(Log.class);
  private Errors errors = new Errors(log);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void shouldContinueAfterAssertIfNoErrors() {
    errors.assertNone();
  }

  @Test(expected = AssertionError.class)
  public void shouldThrowExceptionOnAssertWhenAnyRecordedError() {
    errors.record("an error");
    errors.assertNone();
  }

  @Test
  public void shouldLogRecordedError() {
    errors.record("this is an %s", "error");
    verify(log).error("this is an error");
  }

  @Test
  public void shouldBuildAssertionErrorMessageWithRecordedErrors() {
    thrown.expectMessage("2 Error(s): (this is a first error, this is a second error)");
    errors.record("this is a %s error", "first");
    errors.record("this is a %s error", "second");
    errors.assertNone();
  }

}
