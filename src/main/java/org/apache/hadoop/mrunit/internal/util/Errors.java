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

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;

/**
 * Encapsulation of lazy error throwing. Errors can be recorded any time
 * during the validation but most of the validation should happen and the
 * errors should be thrown only at the end.
 */
public class Errors {
  private final List<String> messages = new ArrayList<String>();
  private final Log log;

  /**
   * @param log
   *          used for interaction with consumer of MRUnit
   */
  public Errors(Log log) {
    this.log = log;
  }

  /**
   * Log a new error message and keep it for later.
   */
  public void record(String message) {
    log.error(message);
    messages.add(message);
  }

  /**
   * Log a new error message and keep it for later.
   */
  public void record(String format, Object... args) {
    final String message = String.format(format, args);
    log.error(message);
    messages.add(message);
  }

  /**
   * Not empty after first record.
   */
  public boolean isEmpty() {
    return messages.isEmpty();
  }

  /**
   * Throw an validation exception if any message have been recorded before.
   */
  public void assertNone() {
    if (!isEmpty()) {
      fail(toString());
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append(messages.size()).append(" Error(s): ");
    StringUtils.formatValueList(messages, buffer);
    return buffer.toString();
  }

}
