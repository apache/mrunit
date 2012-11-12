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
