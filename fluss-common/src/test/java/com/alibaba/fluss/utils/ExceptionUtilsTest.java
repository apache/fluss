/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the utility methods in {@link com.alibaba.fluss.utils.ExceptionUtils}. */
public class ExceptionUtilsTest {
    @Test
    void testStringifyNullException() {
        assertThat(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION).isNotNull();
        assertThat(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)
                .isEqualTo(ExceptionUtils.stringifyException(null));
    }

    @Test
    public void testJvmFatalError() {
        // not all errors are fatal
        assertThat(ExceptionUtils.isJvmFatalError(new Error())).isFalse();

        // linkage errors are not fatal
        assertThat(ExceptionUtils.isJvmFatalError(new LinkageError())).isFalse();

        // some errors are fatal
        assertThat(ExceptionUtils.isJvmFatalError(new InternalError())).isTrue();
        assertThat(ExceptionUtils.isJvmFatalError(new UnknownError())).isTrue();
    }

    @Test
    public void testRethrowFatalError() {
        // fatal error is rethrown
        try {
            ExceptionUtils.rethrowIfFatalError(new InternalError());
            fail("Expected InternalError");
        } catch (InternalError ignored) {
        }

        // non-fatal error is not rethrown
        ExceptionUtils.rethrowIfFatalError(new NoClassDefFoundError());
    }

    @Test
    void rethrow() {
        // error should be rethrown
        Throwable error = new InternalError();
        assertThatThrownBy(() -> ExceptionUtils.rethrow(error)).isInstanceOf(InternalError.class);
        // runtime exception should be rethrown
        Throwable runtimeException = new RuntimeException();
        assertThatThrownBy(() -> ExceptionUtils.rethrow(runtimeException))
                .isInstanceOf(RuntimeException.class);
        // other exception should be rethrow with wrapping to runtime exception
        Throwable classNotFoundException = new ClassNotFoundException();
        assertThatThrownBy(() -> ExceptionUtils.rethrow(classNotFoundException))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testReThrowException() {
        Exception exception = new Exception("exception to be rethrown");
        assertThatThrownBy(() -> ExceptionUtils.tryRethrowException(exception))
                .isEqualTo(exception);

        String parentMsg = "wrapping exception msg";
        Error error = new Error("error");
        assertThatThrownBy(() -> ExceptionUtils.rethrowException(error, parentMsg))
                .isEqualTo(error);
        assertThatThrownBy(() -> ExceptionUtils.rethrowException(exception, parentMsg))
                .isEqualTo(exception);
        Throwable throwable = new Throwable("throwable");
        assertThatThrownBy(() -> ExceptionUtils.rethrowException(throwable, parentMsg))
                .isInstanceOf(Exception.class)
                .hasMessage(parentMsg)
                .cause()
                .isEqualTo(throwable);
    }

    @Test
    public void testFindThrowableByType() {
        assertThat(
                        ExceptionUtils.findThrowable(
                                        new RuntimeException(new IllegalStateException()),
                                        IllegalStateException.class)
                                .isPresent())
                .isTrue();
    }

    @Test
    void testFirstOrSuppressed() {
        // tet first exception
        Exception exception = new Exception("exception");
        assertThat(ExceptionUtils.firstOrSuppressed(exception, null)).isEqualTo(exception);

        // test suppress exception
        Exception newException = new Exception("new exception");
        Exception suppressedException = ExceptionUtils.firstOrSuppressed(newException, exception);
        // verify it's the previous exception
        assertThat(suppressedException).isEqualTo(exception);
        // verify it suppressed the new exception
        assertThat(suppressedException.getSuppressed()).isEqualTo(new Throwable[] {newException});
    }

    @Test
    public void testExceptionStripping() {
        final Exception expectedException = new Exception("test exception");
        Throwable strippedException =
                ExceptionUtils.stripException(
                        new RuntimeException(new RuntimeException(expectedException)),
                        RuntimeException.class);

        assertThat(strippedException).isEqualTo(expectedException);

        // test strip ExecutionException
        strippedException =
                ExceptionUtils.stripExecutionException(new ExecutionException(expectedException));
        assertThat(strippedException).isEqualTo(expectedException);

        // test strip CompletionException
        strippedException =
                ExceptionUtils.stripCompletionException(new CompletionException(expectedException));
        assertThat(strippedException).isEqualTo(expectedException);
    }

    @Test
    public void testInvalidExceptionStripping() {
        final Exception expectedException =
                new Exception(new RuntimeException(new Exception("inner exception")));
        final Throwable strippedException =
                ExceptionUtils.stripException(expectedException, RuntimeException.class);

        assertThat(strippedException).isEqualTo(expectedException);
    }

    @Test
    public void testTryEnrichTaskExecutorErrorCanHandleNullValueWithoutCausingException() {
        ExceptionUtils.tryEnrichOutOfMemoryError(null, "", "", "");
    }

    @Test
    public void testUpdateDetailMessageOfBasicThrowable() {
        Throwable rootThrowable = new OutOfMemoryError("old message");
        ExceptionUtils.updateDetailMessage(rootThrowable, t -> "new message");

        assertThat(rootThrowable.getMessage()).isEqualTo("new message");
    }

    @Test
    public void testUpdateDetailMessageOfRelevantThrowableAsCause() {
        Throwable oomCause =
                new IllegalArgumentException("another message deep down in the cause tree");

        Throwable oom = new OutOfMemoryError("old message").initCause(oomCause);
        oom.setStackTrace(
                new StackTraceElement[] {new StackTraceElement("class", "method", "file", 1)});
        oom.addSuppressed(new NullPointerException());

        Throwable rootThrowable = new IllegalStateException("another message", oom);
        ExceptionUtils.updateDetailMessage(
                rootThrowable,
                t -> t.getClass().equals(OutOfMemoryError.class) ? "new message" : null);

        assertThat(rootThrowable.getCause()).isSameAs(oom);
        assertThat(rootThrowable.getCause().getMessage()).isEqualTo("new message");
        assertThat(rootThrowable.getCause().getStackTrace()).isEqualTo(oom.getStackTrace());
        assertThat(rootThrowable.getCause().getSuppressed()).isEqualTo(oom.getSuppressed());

        assertThat(rootThrowable.getCause().getCause()).isSameAs(oomCause);
    }

    @Test
    public void testUpdateDetailMessageWithoutRelevantThrowable() {
        Throwable originalThrowable =
                new IllegalStateException(
                        "root message", new IllegalArgumentException("cause message"));
        ExceptionUtils.updateDetailMessage(originalThrowable, t -> null);

        assertThat(originalThrowable.getMessage()).isEqualTo("root message");
        assertThat(originalThrowable.getCause().getMessage()).isEqualTo("cause message");
    }

    @Test
    public void testUpdateDetailMessageOfNullWithoutException() {
        ExceptionUtils.updateDetailMessage(null, t -> "new message");
    }

    @Test
    public void testUpdateDetailMessageWithMissingPredicate() {
        Throwable root = new Exception("old message");
        ExceptionUtils.updateDetailMessage(root, null);

        assertThat(root.getMessage()).isEqualTo("old message");
    }

    @Test
    public void testIsMetaspaceOutOfMemoryErrorCanHandleNullValue() {
        assertThat(ExceptionUtils.isMetaspaceOutOfMemoryError(null)).isFalse();
    }

    @Test
    public void testIsDirectOutOfMemoryErrorCanHandleNullValue() {
        assertThat(ExceptionUtils.isDirectOutOfMemoryError(null)).isFalse();
    }
}
