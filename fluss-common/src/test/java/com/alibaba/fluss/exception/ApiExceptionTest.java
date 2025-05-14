package com.alibaba.fluss.exception;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


/** Tests for the {@link com.alibaba.fluss.exception.ApiException} class. */
public class ApiExceptionTest {

    @Test
    public void testConstructor() {
        ApiException exception = new ApiException("test");
        ApiException ioException = new ApiException("test",new IOException("error"));
        ApiException runtimeException = new ApiException(new RuntimeException("error"));
        assertThat(exception.getMessage()).isEqualTo("test");
        assertThat(ioException.getMessage()).isEqualTo("test");
        assertThat(ioException.getCause()).isInstanceOf(IOException.class);
        assertThat(runtimeException.getCause()).isInstanceOf(RuntimeException.class);

    }
}