package com.alibaba.fluss.exception;

/** This exception is thrown if the number of table partitions is exceed max.partition.num. */
public class TooManyPartitionsException extends ApiException {

    private static final long serialVersionUID = 1L;

    public TooManyPartitionsException(String message) {
        super(message);
    }
}
