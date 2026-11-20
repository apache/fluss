/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.oss;

import org.apache.fluss.fs.FileSystemFailure;
import org.apache.fluss.fs.FileSystemOperationException;
import org.apache.fluss.fs.FileSystemPathNotFoundException;
import org.apache.fluss.fs.hdfs.HadoopFileSystem;
import org.apache.fluss.fs.oss.token.OSSSecurityTokenProvider;
import org.apache.fluss.fs.token.ObtainedSecurityToken;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketTimeoutException;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A {@link FileSystem} for Oss that wraps an {@link HadoopFileSystem}, but overwrite method to
 * generate access security token.
 */
class OSSFileSystem extends HadoopFileSystem {

    private final Configuration conf;
    private volatile OSSSecurityTokenProvider ossSecurityTokenProvider;
    private final String scheme;

    OSSFileSystem(FileSystem hadoopFileSystem, String scheme, Configuration conf) {
        super(hadoopFileSystem);
        this.scheme = scheme;
        this.conf = conf;
    }

    @Override
    protected IOException normalize(Exception failure, Operation operation) {
        if (failure instanceof IOException && failure instanceof FileSystemFailure) {
            return (IOException) failure;
        }
        OSSException serviceFailure = findCause(failure, OSSException.class);
        if (serviceFailure == null) {
            ClientException clientFailure = findCause(failure, ClientException.class);
            if (clientFailure != null) {
                return new FileSystemOperationException(
                        FileSystemFailure.Kind.UNEXPECTED,
                        FileSystemFailure.Resource.UNKNOWN,
                        operation.code(),
                        findCause(failure, SocketTimeoutException.class) != null,
                        clientFailure.getErrorCode(),
                        clientFailure.getRequestId(),
                        failure);
            }
            // AliyunOSSFileSystem can discard a metadata request failure and synthesize a
            // FileNotFoundException after an empty listing, even when metadata access was denied.
            if (failure instanceof FileNotFoundException) {
                return new FileSystemOperationException(
                        FileSystemFailure.Kind.UNEXPECTED,
                        FileSystemFailure.Resource.UNKNOWN,
                        operation.code(),
                        false,
                        null,
                        null,
                        failure);
            }
            return super.normalize(failure, operation);
        }

        String code = serviceFailure.getErrorCode();
        String requestId = serviceFailure.getRequestId();
        if ("NoSuchKey".equals(code) && operation == Operation.OPEN) {
            return new FileSystemPathNotFoundException(operation.code(), code, requestId, failure);
        }
        FileSystemFailure.Kind kind = FileSystemFailure.Kind.UNEXPECTED;
        FileSystemFailure.Resource resource = FileSystemFailure.Resource.UNKNOWN;
        boolean temporary = false;
        if ("NoSuchBucket".equals(code)) {
            kind = FileSystemFailure.Kind.NOT_FOUND;
            resource = FileSystemFailure.Resource.ROOT;
        } else if ("NoSuchKey".equals(code)) {
            kind = FileSystemFailure.Kind.NOT_FOUND;
        } else if ("AccessDenied".equals(code)
                || "InvalidAccessKeyId".equals(code)
                || "SignatureDoesNotMatch".equals(code)) {
            kind = FileSystemFailure.Kind.PERMISSION_DENIED;
        } else if ("TotalQpsLimitExceeded".equals(code)
                || "MetaOperationQpsLimitExceeded".equals(code)
                || "ActiveRequestLimitExceeded".equals(code)
                || "DownloadTrafficRateLimitExceeded".equals(code)
                || "UploadTrafficRateLimitExceeded".equals(code)
                || "SlowDown".equals(code)) {
            kind = FileSystemFailure.Kind.RATE_LIMITED;
            temporary = true;
        } else if ("ServiceUnavailable".equals(code)
                || "InternalError".equals(code)
                || "RequestTimeout".equals(code)) {
            temporary = true;
        }
        return new FileSystemOperationException(
                kind, resource, operation.code(), temporary, code, requestId, failure);
    }

    private static <T extends Throwable> T findCause(Throwable failure, Class<T> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
        }
        return null;
    }

    @Override
    public ObtainedSecurityToken obtainSecurityToken() throws IOException {
        try {
            mayCreateSecurityTokenProvider();
            return ossSecurityTokenProvider.obtainSecurityToken(scheme);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void mayCreateSecurityTokenProvider() throws IOException {
        if (ossSecurityTokenProvider == null) {
            synchronized (this) {
                if (ossSecurityTokenProvider == null) {
                    ossSecurityTokenProvider = new OSSSecurityTokenProvider(conf);
                }
            }
        }
    }
}
