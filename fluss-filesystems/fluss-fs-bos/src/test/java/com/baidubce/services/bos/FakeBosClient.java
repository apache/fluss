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

package com.baidubce.services.bos;

import com.baidubce.BceClientException;
import com.baidubce.BceServiceException;
import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.model.BosObject;
import com.baidubce.services.bos.model.BosObjectSummary;
import com.baidubce.services.bos.model.CompleteMultipartUploadResponse;
import com.baidubce.services.bos.model.CopyObjectResponse;
import com.baidubce.services.bos.model.GetObjectRequest;
import com.baidubce.services.bos.model.InitiateMultipartUploadResponse;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.ObjectMetadata;
import com.baidubce.services.bos.model.PartETag;
import com.baidubce.services.bos.model.PutObjectResponse;
import com.baidubce.services.bos.model.UploadPartRequest;
import com.baidubce.services.bos.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * In-memory BOS stand-in (must live under com.baidubce.services.* because the SDK derives the
 * service id from the client class's package) with failure injection and the "maxKeys=1 returns
 * deleted objects" quirk.
 */
public class FakeBosClient extends BosClient {
    public final Map<String, byte[]> objects = new TreeMap<>();
    public final List<String> calls = new ArrayList<>();
    public final Set<String> phantoms =
            new HashSet<>(); // deleted keys still returned by list(maxKeys=1)

    public int failUploadPartNumber = -1; // fail this part number (1-based)
    public boolean failComplete;
    public boolean failPut;
    public int phantomMaxKeys = 1; // list calls with maxKeys <= this also return the phantoms

    private final Map<String, Map<Integer, byte[]>> uploads = new TreeMap<>();

    public FakeBosClient() {
        super(config());
    }

    private static BosClientConfiguration config() {
        BosClientConfiguration c = new BosClientConfiguration();
        c.setEndpoint("http://127.0.0.1:1");
        c.setCredentials(new DefaultBceCredentials("ak", "sk"));
        return c;
    }

    @Override
    public PutObjectResponse putObject(String bucket, String key, byte[] data) {
        calls.add("put:" + key);
        if (failPut) {
            throw new BceClientException("injected put failure");
        }
        objects.put(key, data.clone());
        return new PutObjectResponse();
    }

    @Override
    public InitiateMultipartUploadResponse initiateMultipartUpload(String bucket, String key) {
        calls.add("init:" + key);
        String id = "upload-" + uploads.size();
        uploads.put(id, new TreeMap<>());
        InitiateMultipartUploadResponse r = new InitiateMultipartUploadResponse();
        r.setUploadId(id);
        return r;
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest req) {
        calls.add("part:" + req.getPartNumber());
        if (req.getPartNumber() == failUploadPartNumber) {
            throw new BceClientException("injected uploadPart failure");
        }
        try (InputStream in = req.getInputStream()) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            // InputStream#transferTo is Java 9+, keep the test sources buildable on Java 8
            byte[] chunk = new byte[4096];
            int n;
            while ((n = in.read(chunk)) >= 0) {
                bos.write(chunk, 0, n);
            }
            uploads.get(req.getUploadId()).put(req.getPartNumber(), bos.toByteArray());
        } catch (IOException e) {
            throw new BceClientException("read failed", e);
        }
        UploadPartResponse r = new UploadPartResponse();
        r.setPartNumber(req.getPartNumber());
        r.setETag("etag-" + req.getPartNumber());
        return r;
    }

    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(
            String bucket, String key, String uploadId, List<PartETag> parts) {
        calls.add("complete:" + parts.size());
        if (failComplete) {
            throw new BceClientException("injected complete failure");
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (PartETag p : parts) {
            byte[] b = uploads.get(uploadId).get(p.getPartNumber());
            bos.write(b, 0, b.length);
        }
        objects.put(key, bos.toByteArray());
        return new CompleteMultipartUploadResponse();
    }

    @Override
    public void abortMultipartUpload(String bucket, String key, String uploadId) {
        calls.add("abort");
        uploads.remove(uploadId);
    }

    @Override
    public BosObject getObject(GetObjectRequest req) {
        byte[] b = objects.get(req.getKey());
        if (b == null) {
            BceServiceException e = new BceServiceException("not found");
            e.setStatusCode(404);
            throw e;
        }
        long[] range = req.getRange();
        int from = range == null ? 0 : (int) range[0];
        int to = range == null ? b.length : (int) Math.min(b.length, range[1] + 1);
        BosObject o = new BosObject();
        o.setObjectContent(
                new BosObjectInputStream(new ByteArrayInputStream(b, from, to - from), null) {
                    @Override
                    public void close() throws IOException {
                        in.close(); // there is no HTTP response to release
                    }
                });
        return o;
    }

    @Override
    public ObjectMetadata getObjectMetadata(String bucket, String key) {
        byte[] b = objects.get(key);
        if (b == null) {
            BceServiceException e = new BceServiceException("not found");
            e.setStatusCode(404);
            throw e;
        }
        ObjectMetadata m = new ObjectMetadata();
        m.setContentLength(b.length);
        m.setLastModified(new Date(0));
        return m;
    }

    @Override
    public boolean doesObjectExist(String bucket, String key) {
        return objects.containsKey(key);
    }

    @Override
    public void deleteObject(String bucket, String key) {
        calls.add("delete:" + key);
        objects.remove(key);
    }

    @Override
    public CopyObjectResponse copyObject(String sb, String sk, String db, String dk) {
        calls.add("copy:" + sk + "->" + dk);
        objects.put(dk, objects.get(sk).clone());
        return new CopyObjectResponse();
    }

    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest req) {
        String prefix = req.getPrefix() == null ? "" : req.getPrefix();
        String delim = req.getDelimiter();
        String marker = req.getMarker();
        int max = req.getMaxKeys();
        List<BosObjectSummary> contents = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Set<String> keys = new java.util.TreeSet<>(objects.keySet());
        if (max <= phantomMaxKeys) {
            keys.addAll(phantoms); // the quirk observed on the real service
        }
        int count = 0;
        String last = null;
        boolean truncated = false;
        for (String k : keys) {
            if (!k.startsWith(prefix) || (marker != null && k.compareTo(marker) <= 0)) {
                continue;
            }
            String rest = k.substring(prefix.length());
            int idx = delim == null ? -1 : rest.indexOf(delim);
            String entry = idx >= 0 ? prefix + rest.substring(0, idx + delim.length()) : k;
            if (idx >= 0 && !seen.add(entry)) {
                continue;
            }
            if (count == max) {
                truncated = true;
                break;
            }
            if (idx >= 0) {
                prefixes.add(entry);
            } else {
                BosObjectSummary s = new BosObjectSummary();
                s.setKey(k);
                s.setSize(objects.containsKey(k) ? objects.get(k).length : 1000);
                s.setLastModified(new Date(0));
                contents.add(s);
            }
            last = entry;
            count++;
        }
        ListObjectsResponse r = new ListObjectsResponse();
        r.setContents(contents);
        r.setCommonPrefixes(prefixes);
        r.setTruncated(truncated);
        r.setNextMarker(truncated ? last : null);
        return r;
    }
}
