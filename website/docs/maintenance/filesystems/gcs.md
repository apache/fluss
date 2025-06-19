---
title: Google Cloud Storage
sidebar_position: 5
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Google Cloud Storage

[Google Cloud Storage](https://cloud.google.com/storage) (Google GCS) is an enterprise-grade cloud object storage service offering industry-leading scalability, global data availability, multi-layered security, and optimized performance.

## Configurations setup

To enabled GCS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: gs://<your-bucket>/path/to/remote/storage
# SERVICE_ACCOUNT_JSON_KEYFILE
gs.auth.type: <your-auth-type>
# PATH TO GOOGLE AUTHENTICATION JSON FILE
gs.auth.service.account.json.keyfile: <your-json-keyfile>
```
