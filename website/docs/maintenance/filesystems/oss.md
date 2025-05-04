---
title: Aliyun OSS
sidebar_position: 3
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Aliyun OSS

## OSS: Object Storage Service 

[Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (Aliyun OSS) is widely used, particularly popular among China’s cloud users, and it provides cloud object storage for a variety of use cases.


## Configurations setup

To enabled OSS as remote storage, there are some required configurations that must be add to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: oss://<your-bucket>/path/to/remote/storage
# Aliyun OSS endpoint to connect to, such as: oss-cn-hangzhou.aliyuncs.com
fs.oss.endpoint: <your-endpoint>
# Aliyun access key ID
fs.oss.accessKeyId: <your-access-key>
# Aliyun access key secret
fs.oss.accessKeySecret: <your-secret-key>

# Aliyun STS endpoint to connect to obtain a STS token, such as: sts.cn-hangzhou.aliyuncs.com
fs.oss.sts.endpoint: <your-sts-endpoint>
# For the role of the STS token obtained from the STS endpoint, such as: acs:ram::123456789012:role/testrole
fs.oss.roleArn: <your-role-arn>
```

## Token-based Authentication

For client to access the remote storage such as reading snapshot or tiered log, client must obtain a STS token from Fluss cluster. So you must
configure `fs.oss.sts.endpoint` and `fs.oss.roleArn`.
`fs.oss.sts.endpoint` is the STS endpoint to obtain a STS token, such as `sts.cn-hangzhou.aliyuncs.com` for hangzhou region, you can 
find different endpoints for different regions in [Aliyun STS Endpoint](https://help.aliyun.com/zh/ram/developer-reference/api-sts-2015-04-01-endpoint).
`fs.oss.roleArn` is for the role of the STS token obtained from the STS endpoint, it should be in the format of `acs:ram::<aliyun-account-id>:role/<role-name>`, 
such as `acs:ram::123456789012:role/testrole`. Since client will use the STS token to read the remote storage, the role must be granted with the read permission of the remote storage. 
See more detail in [AssumeRole](https://help.aliyun.com/zh/ram/developer-reference/api-sts-2015-04-01-assumerole).

Apart from the above configurations, you can also define the configuration keys mentioned in the [Hadoop OSS documentation](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html)
in the Fluss' `server.yaml`. These configurations defines in Hadoop OSS documentation are advanced configurations which are usually used by performance tuning.

