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

# Fluss Helm Chart

This chart deploys an Apache Fluss cluster on Kubernetes, following Helm best practices.

## Development

For how to build your local Fluss image and use it in Minikube refer to the
[official documentation](https://fluss.apache.org/docs/next/install-deploy/deploying-with-helm/#installation).

Refer to the [official documentation](https://fluss.apache.org/docs/next/install-deploy/deploying-with-helm/#configuration-parameters)
as well for configuration values.

We use the [`helm-unittest`](https://github.com/helm-unittest/helm-unittest) plugin for testing Fluss Helm charts.  
You can run tests locally via:

```bash
# From the /helm folder:
docker run -ti --rm -v $(pwd):/apps helmunittest/helm-unittest .
```

## Contributing

Follow the [development section](#development) for local development.

Every contribution should add a unit test to the [tests folder](tests).

Whether relevant, update the [official documentation](../website/docs/install-deploy/deploying-with-helm.md).
