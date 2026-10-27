<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Fluss Gateway Helm chart

Deploys Gateway independently against an existing Fluss cluster. Requires
Helm 3.8 or newer, Kubernetes 1.19 or newer, and a Gateway image available to
the cluster nodes.

See the [deployment guide](../../website/docs/install-deploy/deploying-gateway.md#run-on-kubernetes-with-helm)
for image preparation, backend credentials, monitoring, and upgrades.
[values.yaml](values.yaml) lists the chart options.

Create `gateway-values.yaml` as described in the guide, then run from the
repository root:

```bash
helm upgrade --install fluss-gateway ./helm/fluss-gateway \
  --namespace fluss --create-namespace \
  -f gateway-values.yaml --wait --timeout 5m
```

See the [shared development instructions](../README.md#development) for linting,
testing, and packaging both charts.
