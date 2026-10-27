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

# Apache Fluss Helm charts

| Chart | Deploys | Deployment guide |
| --- | --- | --- |
| [fluss](fluss/) | Coordinator and Tablet servers | [Deploy Fluss](../website/docs/install-deploy/deploying-with-helm.md) |
| [fluss-gateway](fluss-gateway/) | REST Gateway connected to an existing Fluss cluster | [Deploy Gateway](../website/docs/install-deploy/deploying-gateway.md#run-on-kubernetes-with-helm) |

The charts are independent Helm releases, each with its own values, installation,
upgrade, and removal. Installing Gateway does not install Fluss servers.
The `helm/` directory is a collection of charts, not an installable chart.

When installing from a source checkout, select the chart directory:

```bash
# Run from the repository root; prepare values using the deployment guides.
helm install fluss ./helm/fluss -f fluss-values.yaml
helm install fluss-gateway ./helm/fluss-gateway -f gateway-values.yaml
```

## Development

Both charts use the [helm-unittest](https://github.com/helm-unittest/helm-unittest)
plugin. After installing it, run the same checks used by CI from the repository root:

```bash
for chart in fluss fluss-gateway; do
  helm lint --strict "./helm/${chart}"
  helm unittest "./helm/${chart}"
  helm package "./helm/${chart}" --destination /tmp
done
```

Keep chart-specific tests in each chart's `tests/` directory and update its
deployment guide when behavior changes. The shared
[release script](../tools/releasing/create_helm_packages.sh) packages both charts
as separate archives with a common release version.
