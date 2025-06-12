---
title: Exposing a Fluss Cluster to External Clients
sidebar_position: 2
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

# Exposing a Fluss Cluster to External Clients (using `advertised.listeners`)

Clients must be able to connect to **all** CoordinatorServer and TabletServer instances of a Fluss cluster.
This means that potential host names of all Fluss cluster components must be resolvable, and the IP addresses must routable from the machine where the client is running on.

While there is no additional setup needed when clients are part of the same subnet as the Fluss cluster components, exposing a Fluss cluster to clients that are part of a different subnet ("external clients") requires additional configuration.
This includes clients that are running on

- the _same host_ but are part of a different subnet.   
    Example: Fluss cluster components are deployed with Docker and are all part of an internal Docker network. The client is running directly on the host machine.
- a _different host_ that is part of a different subnet.   
    Example: Fluss cluster components are deployed in the cloud. The client is running on a different host and connects to the Fluss cluster over the public internet.

In the following you can find

- [A Primer on Fluss Networking](#a-primer-on-fluss-networking), which illustrates why additional configuration for external clients is needed. Furthermore, the most common networking configuration options are explained.
- An example how to [Connect to a Fluss Cluster Deployed with Docker](#connect-to-a-fluss-cluster-deployed-with-docker), which shows the necessary steps to connect a client that is running directly on the host to a Fluss cluster that is deployed with Docker on the _same host_.

:::note
We do not give an example how to expose a Fluss cluster to external clients that are running on a _different host_ that is part of a different subnet, because the configuration highly depends on the used environment (on-premise, cloud provider). 
However, the underlying principles explained in the following sections remain the same.
:::


## A Primer on Fluss Networking

...

## Connect to a Fluss Cluster Deployed with Docker

...
