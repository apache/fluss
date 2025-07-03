---
sidebar_label: Overview
title: Tiered Storage
sidebar_position: 1
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

# Overview

With tiered storage, Fluss allows you to scale compute and storage resources independently, provides better client isolation, and
allows faster maintenance.

Fluss organizes data into different storage layers based on its access patterns, performance requirements, and cost considerations.

Fluss ensures the recent data is stored in local for higher write/read performance and the historical data is stored in [remote storage](remote-storage.md) for lower cost.

What's more, since the native format of Fluss's data is optimized for real-time write/read which is inevitable unfriendly to batch analytics, Fluss also introduces a [lakehouse storage](lakehouse-storage.md) which stores the data
in the well-known open data lake format for better analytics performance. Currently, only Paimon is supported, but more kinds of data lake support are on the way. Keep eyes on us!

The overall tiered storage architecture is shown in the following diagram:

<img width="600px" src={require('../../assets/tiered-storage.png').default} />
