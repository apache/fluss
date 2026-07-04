/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod file_io;

pub use file_io::*;

mod storage;
pub use storage::*;

#[cfg(feature = "storage-fs")]
mod storage_fs;
#[cfg(feature = "storage-fs")]
use storage_fs::*;

#[cfg(feature = "storage-memory")]
mod storage_memory;
#[cfg(feature = "storage-memory")]
use storage_memory::*;

#[cfg(feature = "storage-s3")]
mod storage_s3;
#[cfg(feature = "storage-s3")]
use storage_s3::*;

#[cfg(feature = "storage-oss")]
mod storage_oss;
#[cfg(feature = "storage-oss")]
use storage_oss::*;
