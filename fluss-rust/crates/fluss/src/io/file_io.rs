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

use crate::error::*;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use jiff::Timestamp;
use opendal::Operator;

use url::Url;

use super::Storage;

use crate::error::Result;

#[derive(Clone, Debug)]
pub struct FileIO {
    storage: Arc<Storage>,
}

impl FileIO {
    /// Try to infer file io scheme from path.
    pub fn from_url(path: &str) -> Result<FileIOBuilder> {
        let url = Url::parse(path).map_err(|e| Error::IllegalArgument {
            message: format!("Invalid URL '{path}': {e}"),
        })?;
        Ok(FileIOBuilder::new(url.scheme()))
    }

    /// Create a new input file to read data.
    pub fn new_input(&self, path: &str) -> Result<InputFile> {
        let (op, relative_path) = self.storage.create(path)?;
        let path = path.to_string();
        let relative_path_pos = path.len() - relative_path.len();
        Ok(InputFile {
            op,
            path,
            relative_path_pos,
        })
    }
}

#[derive(Debug)]
pub struct FileIOBuilder {
    scheme_str: Option<String>,
    props: HashMap<String, String>,
}

impl FileIOBuilder {
    pub fn new(scheme_str: impl ToString) -> Self {
        Self {
            scheme_str: Some(scheme_str.to_string()),
            props: HashMap::default(),
        }
    }

    pub(crate) fn into_parts(self) -> (String, HashMap<String, String>) {
        (self.scheme_str.unwrap_or_default(), self.props)
    }

    pub fn with_prop(mut self, key: impl ToString, value: impl ToString) -> Self {
        self.props.insert(key.to_string(), value.to_string());
        self
    }

    pub fn with_props(
        mut self,
        args: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        self.props
            .extend(args.into_iter().map(|e| (e.0.to_string(), e.1.to_string())));
        self
    }

    pub fn build(self) -> Result<FileIO> {
        let storage = Storage::build(self)?;
        Ok(FileIO {
            storage: Arc::new(storage),
        })
    }
}

pub trait FileRead: Send + Unpin + 'static {
    fn read(&self, range: Range<u64>) -> impl Future<Output = Result<Bytes>> + Send;
}

impl FileRead for opendal::Reader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(self, range).await?.to_bytes())
    }
}

#[derive(Debug)]
pub struct InputFile {
    op: Operator,
    path: String,
    relative_path_pos: usize,
}

impl InputFile {
    pub fn location(&self) -> &str {
        &self.path
    }

    pub async fn exists(&self) -> Result<bool> {
        Ok(self.op.exists(&self.path[self.relative_path_pos..]).await?)
    }

    pub async fn metadata(&self) -> Result<FileStatus> {
        let meta = self.op.stat(&self.path[self.relative_path_pos..]).await?;

        Ok(FileStatus {
            size: meta.content_length(),
            is_dir: meta.is_dir(),
            path: self.path.clone(),
            last_modified: meta.last_modified().map(Into::into),
        })
    }

    pub async fn read(&self) -> Result<Bytes> {
        Ok(self
            .op
            .read(&self.path[self.relative_path_pos..])
            .await?
            .to_bytes())
    }

    pub async fn reader(&self) -> Result<impl FileRead> {
        Ok(self.op.reader(&self.path[self.relative_path_pos..]).await?)
    }
}

#[derive(Clone, Debug)]
pub struct FileStatus {
    pub size: u64,
    pub is_dir: bool,
    pub path: String,
    pub last_modified: Option<Timestamp>,
}
