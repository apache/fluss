// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use prost::DecodeError;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ReadError {
    #[error("Cannot read data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Negative message size: {size}")]
    NegativeMessageSize { size: i32 },

    #[error("Message too large, limit is {limit} bytes but got {actual} bytes")]
    MessageTooLarge { limit: usize, actual: usize },

    #[error("Fail to decode error response: {0}")]
    ProtoErrorResponseDecodeError(#[from] DecodeError),
}

pub trait AsyncMessageRead {
    fn read_message(
        &mut self,
        max_message_size: usize,
    ) -> impl Future<Output = Result<Vec<u8>, ReadError>> + Send;
}

impl<R> AsyncMessageRead for R
where
    R: AsyncRead + Send + Unpin,
{
    async fn read_message(&mut self, max_message_size: usize) -> Result<Vec<u8>, ReadError> {
        let mut len_buf = [0u8; 4];
        self.read_exact(&mut len_buf).await?;
        let len = i32::from_be_bytes(len_buf);

        let len = usize::try_from(len).map_err(|_| ReadError::NegativeMessageSize { size: len })?;
        // check max message size to not blow up memory
        if len > max_message_size {
            // We need to seek so that next message is readable. However `self.seek` would require `R: AsyncSeek` which
            // doesn't hold for many types we want to work with. So do some manual seeking.
            let mut to_read = len;
            let mut buf = vec![]; // allocate empty buffer
            while to_read > 0 {
                let step = max_message_size.min(to_read);

                // resize buffer if required
                buf.resize(step, 0);

                self.read_exact(&mut buf).await?;
                to_read -= step;
            }

            return Err(ReadError::MessageTooLarge {
                limit: max_message_size,
                actual: len,
            });
        }

        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
}

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum WriteError {
    #[error("Cannot write data: {0}")]
    IO(#[from] std::io::Error),

    #[error("Message too large: {size}")]
    TooLarge { size: usize },
}

pub trait AsyncMessageWrite {
    fn write_message(&mut self, msg: &[u8]) -> impl Future<Output = Result<(), WriteError>> + Send;
}

impl<W> AsyncMessageWrite for W
where
    W: AsyncWrite + Send + Unpin,
{
    async fn write_message(&mut self, msg: &[u8]) -> Result<(), WriteError> {
        let len = i32::try_from(msg.len()).map_err(|_| WriteError::TooLarge { size: msg.len() })?;
        self.write_all(len.to_be_bytes().as_ref()).await?;

        if !msg.is_empty() {
            self.write_all(msg).await?;
        }
        Ok(())
    }
}
