use std::{fmt::Debug, pin::Pin, time::Duration};

use async_stream::try_stream;
use ethers::types::{BlockNumber, Filter, Log, U64};
use futures::Stream;
use jsonrpsee::{
    core::{client::ClientT, params::BatchRequestBuilder},
    http_client::{HttpClient, HttpClientBuilder},
    types::ErrorObject,
};
use serde_json::Value;
use thiserror::Error;
use tokio::time::Interval;

use crate::types::Update;

#[derive(Error, Debug)]
pub enum ScannerError {
    #[error("could not connect to rpc url {rpc_url}: {source:#}")]
    Connection {
        rpc_url: String,
        source: jsonrpsee::core::Error,
    },
    #[error("could not batch rpc call with method {method}: {source:#}")]
    AddCallToBatch {
        method: String,
        #[source]
        source: jsonrpsee::core::Error,
    },
    #[error("could get new logs filter update: {0:#}")]
    FilterUpdate(#[source] jsonrpsee::core::Error),
    #[error("inconsistent number of rpc responses")]
    InconsistentResponses,
    #[error("{method} call failed: ({}: {})", .source.code(), .source.message())]
    Response {
        method: String,
        #[source]
        source: ErrorObject<'static>,
    },
    #[error("error deserializing response: {0:#}")]
    Deserialize(#[source] serde_json::error::Error),
}

pub struct Scanner {
    client: HttpClient,
    interval: Interval,
    from_block_number: U64,
    filter: Filter,
}

impl Scanner {
    pub fn new(
        rpc_url: String,
        interval: Duration,
        from_block_number: U64,
        filter: Filter,
    ) -> Result<Self, ScannerError> {
        Ok(Self {
            client: HttpClientBuilder::new()
                .build(rpc_url.clone())
                .map_err(|err| ScannerError::Connection {
                    rpc_url,
                    source: err,
                })?,
            interval: tokio::time::interval(interval),
            from_block_number,
            filter,
        })
    }

    pub fn stream(mut self) -> Pin<Box<impl Stream<Item = Result<Vec<Update>, ScannerError>>>> {
        let stream = try_stream! {
            loop {
                self.interval.tick().await;

                let filter = self.filter.clone().from_block(self.from_block_number).to_block(BlockNumber::Latest);
                tracing::debug!("fetching new logs from block {:?} to {:?}", filter.get_from_block(), filter.get_to_block());

                const ETH_GET_BLOCK_NUMBER_METHOD: &str = "eth_blockNumber";
                const ETH_GET_LOGS_METHOD: &str = "eth_getLogs";

                let mut batched_requests_builder = BatchRequestBuilder::new();

                batched_requests_builder
                    .insert(ETH_GET_BLOCK_NUMBER_METHOD, Vec::<String>::new()).map_err(|err| ScannerError::AddCallToBatch {
                        method: ETH_GET_BLOCK_NUMBER_METHOD.to_owned(),
                        source: err,
                    })?;

                batched_requests_builder
                    .insert(ETH_GET_LOGS_METHOD, vec![filter]).map_err(|err| ScannerError::AddCallToBatch {
                        method: ETH_GET_LOGS_METHOD.to_owned(),
                        source: err,
                    })?;

                let responses = self.client
                    .batch_request::<Value>(batched_requests_builder)
                    .await.map_err(|err| ScannerError::FilterUpdate(err))?;
                if responses.len() != 2 {
                    Err(ScannerError::InconsistentResponses)?;
                }

                let mut responses = responses.into_iter();

                let next_response = responses.next().unwrap().map_err(|err| ScannerError::Response {
                    method: ETH_GET_BLOCK_NUMBER_METHOD.to_owned(),
                    source: err.into_owned(),
                })?;

                let block_number = serde_json::from_value::<U64>(next_response).map_err(|err| ScannerError::Deserialize(err))?;

                self.from_block_number = block_number;
                tracing::debug!("from block number for next loop iteration updated to {}", self.from_block_number);

                let next_response = responses.next().unwrap().map_err(|err| ScannerError::Response {
                    method: ETH_GET_LOGS_METHOD.to_owned(),
                    source: err.into_owned(),
                })?;

                let logs = serde_json::from_value::<Vec<Log>>(next_response).map_err(|err| ScannerError::Deserialize(err))?;

                let mut updates = logs
                    .into_iter()
                    .map(|log| Update::NewLog(log))
                    .collect::<Vec<Update>>();
                updates.push(Update::NewBlock(block_number.as_u64()));

                yield updates;
            }
        };

        Box::pin(stream)
    }
}
