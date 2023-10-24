use std::{collections::HashMap, fmt::Debug, pin::Pin, sync::Arc, time::Duration};

use async_stream::try_stream;
use backoff::ExponentialBackoffBuilder;
use ethers::{
    providers::{Http, Provider},
    types::{BlockNumber, Filter, Log, U64},
};
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

const ETH_GET_BLOCK_NUMBER_METHOD: &str = "eth_blockNumber";
const ETH_GET_LOGS_METHOD: &str = "eth_getLogs";

#[derive(Error, Debug)]
pub enum ScannerError {
    #[error("could not connect to rpc url {rpc_url}: {source:?}")]
    Connection {
        rpc_url: url::Url,
        source: jsonrpsee::core::Error,
    },
    #[error("could not batch rpc call with method {method}: {source:?}")]
    AddCallToBatch {
        method: String,
        #[source]
        source: jsonrpsee::core::Error,
    },
    #[error("could get new logs filter update: {0:?}")]
    FilterUpdate(#[source] jsonrpsee::core::Error),
    #[error("inconsistent number of rpc responses")]
    InconsistentResponses,
    #[error("{method} call failed: ({}: {})", .source.code(), .source.message())]
    Response {
        method: String,
        #[source]
        source: ErrorObject<'static>,
    },
    #[error("error deserializing response: {0:?}")]
    Deserialize(#[source] serde_json::error::Error),
    #[error("error creating log key: no {0}")]
    LogKeyCreation(String),
}
pub struct Scanner {
    client: HttpClient,
    interval: Interval,
    from_block_number: U64,
    previous_logs: HashMap<Vec<u8>, Log>,
    filter: Filter,
}

impl Scanner {
    pub fn new(
        provider: Arc<Provider<Http>>,
        interval: Duration,
        from_block_number: U64,
        filter: Filter,
    ) -> Result<Self, ScannerError> {
        let rpc_url = provider.url();

        Ok(Self {
            client: HttpClientBuilder::new().build(rpc_url).map_err(|err| {
                ScannerError::Connection {
                    rpc_url: rpc_url.clone(),
                    source: err,
                }
            })?,
            previous_logs: HashMap::new(),
            interval: tokio::time::interval(interval),
            from_block_number,
            filter,
        })
    }

    pub fn stream(mut self) -> Pin<Box<impl Stream<Item = Result<Vec<Update>, ScannerError>>>> {
        let stream = try_stream! {
            loop {
                self.interval.tick().await;

                let perform = || async {
                    let filter = self.filter.clone().from_block(self.from_block_number).to_block(BlockNumber::Latest);
                    tracing::debug!("fetching new logs from block {:?} to {:?}", filter.get_from_block(), filter.get_to_block());

                    let mut batched_requests_builder = BatchRequestBuilder::new();

                    batched_requests_builder
                        .insert(ETH_GET_BLOCK_NUMBER_METHOD, Vec::<String>::new()).map_err(|err| backoff::Error::Transient {
                            err: ScannerError::AddCallToBatch {
                                method: ETH_GET_BLOCK_NUMBER_METHOD.to_owned(),
                                source: err,
                            },
                            retry_after: None,
                        })?;

                    batched_requests_builder
                        .insert(ETH_GET_LOGS_METHOD, vec![filter]).map_err(|err| backoff::Error::Transient {
                            err: ScannerError::AddCallToBatch {
                                method: ETH_GET_LOGS_METHOD.to_owned(),
                                source: err,
                            },
                            retry_after: None,
                        })?;

                    self.client
                    .batch_request::<Value>(batched_requests_builder)
                    .await.map_err(|err| {backoff::Error::Transient {
                        err: ScannerError::FilterUpdate(err),
                        retry_after: None,
                    }})
                };

                let responses = backoff::future::retry(
                    ExponentialBackoffBuilder::new()
                        .with_max_elapsed_time(Some(
                            self.interval.period() / 2,
                        ))
                        .build(),
                        perform,
                )
                .await?;

                if responses.len() != 2 {
                    Err(ScannerError::InconsistentResponses)?;
                }

                let mut responses = responses.into_iter();

                let next_response = responses.next().unwrap().map_err(|err| ScannerError::Response {
                    method: ETH_GET_BLOCK_NUMBER_METHOD.to_owned(),
                    source: err.into_owned(),
                })?;

                let block_number = serde_json::from_value::<U64>(next_response).map_err(|err| ScannerError::Deserialize(err))?;

                self.from_block_number = block_number + 1;
                tracing::debug!("from block number for next loop iteration updated to {}", self.from_block_number);

                let next_response = responses.next().unwrap().map_err(|err| ScannerError::Response {
                    method: ETH_GET_LOGS_METHOD.to_owned(),
                    source: err.into_owned(),
                })?;

                let logs = serde_json::from_value::<Vec<Log>>(next_response).map_err(|err| ScannerError::Deserialize(err))?;
                let mut updates = vec![];
                let mut new_previous_logs = HashMap::new();
                for log in logs.into_iter() {
                    let log_hash = log_hash(&log)?;
                    if !self.previous_logs.contains_key(&log_hash) {
                        updates.push(Update::NewLog(log.clone()));
                        new_previous_logs.insert(log_hash, log);
                    }
                }
                self.previous_logs = new_previous_logs;

                updates.push(Update::NewBlock(block_number.as_u64()));

                yield updates;
            }
        };

        Box::pin(stream)
    }
}

fn log_hash(log: &Log) -> Result<Vec<u8>, ScannerError> {
    let address = log.address;

    let block_hash = if let Some(block_hash) = log.block_hash {
        block_hash
    } else {
        return Err(ScannerError::LogKeyCreation("blockHash".to_owned()));
    };

    let log_index = if let Some(log_index) = log.log_index {
        log_index
    } else {
        return Err(ScannerError::LogKeyCreation("logIndex".to_owned()));
    };

    let mut out = vec![];
    out.extend_from_slice(address.as_bytes());
    out.extend_from_slice(block_hash.as_bytes());

    let mut log_index_bytes: [u8; 32] = [0; 32];
    log_index.to_big_endian(&mut log_index_bytes);
    out.extend_from_slice(log_index_bytes.as_slice());

    Ok(out)
}

#[cfg(test)]
mod test {
    use ethers::types::{Log, H256};

    use super::log_hash;

    #[test]
    fn log_hash_block_hash_none() {
        let log = Log {
            ..Default::default()
        };
        let error = log_hash(&log).unwrap_err();
        assert_eq!(error.to_string(), "error creating log key: no blockHash",);
    }

    #[test]
    fn log_hash_log_index_none() {
        let log = Log {
            block_hash: Some(H256::random()),
            ..Default::default()
        };
        let error = log_hash(&log).unwrap_err();
        assert_eq!(error.to_string(), "error creating log key: no logIndex",);
    }
}
