use std::{fmt::Debug, pin::Pin, time::Duration};

use ethers::types::{Filter, Log, U256};
use jsonrpsee::{
    core::{client::ClientT, params::BatchRequestBuilder},
    http_client::{HttpClient, HttpClientBuilder},
};
use serde_json::Value;
use thiserror::Error;

use crate::types::Update;

#[derive(Error, Debug)]
pub enum ScannerError {
    #[error("could not connect to rpc url {rpc_url}")]
    Connection {
        rpc_url: String,
        source: jsonrpsee::core::Error,
    },
    #[error("could not create new logs filter to stream changes")]
    NewFilter(jsonrpsee::core::Error),
    #[error("could not batch rpc call with method {method}")]
    AddCallToBatch {
        method: String,
        source: jsonrpsee::core::Error,
    },
    #[error("could get new logs filter update")]
    FilterUpdate(jsonrpsee::core::Error),
    #[error("inconsistent number of rpc responses")]
    InconsistentResponses,
    #[error("some or all responses errored out")]
    Responses,
    #[error("error deserializing response")]
    Deserialize(serde_json::error::Error),
}

pub struct Scanner {
    client: HttpClient,
    interval: Duration,
    filter: Filter,
}

impl Scanner {
    pub fn new(rpc_url: String, interval: Duration, filter: Filter) -> Result<Self, ScannerError> {
        Ok(Self {
            client: HttpClientBuilder::new()
                .build(rpc_url.clone())
                .map_err(|err| ScannerError::Connection {
                    rpc_url,
                    source: err,
                })?,
            interval,
            filter,
        })
    }

    pub async fn stream(
        self,
    ) -> Result<
        Pin<Box<impl futures::Stream<Item = Result<Vec<Update>, ScannerError>>>>,
        ScannerError,
    > {
        let filter_id = self
            .client
            .request::<U256, Vec<Filter>>("eth_newFilter", vec![self.filter])
            .await
            .map_err(|err| ScannerError::NewFilter(err))?;

        let interval = self.interval.clone();
        let stream = futures::stream::unfold(Vec::<Update>::new(), move |_| {
            let mut interval = tokio::time::interval(interval);
            let filter_id = filter_id.clone();
            let client = self.client.clone();

            async move {
                interval.tick().await;

                const ETH_GET_BLOCK_NUMBER_METHOD: &str = "eth_blockNumber";
                const ETH_GET_FILTER_CHANGES_METHOD: &str = "eth_getFilterChanges";

                let mut batched_requests_builder = BatchRequestBuilder::new();

                match batched_requests_builder
                    .insert(ETH_GET_BLOCK_NUMBER_METHOD, Vec::<String>::new())
                {
                    Err(err) => {
                        return Some((
                            Err(ScannerError::AddCallToBatch {
                                method: ETH_GET_BLOCK_NUMBER_METHOD.to_owned(),
                                source: err,
                            }),
                            vec![],
                        ))
                    }
                    Ok(_) => {}
                }

                match batched_requests_builder
                    .insert(ETH_GET_FILTER_CHANGES_METHOD, vec![filter_id])
                {
                    Err(err) => {
                        return Some((
                            Err(ScannerError::AddCallToBatch {
                                method: ETH_GET_FILTER_CHANGES_METHOD.to_owned(),
                                source: err,
                            }),
                            vec![],
                        ))
                    }
                    Ok(_) => {}
                };

                let responses = match client
                    .batch_request::<Value>(batched_requests_builder)
                    .await
                {
                    Err(err) => return Some((Err(ScannerError::FilterUpdate(err)), vec![])),
                    Ok(res) => res,
                };
                if responses.len() != 2 {
                    return Some((Err(ScannerError::InconsistentResponses), vec![]));
                }

                let mut responses = match responses.into_ok() {
                    Err(_) => return Some((Err(ScannerError::Responses), vec![])),
                    Ok(res) => res,
                };

                let block_number = match serde_json::from_value::<U256>(responses.nth(0).unwrap()) {
                    Err(err) => return Some((Err(ScannerError::Deserialize(err)), vec![])),
                    Ok(res) => res,
                };

                let logs = match serde_json::from_value::<Vec<Log>>(responses.nth(0).unwrap()) {
                    Err(err) => return Some((Err(ScannerError::Deserialize(err)), vec![])),
                    Ok(res) => res,
                };

                let mut updates = logs
                    .into_iter()
                    .map(|log| Update::NewLog(log))
                    .collect::<Vec<Update>>();
                updates.push(Update::NewBlock(block_number.as_u64()));

                Some((Ok(updates), vec![]))
            }
        });

        Ok(Box::pin(stream))
    }
}
