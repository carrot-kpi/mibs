use std::{collections::HashMap, fmt::Debug, pin::Pin, sync::Arc, time::Duration};

use async_stream::try_stream;
use backoff::ExponentialBackoffBuilder;
use ethers::{
    middleware::Middleware,
    providers::{Http, Provider, ProviderError},
    types::{Filter, Log},
};
use futures::Stream;
use thiserror::Error;
use tokio::{sync::Mutex, time::Interval};

use crate::types::Update;

const STARTING_BACKOFF_DELAY: Duration = Duration::from_secs(1);
const MAX_BACKOFF_DELAY: Duration = Duration::from_secs(8);
const BACKOFF_FACTOR: f64 = 2.0;

#[derive(Error, Debug)]
pub enum ScannerError {
    #[error("could not get block number: {0}:?")]
    GetBlockNumber(#[source] ProviderError),
    #[error("inconsistent block number fetched: {0} is less than {1}")]
    InconsistentBlockNumber(u64, u64),
    #[error("could not get logs: {0:?}")]
    GetLogs(#[source] ProviderError),
    #[error("error creating log key: no {0}")]
    LogKeyCreation(String),
}
pub struct Scanner {
    provider: Arc<Provider<Http>>,
    interval: Interval,
    from_block_number: u64,
    previous_logs: HashMap<Vec<u8>, Log>,
    base_filter: Filter,
}

impl Scanner {
    pub fn new(
        provider: Arc<Provider<Http>>,
        interval: Duration,
        from_block_number: u64,
        base_filter: Filter,
    ) -> Result<Self, ScannerError> {
        Ok(Self {
            provider: provider.clone(),
            previous_logs: HashMap::new(),
            interval: tokio::time::interval(interval),
            from_block_number: from_block_number,
            base_filter,
        })
    }

    pub fn stream(
        mut self,
    ) -> Pin<Box<impl Stream<Item = Result<(Vec<Update>, u64), ScannerError>>>> {
        let stream = try_stream! {
            loop {
                self.interval.tick().await;

                let backoff = ExponentialBackoffBuilder::new()
                    .with_initial_interval(STARTING_BACKOFF_DELAY)
                    .with_max_elapsed_time(Some(MAX_BACKOFF_DELAY))
                    .with_multiplier(BACKOFF_FACTOR)
                    .build();

                let this = Arc::new(Mutex::new(&mut self));
                let fetch = || async {
                    this.lock().await.fetch_current_block_number_and_logs_update().await.map_err(|err| {
                        tracing::warn!("error while fetching the current block number and log updates, retrying: {:?}", err);
                        backoff::Error::Transient {
                            err,
                            retry_after: None,
                        }
                    })
                };

                let (block_number, logs) = match backoff::future::retry(backoff, fetch).await {
                    Ok(logs) => logs,
                    Err(error) => {
                        tracing::error!(
                            "error fetching current block number and logs update: {:?}",
                            error
                        );
                        continue;
                    }
                };

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

                updates.push(Update::NewBlock(block_number));

                yield (updates, block_number);
            }
        };

        Box::pin(stream)
    }

    async fn fetch_current_block_number_and_logs_update(
        &mut self,
    ) -> Result<(u64, Vec<Log>), ScannerError> {
        let block_number = self
            .provider
            .get_block_number()
            .await
            .map_err(|err| ScannerError::GetBlockNumber(err))?
            .as_u64();
        tracing::debug!("latest block number: {:?}", block_number);

        let from_block_number = self.from_block_number;
        if block_number < from_block_number {
            Err(ScannerError::InconsistentBlockNumber(
                block_number,
                from_block_number,
            ))?
        }

        tracing::debug!(
            "updating filter from block number to {:?} and to block number to {:?}",
            from_block_number,
            block_number
        );
        let filter = self
            .base_filter
            .clone()
            .from_block(from_block_number)
            .to_block(block_number);

        tracing::debug!(
            "fetching new logs from block {:?} to {:?}",
            filter.get_from_block(),
            filter.get_to_block()
        );
        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .map_err(|err| ScannerError::GetLogs(err))?;

        tracing::debug!("updating from block number to {:?}", block_number);
        self.from_block_number = block_number;

        Ok((block_number, logs))
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
