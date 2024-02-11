pub mod chain_config;
mod scanner;
pub mod types;

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use backoff::ExponentialBackoffBuilder;
use chain_config::ChainConfig;
use ethers::providers::{Http, Middleware, Provider, ProviderError};
use futures::StreamExt;
use governor::{Quota, RateLimiter};
use thiserror::Error;
use tokio::task::{JoinError, JoinSet};
use tracing_futures::Instrument;
use types::{Config, Listener};

use crate::{scanner::Scanner, types::Update};

#[derive(Error, Debug)]
pub enum Error {
    #[error("error joining past and present scanning tasks for chain {chain_id}: {source:?}")]
    PresentPastJoin { chain_id: u64, source: JoinError },
    #[error("error joining chain scanning tasks: {0:?}")]
    ChainsJoin(#[source] JoinError),
    #[error("chain id mismatch, provider gave {from_provider} while {expected} was expected")]
    ProviderChainIdMismatch { from_provider: u64, expected: u64 },
    #[error("could not get provider for chain {chain_id}")]
    ProviderConnection { chain_id: u64 },
    #[error("could not get remote chain id from provider for chain {chain_id}: {source:?}")]
    ProviderChainId {
        chain_id: u64,
        #[source]
        source: ProviderError,
    },
    #[error("could not get current block number for chain {chain_id}: {source:?}")]
    BlockNumber {
        chain_id: u64,
        #[source]
        source: ProviderError,
    },
}

pub struct Mibs<L: Listener + Send> {
    config: Config<L>,
}

impl<L: Listener + Send + Sync + 'static> Mibs<L> {
    pub fn builder() -> MibsBuilder<L>
    where
        L: Listener,
    {
        MibsBuilder::new()
    }

    pub async fn scan(self) -> Result<(), Error> {
        let mut join_set = JoinSet::new();
        for chain_config in self.config.into_iter() {
            let chain_id = chain_config.chain_id;
            tracing::info!("setting up listener for chain with id {}", chain_id);

            let remote_chain_id = chain_config
                .provider
                .get_chainid()
                .await
                .map_err(|err| Error::ProviderChainId {
                    chain_id,
                    source: err,
                })?
                .as_u64();
            if remote_chain_id != chain_id {
                return Err(Error::ProviderChainIdMismatch {
                    from_provider: remote_chain_id,
                    expected: chain_id,
                });
            }

            join_set.spawn(Self::scan_chain(Arc::new(chain_config)));
        }

        // wait forever unless some task stops with an error
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(result) => {
                    if let Err(error) = result {
                        return Err(error);
                    }
                }
                Err(err) => {
                    return Err(Error::ChainsJoin(err));
                }
            }
        }

        Ok(())
    }

    async fn scan_chain(chain_config: Arc<ChainConfig<L>>) -> Result<(), Error> {
        let chain_id = chain_config.chain_id;

        let provider = chain_config.provider.clone();
        let block_number = provider
            .get_block_number()
            .await
            .map_err(|err| Error::BlockNumber {
                chain_id,
                source: err,
            })?
            .as_u64();

        let mut join_set = JoinSet::new();

        let present_scanner = Self::scan_present(chain_config.clone(), block_number)
            .instrument(tracing::info_span!("present-scanner", chain_id));
        join_set.spawn(present_scanner);

        if !chain_config.skip_past {
            let past_scanner = Self::scan_past(chain_config.clone(), provider, block_number)
                .instrument(tracing::info_span!("past-scanner", chain_id));
            join_set.spawn(past_scanner);
        }

        // wait forever unless some task stops with an error
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(result) => {
                    if let Err(error) = result {
                        return Err(error);
                    }
                }
                Err(err) => {
                    return Err(Error::PresentPastJoin {
                        chain_id,
                        source: err,
                    });
                }
            }
        }

        Ok(())
    }

    async fn scan_past(
        chain_config: Arc<ChainConfig<L>>,
        provider: Arc<Provider<Http>>,
        current_block_number: u64,
    ) -> Result<(), Error> {
        tracing::debug!("current block number: {current_block_number}");

        let mut from_block = if current_block_number < chain_config.checkpoint_block {
            tracing::warn!(
                "had to adjust initial past scanning block from the given checkpoint {} to {}",
                chain_config.checkpoint_block,
                current_block_number
            );
            current_block_number
        } else {
            chain_config.checkpoint_block
        };
        tracing::debug!("chain checkpoint block number: {from_block}");

        let full_range = current_block_number - from_block;
        tracing::debug!(
            "full range calculated from current block number and checkpoint block: {full_range}"
        );

        let chunk_size = chain_config.past_events_query_range;

        if full_range == 0 {
            tracing::info!("no past blocks to scan, skipping");

            let mut locked_listener = chain_config.listener.lock().await;

            let update = Update::PastBatchCompleted {
                from_block,
                to_block: from_block,
            };
            tracing::debug!(
                "sending past batch completed update event to listener: {:?}",
                update
            );
            locked_listener.on_update(update).await;
            tracing::debug!("past batch completed update event successfully sent to listener");

            tracing::debug!("sending past scanning completed update event to listener");
            locked_listener
                .on_update(Update::PastScanningCompleted)
                .await;
            tracing::debug!("past scanning completed update event successfully sent to listener");

            return Ok(());
        }

        tracing::info!(
            "analyzing {} past blocks {} at a time",
            current_block_number - from_block,
            chunk_size
        );

        let rate_limiter =
            if let Some(past_events_query_max_rps) = chain_config.past_events_query_max_rps {
                Some(RateLimiter::direct(Quota::per_second(
                    NonZeroU32::new(past_events_query_max_rps).unwrap(),
                )))
            } else {
                None
            };

        loop {
            let to_block = if from_block + chunk_size > current_block_number {
                current_block_number
            } else {
                from_block + chunk_size
            };

            let filter = chain_config
                .events_filter
                .clone()
                .from_block(from_block)
                .to_block(to_block);

            // apply rate limiting if necessary
            if let Some(rate_limiter) = &rate_limiter {
                rate_limiter.until_ready().await;
            }

            let fetch_logs = || async {
                provider
                    .get_logs(&filter)
                    .await
                    .map_err(|err| backoff::Error::Transient {
                        err,
                        retry_after: None,
                    })
            };

            let logs = match backoff::future::retry(
                ExponentialBackoffBuilder::new()
                    .with_max_elapsed_time(Some(Duration::from_secs(30)))
                    .build(),
                fetch_logs,
            )
            .await
            {
                Ok(logs) => logs,
                Err(error) => {
                    tracing::error!(
                        "error fetching logs from block {} to {}: {:?}",
                        from_block,
                        to_block,
                        error
                    );
                    continue;
                }
            };

            for log in logs.into_iter() {
                let update = Update::NewLog(log);
                tracing::debug!("sending new log update event to listener: {:?}", update);
                chain_config.listener.lock().await.on_update(update).await;
                tracing::debug!("new log update event successfully sent to listener");
            }

            let update = Update::PastBatchCompleted {
                from_block,
                to_block,
            };
            tracing::debug!(
                "sending past batch completed event to listener: {:?}",
                update
            );
            chain_config.listener.lock().await.on_update(update).await;
            tracing::debug!("past batch completed event successfully sent to listener");

            if to_block == current_block_number {
                break;
            }
            from_block = to_block + 1;
        }

        tracing::debug!("sending past scanning completed update event to listener");
        chain_config
            .listener
            .lock()
            .await
            .on_update(Update::PastScanningCompleted)
            .await;
        tracing::debug!("past scanning completed update event successfully sent to listener");

        Ok(())
    }

    async fn scan_present(
        chain_config: Arc<ChainConfig<L>>,
        block_number: u64,
    ) -> Result<(), Error> {
        let mut backoff_duration = Duration::from_secs(1);

        // this is used in case an error happens and the loop is once again invoked after
        // the stream is "broken". If that happens let's say 8 hours into the indexer operations,
        // the from_block_number passed to the scanner constructor will be too outdated and trigger
        // a range too wide error. we solve this by keeping an updated from block number in memory
        // and updating it at each stream update so that if anything happens the scanner will be
        // reinstantiated with a reasonably up to date from block number (unless something catastrophic
        // happens)
        let mut from_block_number = block_number;

        loop {
            tracing::info!("watching present logs");

            let mut stream = match Scanner::new(
                chain_config.provider.clone(),
                chain_config.present_events_polling_interval,
                from_block_number,
                chain_config.events_filter.clone(),
            ) {
                Ok(scanner) => {
                    backoff_duration = Duration::from_secs(1);
                    scanner.stream()
                }
                Err(error) => {
                    tracing::error!(
                        "could not get on-chain scanner, retrying after {}s backoff: {:?}",
                        backoff_duration.as_secs(),
                        error
                    );
                    tokio::time::sleep(backoff_duration).await;
                    backoff_duration = backoff_duration * 2;
                    continue;
                }
            };

            while let Some(updates_result) = stream.next().await {
                match updates_result {
                    Ok((updates, latest_block_number)) => {
                        backoff_duration = Duration::from_secs(1);
                        let mut locked_listener = chain_config.listener.lock().await;
                        for update in updates.into_iter() {
                            tracing::debug!("sending update to listener: {update:?}");
                            locked_listener.on_update(update).await;
                            tracing::debug!("update sent to listener");
                        }
                        from_block_number = latest_block_number;
                    }
                    Err(err) => {
                        tokio::time::sleep(backoff_duration).await;
                        backoff_duration = backoff_duration * 2;
                        tracing::error!(
                            "error while scanning, retrying after {:?}s backoff: {:?}",
                            backoff_duration,
                            err
                        );
                    }
                }
            }
        }
    }
}

pub struct MibsBuilder<L: Listener + Send + Sync + 'static> {
    config: Config<L>,
}

impl<L: Listener + Send + Sync + 'static> MibsBuilder<L> {
    pub fn new() -> Self {
        Self { config: vec![] }
    }

    pub fn build(self) -> Mibs<L> {
        Mibs {
            config: self.config,
        }
    }

    pub fn chain_config(mut self, config: ChainConfig<L>) -> Self {
        self.config.push(config);
        self
    }
}
