pub mod chain_config;
mod scanner;
pub mod types;
mod utils;

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use chain_config::ChainConfig;
use ethers::{
    providers::{Middleware, ProviderError},
    types::U64,
};
use futures::StreamExt;
use governor::{Quota, RateLimiter};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinSet},
};
use tracing_futures::Instrument;
use types::{Config, Listener, Provider};

use crate::{scanner::Scanner, types::Update, utils::get_provider};

#[derive(Error, Debug)]
pub enum Error {
    #[error("error joining past and present scanning tasks for chain {chain_id}: {source:#}")]
    PresentPastJoin { chain_id: u64, source: JoinError },
    #[error("error joining chain scanning tasks: {0:#}")]
    ChainsJoin(#[source] JoinError),
    #[error("chain id mismatch, provider gave {from_provider} while {expected} was expected")]
    ProviderChainIdMismatch { from_provider: u64, expected: u64 },
    #[error("could not get provider for chain {chain_id}")]
    ProviderConnection { chain_id: u64 },
    #[error("could not get chain id from provider for chain {chain_id}")]
    ProviderChainId { chain_id: u64 },
    #[error("could not get current block number for chain {chain_id}: {source:#}")]
    BlockNumber {
        chain_id: u64,
        #[source]
        source: ProviderError,
    },
}

pub struct Mibs<L: Listener + Send> {
    config: Config,
    listener: Arc<Mutex<L>>,
}

impl<L: Listener + Send + Sync + 'static> Mibs<L> {
    pub fn builder(listener: L) -> MibsBuilder<L>
    where
        L: Listener,
    {
        MibsBuilder::new(listener)
    }

    pub async fn scan(self) -> Result<(), Error> {
        let mut join_set = JoinSet::new();
        for chain_config in self.config.into_iter() {
            let rpc_endpoint = chain_config.rpc_url.as_str();

            let chain_id = chain_config.id;
            tracing::info!(
                "setting up listener for chain with id {} with rpc endpoint: {}",
                chain_id,
                rpc_endpoint
            );

            join_set.spawn(Self::scan_chain(
                self.listener.clone(),
                Arc::new(chain_config),
            ));
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

    async fn scan_chain(
        listener: Arc<Mutex<L>>,
        chain_config: Arc<ChainConfig>,
    ) -> Result<(), Error> {
        let chain_id = chain_config.id;

        let provider = get_provider(chain_config.rpc_url.clone(), chain_id).await?;
        let block_number = provider
            .get_block_number()
            .await
            .map_err(|err| Error::BlockNumber {
                chain_id,
                source: err,
            })?;

        let mut join_set = JoinSet::new();

        let present_scanner =
            Self::scan_present(chain_config.clone(), block_number, listener.clone())
                .instrument(tracing::info_span!("present-scanner", chain_id));
        join_set.spawn(present_scanner);

        if !chain_config.skip_past {
            let past_scanner = Self::scan_past(
                chain_config.clone(),
                provider,
                block_number,
                listener.clone(),
            )
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
        chain_config: Arc<ChainConfig>,
        provider: Arc<Provider>,
        block_number: U64,
        listener: Arc<Mutex<L>>,
    ) -> Result<(), Error> {
        let block_number = block_number.as_u64();

        let initial_block = chain_config.checkpoint_block;
        let mut from_block = initial_block;
        let full_range = block_number - initial_block;
        let chunk_size = chain_config.past_events_query_range;

        if full_range == 0 {
            tracing::info!("no past blocks to scan, skipping");
            listener
                .lock()
                .await
                .on_update(
                    provider.clone(),
                    &chain_config,
                    Update::PastBatchCompleted {
                        from_block,
                        to_block: from_block,
                        progress_percentage: 100f32,
                    },
                )
                .await;
            return Ok(());
        }

        tracing::info!(
            "pinning from {} past blocks, analyzing {} blocks at a time",
            block_number - from_block,
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
            let to_block = if from_block + chunk_size > block_number {
                block_number
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

            let logs = match provider.get_logs(&filter).await {
                Ok(logs) => logs,
                Err(error) => {
                    tracing::error!(
                        "error fetching logs from block {} to {}: {:#}",
                        from_block,
                        to_block,
                        error
                    );
                    continue;
                }
            };

            for log in logs.into_iter() {
                listener
                    .lock()
                    .await
                    .on_update(provider.clone(), &chain_config, Update::NewLog(log))
                    .await;
            }

            listener
                .lock()
                .await
                .on_update(
                    provider.clone(),
                    &chain_config,
                    Update::PastBatchCompleted {
                        from_block,
                        to_block,
                        progress_percentage: ((to_block as f32 - initial_block as f32)
                            / full_range as f32)
                            * 100f32,
                    },
                )
                .await;

            if to_block == block_number {
                break;
            }
            from_block = to_block + 1;
        }

        Ok(())
    }

    async fn scan_present(
        chain_config: Arc<ChainConfig>,
        block_number: U64,
        listener: Arc<Mutex<L>>,
    ) -> Result<(), Error> {
        let mut backoff_duration = Duration::from_secs(1);

        loop {
            tracing::info!("watching present logs");

            let chain_id = chain_config.id;
            let provider = get_provider(chain_config.rpc_url.clone(), chain_id).await?;
            let mut stream = match Scanner::new(
                chain_config.rpc_url.clone(),
                chain_config.present_events_polling_interval,
                block_number,
                chain_config.events_filter.clone(),
            ) {
                Ok(scanner) => scanner.stream(),
                Err(error) => {
                    tracing::error!(
                        "could not get onchain scanner, retrying after {}s backoff: {:#}",
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
                    Ok(logs) => {
                        for log in logs.into_iter() {
                            listener
                                .lock()
                                .await
                                .on_update(provider.clone(), &chain_config, log)
                                .await;
                        }
                    }
                    Err(err) => {
                        tracing::error!("error while scanning: {:#}", err);
                    }
                }
            }
        }
    }
}

pub struct MibsBuilder<L: Listener + Send + Sync + 'static> {
    config: Config,
    listener: L,
}

impl<L: Listener + Send + Sync + 'static> MibsBuilder<L> {
    pub fn new(listener: L) -> Self {
        Self {
            config: vec![],
            listener,
        }
    }

    pub fn build(self) -> Mibs<L> {
        Mibs {
            config: self.config,
            listener: Arc::new(Mutex::new(self.listener)),
        }
    }

    pub fn chain_config(mut self, config: ChainConfig) -> Self {
        self.config.push(config);
        self
    }
}
