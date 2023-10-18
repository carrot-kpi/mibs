use std::sync::Arc;

use async_trait::async_trait;
use ethers::{
    providers::{Http, Provider as EthersProvider, ProviderError},
    types::Log,
};
use thiserror::Error;
use tokio::task::JoinError;

use crate::chain_config::ChainConfig;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error joining past and present scanning tasks for chain {chain_id}")]
    PresentPastJoin { chain_id: u64, source: JoinError },
    #[error("error joining chain scanning tasks")]
    ChainsJoin(#[from] JoinError),
    #[error("chain id mismatch, provider gave {from_provider} while {expected} was expected")]
    ProviderChainIdMismatch { from_provider: u64, expected: u64 },
    #[error("could not get provider for chain {chain_id}")]
    ProviderConnection { chain_id: u64 },
    #[error("could not get chain id from provider for chain {chain_id}")]
    ProviderChainId { chain_id: u64 },
    #[error("could not get current block number for chain {chain_id}")]
    BlockNumber {
        chain_id: u64,
        source: ProviderError,
    },
}

pub const DEFAULT_PAST_EVENTS_QUERY_RANGE: u64 = 5_000;
pub const DEFAULT_PRESENT_EVENTS_POLLING_INTERVAL_SECONDS: u64 = 60;

pub type Config = Vec<ChainConfig>;
pub type Provider = EthersProvider<Http>;

#[async_trait]
pub trait Listener {
    async fn on_past_event(
        &self,
        provider: Arc<Provider>,
        chain_config: &ChainConfig,
        range_from_block: u64,
        range_to_block: u64,
        log: Log,
    );

    async fn on_past_events_finished(&self, provider: Arc<Provider>, chain_config: &ChainConfig);

    async fn on_present_event(&self, provider: Arc<Provider>, chain_config: &ChainConfig, log: Log);
}
