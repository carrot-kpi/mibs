use async_trait::async_trait;
use ethers::{providers::ProviderError, types::Log};
use thiserror::Error;
use tokio::task::JoinError;

use crate::chain_config::ChainConfig;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error joining past and present scanning tasks for chain {chain_id}")]
    PresentPastJoin { chain_id: u64, source: JoinError },
    #[error("error joining chain scanning tasks")]
    ChainsJoin(#[from] JoinError),
    #[error(
        "error sending checkpoint updates ownership transfer message from past to present scanner"
    )]
    CheckpointUpdatesOwnershipTransfer,
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

#[async_trait]
pub trait Listener {
    async fn on_event(&self, config: &ChainConfig, log: Log);
}
