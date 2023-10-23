use async_trait::async_trait;
use ethers::types::Log;

use crate::chain_config::ChainConfig;

pub enum Update {
    NewBlock(u64),
    NewLog(Log),
    PastBatchCompleted {
        from_block: u64,
        to_block: u64,
        progress_percentage: f32,
    },
}

pub type Config<L> = Vec<ChainConfig<L>>;

#[async_trait]
pub trait Listener {
    async fn on_update(&mut self, update: Update);
}
