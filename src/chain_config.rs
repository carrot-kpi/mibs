use std::{sync::Arc, time::Duration};

use ethers::{
    providers::{Http, Provider},
    types::Filter,
};
use tokio::sync::Mutex;

use crate::types::Listener;

const DEFAULT_PAST_EVENTS_QUERY_RANGE: u64 = 5_000;
const DEFAULT_RPC_REQUESTS_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PRESENT_EVENTS_POLLING_INTERVAL: Duration = Duration::from_secs(60);

pub struct ChainConfig<L: Listener> {
    pub chain_id: u64,
    pub provider: Arc<Provider<Http>>,
    pub checkpoint_block: u64,
    pub events_filter: Filter,
    pub skip_past: bool,
    pub past_events_query_range: u64,
    pub past_events_query_max_rps: Option<u32>,
    pub rpc_requests_timeout: Duration,
    pub present_events_polling_interval: Duration,
    pub listener: Arc<Mutex<L>>,
}

impl<L: Listener> ChainConfig<L> {
    pub fn builder(
        chain_id: u64,
        provider: Arc<Provider<Http>>,
        checkpoint_block: u64,
        events_filter: Filter,
        listener: L,
    ) -> ChainConfigBuilder<L> {
        ChainConfigBuilder::new(
            chain_id,
            provider,
            checkpoint_block,
            events_filter,
            listener,
        )
    }
}

pub struct ChainConfigBuilder<L: Listener> {
    chain_id: u64,
    provider: Arc<Provider<Http>>,
    checkpoint_block: u64,
    events_filter: Filter,
    skip_past: Option<bool>,
    past_events_query_range: Option<u64>,
    past_events_query_max_rps: Option<u32>,
    rpc_requests_timeout: Option<Duration>,
    present_events_polling_interval: Option<Duration>,
    listener: L,
}

impl<L: Listener> ChainConfigBuilder<L> {
    pub fn new(
        chain_id: u64,
        provider: Arc<Provider<Http>>,
        checkpoint_block: u64,
        events_filter: Filter,
        listener: L,
    ) -> Self {
        Self {
            chain_id,
            provider,
            checkpoint_block,
            events_filter,
            skip_past: None,
            past_events_query_range: None,
            past_events_query_max_rps: None,
            rpc_requests_timeout: None,
            present_events_polling_interval: None,
            listener,
        }
    }

    pub fn build(self) -> ChainConfig<L> {
        ChainConfig {
            chain_id: self.chain_id,
            provider: self.provider,
            checkpoint_block: self.checkpoint_block,
            events_filter: self.events_filter,
            skip_past: self.skip_past.unwrap_or(false),
            past_events_query_range: self
                .past_events_query_range
                .unwrap_or(DEFAULT_PAST_EVENTS_QUERY_RANGE),
            past_events_query_max_rps: self.past_events_query_max_rps,
            rpc_requests_timeout: self
                .rpc_requests_timeout
                .unwrap_or(DEFAULT_RPC_REQUESTS_TIMEOUT),
            present_events_polling_interval: self
                .present_events_polling_interval
                .unwrap_or(DEFAULT_PRESENT_EVENTS_POLLING_INTERVAL),
            listener: Arc::new(Mutex::new(self.listener)),
        }
    }

    pub fn skip_past(mut self, skip_past: Option<bool>) -> Self {
        self.skip_past = skip_past;
        self
    }

    pub fn past_events_query_range(mut self, past_events_query_range: Option<u64>) -> Self {
        self.past_events_query_range = past_events_query_range;
        self
    }

    pub fn past_events_query_max_rps(mut self, past_events_query_max_rps: Option<u32>) -> Self {
        self.past_events_query_max_rps = past_events_query_max_rps;
        self
    }

    pub fn present_events_polling_interval(
        mut self,
        present_events_polling_interval: Duration,
    ) -> Self {
        self.present_events_polling_interval = Some(present_events_polling_interval);
        self
    }
}
