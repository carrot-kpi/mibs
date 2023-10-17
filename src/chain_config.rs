use std::time::Duration;

use ethers::types::Filter;

use crate::{commons::{Listener, DEFAULT_PAST_EVENTS_QUERY_RANGE, DEFAULT_PRESENT_EVENTS_POLLING_INTERVAL_SECONDS}, ScannerBuilder};

pub struct ChainConfig {
    pub id: u64,
    pub rpc_url: String,
    pub checkpoint_block: u64,
    pub events_filter: Filter,
    pub past_events_query_range: u64,
    pub past_events_query_max_rps: Option<u32>,
    pub present_events_polling_interval: Duration,
}

pub struct ChainConfigBuilder<L: Listener + Send + Sync + 'static> {
    scanner_builder: ScannerBuilder<L>,

    id: u64,
    rpc_url: String,
    checkpoint_block: u64,
    events_filter: Filter,
    past_events_query_range: Option<u64>,
    past_events_query_max_rps: Option<u32>,
    present_events_polling_interval: Option<Duration>,
}

impl<L: Listener + Send + Sync + 'static> ChainConfigBuilder<L> {
    pub fn new(
        scanner_builder: ScannerBuilder<L>,
        id: u64,
        rpc_url: String,
        checkpoint_block: u64,
        events_filter: Filter,
    ) -> Self {
        Self {
            scanner_builder: scanner_builder,

            id,
            rpc_url,
            checkpoint_block,
            events_filter,
            past_events_query_range: None,
            past_events_query_max_rps: None,
            present_events_polling_interval: None,
        }
    }

    pub fn add(mut self) -> ScannerBuilder<L> {
        self.scanner_builder.config.push(ChainConfig {
            id: self.id,
            rpc_url: self.rpc_url,
            checkpoint_block: self.checkpoint_block,
            events_filter: self.events_filter,
            past_events_query_range: self
                .past_events_query_range
                .unwrap_or(DEFAULT_PAST_EVENTS_QUERY_RANGE),
            past_events_query_max_rps: self.past_events_query_max_rps,
            present_events_polling_interval: self.present_events_polling_interval.unwrap_or(
                Duration::from_secs(DEFAULT_PRESENT_EVENTS_POLLING_INTERVAL_SECONDS),
            ),
        });
        self.scanner_builder
    }

    pub fn past_events_query_range(mut self, past_events_query_range: u64) -> Self {
        self.past_events_query_range = Some(past_events_query_range);
        self
    }

    pub fn past_events_query_max_rps(mut self, past_events_query_max_rps: u32) -> Self {
        self.past_events_query_max_rps = Some(past_events_query_max_rps);
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