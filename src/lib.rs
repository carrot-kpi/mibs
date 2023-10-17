pub mod chain_config;
pub mod commons;
mod provider;
mod scanner;

use std::sync::Arc;

use chain_config::ChainConfig;
use commons::{Config, Error, Listener};
use thiserror::Error;
use tokio::task::JoinSet;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub struct Scanner<L: Listener + Send> {
    config: Config,
    listener: Arc<L>,
}

impl<L: Listener + Send + Sync + 'static> Scanner<L> {
    pub fn builder(listener: L) -> ScannerBuilder<L>
    where
        L: Listener,
    {
        ScannerBuilder::new(listener)
    }

    pub async fn scan(self) -> Result<(), Error> {
        let subscriber = FmtSubscriber::builder()
            .json()
            .with_span_list(true)
            .with_current_span(false)
            .with_env_filter(
                EnvFilter::builder()
                    .with_default_directive(LevelFilter::INFO.into())
                    .from_env()
                    .map_err(|err| Error::LogLevel(err))?,
            )
            .with_ansi(true)
            .finish();
        tracing::subscriber::set_global_default(subscriber).map_err(|err| Error::Tracing(err))?;

        let mut join_set = JoinSet::new();
        for chain_config in self.config.into_iter() {
            let rpc_endpoint = chain_config.rpc_url.as_str();

            let chain_id = chain_config.id;
            tracing::info!(
                "setting up listener for chain with id {} with rpc endpoint: {}",
                chain_id,
                rpc_endpoint
            );

            join_set.spawn(scanner::scan(self.listener.clone(), Arc::new(chain_config)));
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
}

pub struct ScannerBuilder<L: Listener + Send + Sync + 'static> {
    config: Config,
    listener: L,
}

impl<L: Listener + Send + Sync + 'static> ScannerBuilder<L> {
    pub fn new(listener: L) -> Self {
        Self {
            config: vec![],
            listener,
        }
    }

    pub fn build(self) -> Scanner<L> {
        Scanner {
            config: self.config,
            listener: Arc::new(self.listener),
        }
    }

    pub fn chain_config(mut self, config: ChainConfig) -> Self {
        self.config.push(config);
        self
    }
}
