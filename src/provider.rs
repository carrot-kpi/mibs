use std::sync::Arc;

use ethers::providers::{Http, Middleware, Provider as EthersProvider};

use crate::{Error, commons::Provider};

pub async fn get_provider(url: String, expected_chain_id: u64) -> Result<Arc<Provider>, Error> {
    let provider =
        Arc::new(
            EthersProvider::<Http>::try_from(url).map_err(|_| Error::ProviderConnection {
                chain_id: expected_chain_id,
            })?,
        );

    let chain_id_from_provider = provider
        .get_chainid()
        .await
        .map_err(|_| Error::ProviderChainId {
            chain_id: expected_chain_id,
        })?
        .as_u64();

    if chain_id_from_provider != expected_chain_id {
        Err(Error::ProviderChainIdMismatch {
            from_provider: chain_id_from_provider,
            expected: expected_chain_id,
        })
    } else {
        Ok(provider)
    }
}
