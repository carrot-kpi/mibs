use std::{num::NonZeroU32, sync::Arc};

use ethers::{middleware::Middleware, providers::StreamExt};
use governor::{Quota, RateLimiter};
use tokio::{
    sync::{oneshot, Mutex},
    task::JoinSet,
};
use tracing::info_span;
use tracing_futures::Instrument;

use crate::{
    chain_config::ChainConfig,
    commons::{Error, Listener},
    provider::get_provider,
};

pub async fn scan<L: Listener + Send + Sync + Sync + 'static>(
    listener: Arc<L>,
    chain_config: Arc<ChainConfig>,
) -> Result<(), Error> {
    // When the past scanner is running it has control over the checkpoint block number update.
    // This control is passed over to the present scanner only when the past scanner is finished
    // in order to avoid creating holes in the indexing history. In short: the checkpoint block number
    // must always be the minimum block number possible between what the present and past scanners
    // are currently analyzing in order to avoid outcomes resulting in a wrong history when the
    // service is restarted.
    // This dynamic is handled with a oneshot channel. When the past scanner has finished processing
    // its share, it communicates it to the present scanner which can then take over the checkpoint
    // block number updates.
    let (tx, rx) = oneshot::channel();

    let chain_id = chain_config.id;

    let present_scanner = scan_present(rx, listener.clone(), chain_config.clone())
        .instrument(tracing::info_span!("present-scanner", chain_id));

    let past_scanner = scan_past(tx, listener.clone(), chain_config.clone())
        .instrument(tracing::info_span!("past-scanner", chain_id));

    let mut join_set = JoinSet::new();
    join_set.spawn(present_scanner);
    join_set.spawn(past_scanner);

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

async fn scan_past<L: Listener + Send + Sync>(
    checkpoint_ownership_update_sender: oneshot::Sender<bool>,
    listener: Arc<L>,
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
        })?
        .as_u64();

    let initial_block = chain_config.checkpoint_block;
    let mut from_block = initial_block;
    let full_range = block_number - initial_block;
    let chunk_size = chain_config.past_events_query_range;

    if full_range == 0 {
        tracing::info!("no past blocks to scan, skipping");
        yield_checkpoint_updates_ownership(checkpoint_ownership_update_sender)?;
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
            // let locked = listener.lock().await;
            listener.on_event(&chain_config, log).await;
        }

        tracing::info!(
            "{} -> {} - scanned {}% of past blocks",
            from_block,
            to_block,
            ((to_block as f32 - initial_block as f32) / full_range as f32) * 100f32
        );

        if to_block == block_number {
            break;
        }
        from_block = to_block + 1;
    }

    yield_checkpoint_updates_ownership(checkpoint_ownership_update_sender)?;

    tracing::info!(
        "finished scanning past blocks, checkpoint updates ownership transferred to present scanner"
    );

    Ok(())
}

fn yield_checkpoint_updates_ownership(
    checkpoint_ownership_update_sender: oneshot::Sender<bool>,
) -> Result<(), Error> {
    checkpoint_ownership_update_sender
        .send(true)
        .map_err(|_| Error::CheckpointUpdatesOwnershipTransfer)
}

async fn scan_present<L: Listener + Send + Sync>(
    checkpoint_ownership_update_receiver: oneshot::Receiver<bool>,
    listener: Arc<L>,
    chain_config: Arc<ChainConfig>,
) -> Result<(), Error> {
    let update_snapshot_block_number = Arc::new(Mutex::new(false));
    tokio::spawn(
        message_receiver(
            checkpoint_ownership_update_receiver,
            update_snapshot_block_number.clone(),
        )
        .instrument(info_span!("message-receiver")),
    );

    let logs_polling_interval_seconds = chain_config.present_events_polling_interval;

    loop {
        tracing::info!("watching present logs");

        let chain_id = chain_config.id;
        let provider = get_provider(chain_config.rpc_url.clone(), chain_id).await?;
        let mut stream = match provider.watch(&chain_config.events_filter).await {
            Ok(watcher) => watcher.interval(logs_polling_interval_seconds).stream(),
            Err(error) => {
                tracing::error!("could not get events filter watcher: {:#}", error);
                continue;
            }
        };

        while let Some(log) = stream.next().await {
            // let locked = listener.lock().await;
            listener.on_event(&chain_config, log).await;
        }
    }
}

async fn message_receiver(
    receiver: oneshot::Receiver<bool>,
    update_snapshot_block_number: Arc<Mutex<bool>>,
) {
    match receiver.await {
        Ok(value) => {
            if !value {
                panic!("snapshot updates ownership channel receiver received a false value: this should never happen");
            } else {
                *update_snapshot_block_number.lock().await = value;
                tracing::info!("snapshot updates ownership taken");
            }
        }
        Err(error) => {
            tracing::error!("error while receiving control over snapshot block number update from past scanner\n\n{:#}", error);
        }
    }
}
