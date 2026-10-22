use async_trait::async_trait;
use crabber::{
    Selection, StreamDelta,
    providers::{DeltaStream, ModelRequest, ProviderError, ProviderErrorKind, Resolver, Streamer},
};
use std::sync::Arc;

pub fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
pub fn failure(retryable: bool) -> StreamDelta {
    StreamDelta::Error(ProviderError {
        kind: if retryable {
            ProviderErrorKind::Transport
        } else {
            ProviderErrorKind::Server
        },
        retryable,
        message: "secret-marker".into(),
    })
}

pub struct Pending;
#[async_trait]
impl Resolver for Pending {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(Self))
    }
}
#[async_trait]
impl Streamer for Pending {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        Ok(Box::pin(futures::stream::pending()))
    }
}
