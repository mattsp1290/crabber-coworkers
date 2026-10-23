// Shared fixtures are used in different combinations by each integration test binary.
#![allow(dead_code)]

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
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, crabber::providers::ProviderBindingError>
    {
        crabber::core::ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "crabber-coworkers.pending",
            "1",
            crabber::InputFingerprint::new(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
            .expect("constant digest is valid"),
            crabber::core::ProviderBindingLimits {
                context_tokens: None,
                output_tokens: None,
            },
        )
        .map_err(|_| crabber::providers::ProviderBindingError::InvalidDescriptor)
    }

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

pub fn never() -> crabber_coworkers::ask::Cancel {
    Box::pin(std::future::pending())
}

/// Write an isolated example config with an explicit model-turn limit.
pub fn write_state_config(
    directory: &std::path::Path,
    model_turn_seconds: u64,
) -> std::path::PathBuf {
    let mut config: serde_json::Value =
        serde_json::from_str(include_str!("../../../../config.example.json")).unwrap();
    config["state_dir"] = serde_json::json!(directory.join("state"));
    config["limits"]["model_turn_seconds"] = serde_json::json!(model_turn_seconds);
    let path = directory.join("config.json");
    std::fs::write(&path, config.to_string()).unwrap();
    path
}

pub mod slack;
