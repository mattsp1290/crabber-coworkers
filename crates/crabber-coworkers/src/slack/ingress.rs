use crate::config::{SlackConfig, valid_id};
use serde_json::Value;

pub struct Identity {
    pub team_id: String,
    pub user_id: String,
    pub bot_id: String,
}

#[derive(Debug)]
pub struct Mention {
    pub event_id: String,
    pub channel: String,
    pub root_ts: String,
    pub user: String,
    pub text: String,
}

#[derive(Debug)]
pub enum Classified {
    Mention(Mention),
    Ignore,
}

#[must_use]
pub fn classify(config: &SlackConfig, identity: &Identity, payload: &Value) -> Classified {
    let event = &payload["event"];
    if payload["type"] != "event_callback"
        || payload["team_id"] != config.team_id
        || event["type"] != "app_mention"
        || event.get("bot_id").is_some()
        || event.get("subtype").is_some()
        || event["user"] == identity.user_id
    {
        return Classified::Ignore;
    }
    let (Some(channel), Some(user), Some(ts), Some(event_id)) = (
        event["channel"].as_str(),
        event["user"].as_str(),
        event["ts"].as_str(),
        payload["event_id"].as_str(),
    ) else {
        return Classified::Ignore;
    };
    if !config.allowed_channel_ids.iter().any(|v| v == channel)
        || !config.allowed_user_ids.iter().any(|v| v == user)
        || ts.is_empty()
        || event_id.is_empty()
    {
        return Classified::Ignore;
    }
    let root_ts = event["thread_ts"]
        .as_str()
        .filter(|v| !v.is_empty())
        .unwrap_or(ts);
    Classified::Mention(Mention {
        event_id: event_id.into(),
        channel: channel.into(),
        root_ts: root_ts.into(),
        user: user.into(),
        text: strip_mentions(event["text"].as_str().unwrap_or(""), &identity.user_id),
    })
}

#[must_use]
pub fn conversation_name(channel: &str, root_ts: &str) -> Option<String> {
    let name = format!(
        "slack-{}-{}",
        channel.to_ascii_lowercase(),
        root_ts.replace('.', "-")
    );
    valid_id(&name).then_some(name)
}

#[must_use]
pub fn strip_mentions(text: &str, bot_user_id: &str) -> String {
    let mut remaining = text;
    let mut stripped = String::new();
    // Scan actual Slack tokens, leaving malformed or other users' mentions intact.
    while let Some(start) = remaining.find("<@") {
        stripped.push_str(&remaining[..start]);
        let candidate = &remaining[start..];
        let Some(end) = candidate.find('>') else {
            remaining = candidate;
            break;
        };
        let token = &candidate[2..end];
        if token.split('|').next() != Some(bot_user_id) {
            stripped.push_str(&candidate[..=end]);
        }
        remaining = &candidate[end + 1..];
    }
    stripped.push_str(remaining);
    stripped
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .trim()
        .into()
}
