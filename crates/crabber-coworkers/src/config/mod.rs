pub mod constants;
use constants::{
    DEFAULT_CONCURRENT_TURNS, DEFAULT_PROMPT_BYTES, DEFAULT_TURN_SECONDS, MAX_ALLOWED_CHANNELS,
    MAX_ALLOWED_USERS, MAX_CONCURRENT_TURNS, MAX_CONFIG_BYTES, MAX_COWORKERS, MAX_ID_BYTES,
    MAX_INSTRUCTIONS_BYTES, MAX_NAME_BYTES, MAX_PROMPT_BYTES, MAX_ROLE_BYTES, MAX_SLACK_ID_BYTES,
    MAX_STATE_DIR_BYTES, MAX_TURN_SECONDS,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub state_dir: PathBuf,
    pub coworkers: Vec<CoworkerDefinition>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub borb: Option<BorbConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerDefinition {
    pub id: String,
    pub name: String,
    pub role: String,
    pub instructions: String,
    #[serde(default)]
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlackConfig {
    pub team_id: String,
    pub allowed_channel_ids: Vec<String>,
    pub allowed_user_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BorbConfig {
    pub url: String,
    pub name_prefix: String,
    pub image: String,
    pub model_secret: String,
}

impl BorbConfig {
    fn validate(&self, coworkers: &[CoworkerDefinition]) -> Result<(), ConfigError> {
        if coworkers
            .iter()
            .any(|def| self.name_prefix.len() + 1 + def.id.len() > 40)
        {
            return Err(ConfigError(
                "coworker id and borb.name_prefix must fit within 40 bytes".into(),
            ));
        }
        let url = url::Url::parse(&self.url).map_err(|_| ConfigError("invalid borb.url".into()))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ConfigError("invalid borb.url".into()));
        }
        if !valid_id(&self.name_prefix) || self.name_prefix.len() > 24 {
            return Err(ConfigError("invalid borb.name_prefix".into()));
        }
        let Some((repository, digest)) = self.image.split_once("@sha256:") else {
            return Err(ConfigError("borb.image must use a sha256 digest".into()));
        };
        if repository.is_empty()
            || repository.chars().any(char::is_whitespace)
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(ConfigError("borb.image must use a sha256 digest".into()));
        }
        if !valid_id(&self.model_secret) || self.model_secret.len() > 63 {
            return Err(ConfigError("invalid borb.model_secret".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_prompt_bytes: usize,
    pub model_turn_seconds: u64,
    pub max_concurrent_turns: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_prompt_bytes: DEFAULT_PROMPT_BYTES,
            model_turn_seconds: DEFAULT_TURN_SECONDS,
            max_concurrent_turns: DEFAULT_CONCURRENT_TURNS,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(String);

impl Config {
    #[must_use]
    pub fn coworker(&self, id: &str) -> Option<&CoworkerDefinition> {
        self.coworkers.iter().find(|coworker| coworker.id == id)
    }

    pub fn slack_coworkers(&self) -> impl Iterator<Item = &CoworkerDefinition> {
        self.coworkers.iter().filter(|def| def.slack.is_some())
    }

    /// # Errors
    /// Returns fixed diagnostics when configuration values are invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(borb) = &self.borb {
            borb.validate(&self.coworkers)?;
        }
        let mut errors = Vec::new();
        let state_dir = self.state_dir.to_string_lossy();
        if state_dir.trim().is_empty() || state_dir.len() > MAX_STATE_DIR_BYTES {
            errors.push(format!(
                "state_dir must be non-empty and at most {MAX_STATE_DIR_BYTES} bytes"
            ));
        }
        if state_dir.chars().any(char::is_control) {
            errors.push("state_dir must not contain control characters".into());
        }
        if !(1..=MAX_COWORKERS).contains(&self.coworkers.len()) {
            errors.push(format!("coworkers must contain 1–{MAX_COWORKERS} entries"));
        }
        let mut ids = HashSet::new();
        for (i, def) in self.coworkers.iter().enumerate() {
            if !valid_id(&def.id) {
                errors.push(format!("coworkers[{i}].id must be lowercase alphanumeric groups separated by single hyphens, at most {MAX_ID_BYTES} bytes"));
            }
            if !ids.insert(&def.id) {
                errors.push(format!("coworkers[{i}].id must be unique"));
            }
            let fields = [
                ("name", &def.name, MAX_NAME_BYTES, true),
                ("role", &def.role, MAX_ROLE_BYTES, true),
                (
                    "instructions",
                    &def.instructions,
                    MAX_INSTRUCTIONS_BYTES,
                    false,
                ),
            ];
            // Keep control-character diagnostics before length/blankness diagnostics.
            for (field, value, _, single_line) in &fields {
                if *single_line && value.chars().any(char::is_control) {
                    errors.push(format!(
                        "coworkers[{i}].{field} must not contain control characters"
                    ));
                }
            }
            for (field, value, cap, _) in fields {
                if value.trim().is_empty() || value.len() > cap {
                    errors.push(format!(
                        "coworkers[{i}].{field} must be non-empty and at most {cap} bytes"
                    ));
                }
            }
            if let Some(slack) = &def.slack {
                if !valid_slack_id(&slack.team_id, b"T") {
                    errors.push(format!(
                        "coworkers[{i}].slack.team_id must be a Slack team ID"
                    ));
                }
                for (field, values, prefixes, cap, kind) in [
                    (
                        "allowed_channel_ids",
                        &slack.allowed_channel_ids,
                        &b"CG"[..],
                        MAX_ALLOWED_CHANNELS,
                        "channel",
                    ),
                    (
                        "allowed_user_ids",
                        &slack.allowed_user_ids,
                        &b"UW"[..],
                        MAX_ALLOWED_USERS,
                        "user",
                    ),
                ] {
                    if values.is_empty()
                        || values.len() > cap
                        || values.iter().any(|v| !valid_slack_id(v, prefixes))
                        || values.iter().collect::<HashSet<_>>().len() != values.len()
                    {
                        errors.push(format!("coworkers[{i}].slack.{field} must contain 1–{cap} unique Slack {kind} IDs"));
                    }
                }
            }
        }
        if !(1..=MAX_CONCURRENT_TURNS).contains(&self.limits.max_concurrent_turns) {
            errors.push("limits.max_concurrent_turns must be in 1..=32".into());
        }
        if !(1..=MAX_PROMPT_BYTES).contains(&self.limits.max_prompt_bytes) {
            errors.push(format!(
                "limits.max_prompt_bytes must be in 1..={MAX_PROMPT_BYTES}"
            ));
        }
        if !(1..=MAX_TURN_SECONDS).contains(&self.limits.model_turn_seconds) {
            errors.push(format!(
                "limits.model_turn_seconds must be in 1..={MAX_TURN_SECONDS}"
            ));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError(errors.join("\n")))
        }
    }
}

#[must_use]
pub fn valid_slack_id(id: &str, prefixes: &[u8]) -> bool {
    (2..=MAX_SLACK_ID_BYTES).contains(&id.len())
        && prefixes.contains(&id.as_bytes()[0])
        && id
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

#[must_use]
pub fn valid_id(id: &str) -> bool {
    id.len() <= MAX_ID_BYTES
        && id.split('-').all(|group| {
            !group.is_empty()
                && group
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}

/// Load and strictly validate a bounded JSON configuration.
///
/// # Errors
/// Returns a redacted error for IO, encoding, JSON, or validation failures.
pub fn load(path: &Path) -> Result<Config, ConfigError> {
    let file =
        File::open(path).map_err(|_| ConfigError("cannot read configuration file".into()))?;
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError("cannot read configuration file".into()))?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(ConfigError(format!(
            "configuration file exceeds {MAX_CONFIG_BYTES} bytes"
        )));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ConfigError("configuration file must be UTF-8".into()))?;
    let config: Config = serde_json::from_str(text).map_err(|error| {
        let category = match error.classify() {
            serde_json::error::Category::Io => "io",
            serde_json::error::Category::Syntax => "syntax",
            serde_json::error::Category::Data => "data",
            serde_json::error::Category::Eof => "eof",
        };
        ConfigError(format!(
            "invalid configuration JSON at line {}, column {} ({category})",
            error.line(),
            error.column()
        ))
    })?;
    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn example() -> Value {
        serde_json::from_str(include_str!("../../../../config.example.json")).unwrap()
    }
    fn parse(value: &Value) -> Result<Config, ConfigError> {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), value.to_string()).unwrap();
        load(file.path())
    }
    #[test]
    fn borb_requires_safe_endpoint_and_immutable_image() {
        let mut value = example();
        value["borb"] = json!({
            "url":"http://127.0.0.1:18080", "name_prefix":"coworkers",
            "image":format!("registry/borb/coworkers@sha256:{}", "a".repeat(64)),
            "model_secret":"coworkers-model"
        });
        assert!(parse(&value).is_ok());
        for (field, bad) in [
            ("url", "http://user:secret-marker@host"),
            ("url", "http://host/?secret-marker"),
            ("image", "registry/worker:latest"),
            ("name_prefix", "bad/value"),
            ("model_secret", "../secret-marker"),
        ] {
            let mut invalid = value.clone();
            invalid["borb"][field] = json!(bad);
            let error = parse(&invalid).unwrap_err().to_string();
            assert!(!error.contains("secret-marker"));
        }
    }

    #[test]
    fn borb_name_fits_service_limit() {
        let mut value = example();
        value["borb"] = json!({"url":"http://localhost:8080", "name_prefix":"coworkers",
            "image":format!("registry/worker@sha256:{}", "a".repeat(64)), "model_secret":"model-access"});
        value["coworkers"][0]["id"] = json!("a".repeat(30));
        assert!(parse(&value).is_ok());
        value["coworkers"][0]["id"] = json!("a".repeat(31));
        assert!(parse(&value).unwrap_err().to_string().contains("40 bytes"));
    }

    #[test]
    fn slack_block_parses() {
        let config = parse(&example()).unwrap();
        let slack = config.coworker("scout").unwrap().slack.as_ref().unwrap();
        assert_eq!(slack.team_id, "T0000000001");
        assert_eq!(slack.allowed_channel_ids, ["C0000000001"]);
        assert_eq!(slack.allowed_user_ids, ["U0000000001"]);
        assert!(config.coworker("quill").unwrap().slack.is_none());
        assert_eq!(config.slack_coworkers().count(), 1);
        assert_eq!(config.limits.max_concurrent_turns, 4);
    }

    #[test]
    fn slack_unknown_key() {
        let mut v = example();
        v["coworkers"][0]["slack"]["secret-marker"] = json!(true);
        let error = parse(&v).unwrap_err().to_string();
        assert!(error.ends_with("(data)"));
        assert!(!error.contains("secret-marker"));
    }

    #[test]
    fn slack_team_id_shape_and_redaction() {
        for team in [
            String::new(),
            "t123".into(),
            "T".into(),
            "T-1".into(),
            "T".repeat(33),
            "secret-marker".into(),
        ] {
            let mut v = example();
            v["coworkers"][0]["slack"]["team_id"] = json!(team);
            assert_eq!(
                parse(&v).unwrap_err().to_string(),
                "coworkers[0].slack.team_id must be a Slack team ID"
            );
        }
        let mut v = example();
        v["coworkers"][0]["slack"]["team_id"] = json!("T0G9PQBBK");
        assert!(parse(&v).is_ok());
    }

    #[test]
    fn slack_channel_and_user_lists() {
        for (field, prefix, cap, kind) in [
            ("allowed_channel_ids", "C", 64, "channel"),
            ("allowed_user_ids", "U", 256, "user"),
        ] {
            for invalid in [json!([]), json!(["C1", "C1"]), json!(["c1"]), json!(["T1"])] {
                let mut v = example();
                v["coworkers"][0]["slack"][field] = invalid;
                assert_eq!(
                    parse(&v).unwrap_err().to_string(),
                    format!(
                        "coworkers[0].slack.{field} must contain 1–{cap} unique Slack {kind} IDs"
                    )
                );
            }
            let mut v = example();
            v["coworkers"][0]["slack"][field] =
                json!((0..cap).map(|i| format!("{prefix}{i}")).collect::<Vec<_>>());
            assert!(parse(&v).is_ok());
            v["coworkers"][0]["slack"][field]
                .as_array_mut()
                .unwrap()
                .push(json!(format!("{prefix}{cap}")));
            assert!(parse(&v).is_err());
        }
        let mut v = example();
        v["coworkers"][0]["slack"]["allowed_channel_ids"] = json!(["U1"]);
        assert!(parse(&v).is_err());
    }

    #[test]
    fn max_concurrent_turns_bounds() {
        for count in [0, 33] {
            let mut v = example();
            v["limits"] = json!({"max_concurrent_turns":count});
            assert_eq!(
                parse(&v).unwrap_err().to_string(),
                "limits.max_concurrent_turns must be in 1..=32"
            );
        }
    }

    #[test]
    fn example_parses() {
        let config = parse(&example()).unwrap();
        assert_eq!(config.state_dir, PathBuf::from("state"));
        let scout = config.coworker("scout").unwrap();
        assert_eq!((&*scout.name, &*scout.role), ("Scout", "Researcher"));
        assert_eq!(config.limits.max_prompt_bytes, 32_768);
        assert_eq!(config.limits.model_turn_seconds, 120);
    }
    #[test]
    fn state_dir_validation() {
        let mut v = example();
        v.as_object_mut().unwrap().remove("state_dir");
        assert!(parse(&v).unwrap_err().to_string().ends_with("(data)"));
        for value in ["  ".to_owned(), "x".repeat(4097)] {
            let mut v = example();
            v["state_dir"] = json!(value);
            assert_eq!(
                parse(&v).unwrap_err().to_string(),
                "state_dir must be non-empty and at most 4096 bytes"
            );
        }
        let mut v = example();
        v["state_dir"] = json!("secret-marker\nb");
        assert_eq!(
            parse(&v).unwrap_err().to_string(),
            "state_dir must not contain control characters"
        );
    }
    #[test]
    fn unknown_top_level() {
        let mut v = example();
        v["secret-marker"] = json!(true);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.ends_with("(data)"));
        assert!(!e.contains("secret-marker"));
    }
    #[test]
    fn unknown_coworker_key() {
        let mut v = example();
        v["coworkers"][0]["secret-marker"] = json!(true);
        assert!(parse(&v).unwrap_err().to_string().ends_with("(data)"));
    }
    #[test]
    fn unknown_limits_key() {
        let mut v = example();
        v["limits"] = json!({"secret-marker":true});
        assert!(parse(&v).unwrap_err().to_string().ends_with("(data)"));
    }
    #[test]
    fn redacted_shape_error() {
        let mut v = example();
        v["limits"] = json!({"max_prompt_bytes":"secret-marker"});
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.ends_with("(data)"));
        assert!(!e.contains("secret-marker"));
    }
    #[test]
    fn duplicate_id() {
        let mut v = example();
        v["coworkers"][1]["id"] = json!("scout");
        assert!(
            parse(&v)
                .unwrap_err()
                .to_string()
                .contains("coworkers[1].id")
        );
    }
    #[test]
    fn uppercase_id() {
        let mut v = example();
        v["coworkers"][0]["id"] = json!("Scout");
        assert!(parse(&v).is_err());
    }
    #[test]
    fn blank_role() {
        let mut v = example();
        v["coworkers"][0]["role"] = json!("  ");
        assert!(parse(&v).is_err());
    }
    #[test]
    fn long_instructions() {
        let mut v = example();
        let mut instructions = "secret-marker".to_owned();
        instructions.push_str(&"x".repeat(MAX_INSTRUCTIONS_BYTES - instructions.len()));
        v["coworkers"][0]["instructions"] = json!(instructions);
        assert!(parse(&v).is_ok());
        instructions.push('x');
        v["coworkers"][0]["instructions"] = json!(instructions);
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("instructions"));
        assert!(!e.contains("secret-marker"));
    }
    #[test]
    fn zero_coworkers() {
        let mut v = example();
        v["coworkers"] = json!([]);
        assert!(parse(&v).is_err());
    }
    #[test]
    fn multiple_errors() {
        let mut v = example();
        v["coworkers"][0]["id"] = json!("Bad");
        v["coworkers"][0]["role"] = json!("");
        let e = parse(&v).unwrap_err().to_string();
        assert!(e.contains("coworkers[0].id"));
        assert!(e.contains("coworkers[0].role"));
    }
    #[test]
    fn bounds() {
        for (field, value) in [
            ("max_prompt_bytes", 0),
            ("max_prompt_bytes", 1_048_577),
            ("model_turn_seconds", 0),
            ("model_turn_seconds", 3601),
        ] {
            let mut v = example();
            v["limits"] = json!({field:value});
            assert!(parse(&v).is_err());
        }
        for id in ["", "-a", "a-", "a--b", "a_b"] {
            assert!(!valid_id(id));
        }
        assert!(valid_id("a0-b2"));
    }
    #[test]
    fn oversized_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), vec![b' '; 262_145]).unwrap();
        assert_eq!(
            load(file.path()).unwrap_err().to_string(),
            "configuration file exceeds 262144 bytes"
        );
    }
    #[test]
    fn invalid_utf8() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), [255]).unwrap();
        assert_eq!(
            load(file.path()).unwrap_err().to_string(),
            "configuration file must be UTF-8"
        );
    }
    #[test]
    fn single_line_identity_fields() {
        for field in ["name", "role"] {
            for value in ["A\tB", "A\nB", "A\rB", "A\u{1b}B"] {
                let mut v = example();
                v["coworkers"][0][field] = json!(value);
                assert!(parse(&v).unwrap_err().to_string().contains(&format!(
                    "coworkers[0].{field} must not contain control characters"
                )));
            }
        }
        let mut v = example();
        v["coworkers"][0]["instructions"] = json!("First line.\nSecond line.");
        assert!(parse(&v).is_ok());
    }
}
