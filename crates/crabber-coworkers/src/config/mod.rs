pub mod constants;
use constants::{MAX_CONFIG_BYTES, MAX_INSTRUCTIONS_BYTES};
use serde::Deserialize;
use std::{collections::HashSet, fs::File, io::Read, path::Path};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub coworkers: Vec<CoworkerDefinition>,
    #[serde(default)]
    pub limits: Limits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoworkerDefinition {
    pub id: String,
    pub name: String,
    pub role: String,
    pub instructions: String,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub max_prompt_bytes: usize,
    pub model_turn_seconds: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_prompt_bytes: 32_768,
            model_turn_seconds: 120,
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

    fn validate(&self) -> Result<(), ConfigError> {
        let mut errors = Vec::new();
        if !(1..=32).contains(&self.coworkers.len()) {
            errors.push("coworkers must contain 1–32 entries".into());
        }
        let mut ids = HashSet::new();
        for (i, def) in self.coworkers.iter().enumerate() {
            if def.id.len() > 64 || !valid_id(&def.id) {
                errors.push(format!("coworkers[{i}].id must be lowercase alphanumeric groups separated by single hyphens, at most 64 bytes"));
            }
            if !ids.insert(&def.id) {
                errors.push(format!("coworkers[{i}].id must be unique"));
            }
            for (field, value, cap) in [
                ("name", &def.name, 100),
                ("role", &def.role, 200),
                ("instructions", &def.instructions, MAX_INSTRUCTIONS_BYTES),
            ] {
                if value.trim().is_empty() || value.len() > cap {
                    errors.push(format!(
                        "coworkers[{i}].{field} must be non-empty and at most {cap} bytes"
                    ));
                }
            }
        }
        if !(1..=1_048_576).contains(&self.limits.max_prompt_bytes) {
            errors.push("limits.max_prompt_bytes must be in 1..=1048576".into());
        }
        if !(1..=3_600).contains(&self.limits.model_turn_seconds) {
            errors.push("limits.model_turn_seconds must be in 1..=3600".into());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError(errors.join("\n")))
        }
    }
}

fn valid_id(id: &str) -> bool {
    id.split('-').all(|group| {
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
    fn example_parses() {
        let config = parse(&example()).unwrap();
        let scout = config.coworker("scout").unwrap();
        assert_eq!((&*scout.name, &*scout.role), ("Scout", "Researcher"));
        assert_eq!(config.limits.max_prompt_bytes, 32_768);
        assert_eq!(config.limits.model_turn_seconds, 120);
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
        v["coworkers"][0]["instructions"] = json!("secret-marker".repeat(1261));
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
}
