pub const MAX_CONFIG_BYTES: u64 = 262_144;
pub const MAX_INSTRUCTIONS_BYTES: usize = 16_384;
pub const MAX_COWORKERS: usize = 32;
pub const MAX_ID_BYTES: usize = 64;
pub const MAX_NAME_BYTES: usize = 100;
pub const MAX_ROLE_BYTES: usize = 200;
pub const MAX_PROMPT_BYTES: usize = 1_048_576;
pub const MAX_TURN_SECONDS: u64 = 3_600;
pub const DEFAULT_PROMPT_BYTES: usize = 32_768;
pub const DEFAULT_TURN_SECONDS: u64 = 120;

pub const MAX_STATE_DIR_BYTES: usize = 4_096;

pub const MAX_SLACK_ID_BYTES: usize = 32;
pub const MAX_ALLOWED_CHANNELS: usize = 64;
pub const MAX_ALLOWED_USERS: usize = 256;
pub const DEFAULT_CONCURRENT_TURNS: usize = 4;
pub const MAX_CONCURRENT_TURNS: usize = 32;

pub const DEFAULT_API_LISTEN: &str = "127.0.0.1:8787";
pub const DEFAULT_QUEUED_PER_CONVERSATION: usize = 16;
pub const MAX_QUEUED_PER_CONVERSATION: usize = 256;
pub const API_TOKEN_ENV: &str = "CRABBER_COWORKERS_API_TOKEN";
pub const MIN_API_TOKEN_BYTES: usize = 32;
pub const API_SNAPSHOT_ROWS: usize = 100;
pub const API_SNAPSHOT_BYTES: usize = 4 * 1_048_576;
pub const API_PAGE_DEFAULT: usize = 50;
pub const API_PAGE_MAX: usize = 100;
