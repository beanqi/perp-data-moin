use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("failed to read config: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    ConfigParse(#[from] toml::de::Error),
}
