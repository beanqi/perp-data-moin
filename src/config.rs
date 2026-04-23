use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::domain::ExchangeId;
use crate::error::AppError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub web: WebConfig,
    #[serde(default)]
    pub monitor: MonitorConfig,
    #[serde(default)]
    pub display: DisplayConfig,
}

impl AppConfig {
    pub fn load_from_path(path: &Path) -> Result<Self, AppError> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let content = fs::read_to_string(path)?;
        let config = toml::from_str::<Self>(&content)?;
        Ok(config)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            web: WebConfig::default(),
            monitor: MonitorConfig::default(),
            display: DisplayConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitorConfig {
    #[serde(default)]
    pub enabled_exchanges: Vec<ExchangeId>,
}

impl MonitorConfig {
    pub fn includes_exchange(&self, exchange: ExchangeId) -> bool {
        self.enabled_exchanges.is_empty() || self.enabled_exchanges.contains(&exchange)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayConfig {
    #[serde(default)]
    pub sort_by: DisplaySort,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            sort_by: DisplaySort::OpenSpreadAbs,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DisplaySort {
    #[default]
    OpenSpreadAbs,
    UpdatedAt,
    Symbol,
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_owned()
}
