use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use thiserror::Error;
use url::Url;

use crate::fetcher::Fetcher;

#[derive(Debug, Error)]
pub enum RobotsError {
    #[error("invalid url (missing host)")]
    MissingHost,
}

#[derive(Debug, Clone)]
struct RobotsRules {
    disallow: Vec<String>,
}

impl RobotsRules {
    fn allows(&self, path: &str) -> bool {
        for rule in &self.disallow {
            if rule == "/" {
                return false;
            }
            if path.starts_with(rule) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct RobotsEntry {
    rules: RobotsRules,
    fetched_at: Instant,
}

#[derive(Clone)]
pub struct RobotsManager {
    cache: Arc<DashMap<String, RobotsEntry>>,
    ttl: Duration,
}

impl RobotsManager {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(60 * 60), // refresh hourly
        }
    }

    pub async fn allows(
        &self,
        fetcher: &Fetcher,
        url: &Url,
        user_agent: &str,
    ) -> Result<bool, RobotsError> {
        let host = url.host_str().ok_or(RobotsError::MissingHost)?.to_string();

        if let Some(entry) = self.cache.get(&host) {
            if entry.fetched_at.elapsed() < self.ttl {
                return Ok(entry.rules.allows(url.path()));
            }
        }

        let robots_url = format!("{}://{}/robots.txt", url.scheme(), host);
        let fetched = fetcher
            .get(&Url::parse(&robots_url).map_err(|_| RobotsError::MissingHost)?)
            .await;

        let rules = match fetched {
            Ok(resp) if resp.status.is_success() => {
                let body = String::from_utf8_lossy(&resp.body).to_string();
                parse_rules(&body, user_agent)
            }
            _ => RobotsRules {
                disallow: Vec::new(),
            },
        };

        self.cache.insert(
            host,
            RobotsEntry {
                rules: rules.clone(),
                fetched_at: Instant::now(),
            },
        );

        Ok(rules.allows(url.path()))
    }
}

fn parse_rules(body: &str, user_agent: &str) -> RobotsRules {
    let mut disallow: Vec<String> = Vec::new();
    let mut in_scope = false;
    let user_agent = user_agent.to_ascii_lowercase();

    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let mut parts = trimmed.splitn(2, ':');
        let key = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let value = parts.next().unwrap_or("").trim();

        match key.as_str() {
            "user-agent" => {
                let value_lower = value.to_ascii_lowercase();
                in_scope = value_lower == "*" || value_lower.contains(&user_agent);
            }
            "disallow" if in_scope => {
                if !value.is_empty() {
                    disallow.push(value.to_string());
                }
            }
            _ => {}
        }
    }

    RobotsRules { disallow }
}
