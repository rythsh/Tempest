use std::time::Duration;

use rand::{seq::SliceRandom, thread_rng};
use reqwest::{Client, StatusCode, header};
use thiserror::Error;
use tokio::time::sleep;
use url::Url;

const DEFAULT_USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
];

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("request failed after retries for {0}")]
    Exhausted(String),
    #[error("http status {0} for {1}")]
    Status(StatusCode, Url),
    #[error("reqwest error: {0}")]
    Client(#[from] reqwest::Error),
}

#[derive(Debug, Clone)]
pub struct FetchResult {
    pub final_url: Url,
    pub status: StatusCode,
    pub body: bytes::Bytes,
    pub content_type: Option<String>,
}

#[derive(Clone)]
pub struct Fetcher {
    client: Client,
    user_agents: Vec<String>,
    max_retries: usize,
    base_backoff: Duration,
}

impl Fetcher {
    pub fn new(user_agents: Option<Vec<String>>) -> Result<Self, reqwest::Error> {
        let client = Client::builder()
            .user_agent(DEFAULT_USER_AGENTS[0])
            .redirect(reqwest::redirect::Policy::limited(10))
             .timeout(Duration::from_secs(20))
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .pool_max_idle_per_host(8)
            .build()?;

        Ok(Self {
            client,
            user_agents: user_agents.unwrap_or_else(|| {
                DEFAULT_USER_AGENTS
                    .iter()
                    .map(|ua| ua.to_string())
                    .collect()
            }),
            max_retries: 4,
            base_backoff: Duration::from_millis(100),
        })
    }

    pub async fn get(&self, url: &Url) -> Result<FetchResult, FetchError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let user_agent = self.pick_user_agent();
            let jitter_ms: u64 = rand::random::<u8>() as u64 * 10;
            sleep(Duration::from_millis(50 + jitter_ms)).await;
            let resp = self
                .client
                .get(url.clone())
                    .header(header::USER_AGENT, user_agent)
                .header(header::ACCEPT, "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .header(header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
                .header(header::ACCEPT_ENCODING, "gzip, deflate, br")
                .send()
                .await;

            match resp {
                Ok(resp) if resp.status().is_success() => {
                    let status = resp.status();
                    let final_url = resp.url().clone();
                    let content_type = resp
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    let body = resp.bytes().await?;
                    return Ok(FetchResult {
                        final_url,
                        status,
                        body,
                        content_type,
                    });
                }
                Ok(resp) if resp.status().is_client_error() => {
                    return Err(FetchError::Status(resp.status(), resp.url().clone()));
                }
                Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                    if let Some(retry_after) = parse_retry_after(&resp) {
                        sleep(retry_after).await;
                    } else {
                        self.backoff(attempt).await;
                    }
                }
                Ok(resp) if resp.status().is_server_error() => {
                    self.backoff(attempt).await;
                }
                Err(err) => {
                    tracing::debug!("network error for {}: {err}", url);
                    self.backoff(attempt).await;
                }
                Ok(_) => return Err(FetchError::Exhausted(url.as_str().to_string())),
            }

            if attempt >= self.max_retries {
                return Err(FetchError::Exhausted(url.as_str().to_string()));
            }
        }
    }

    fn pick_user_agent(&self) -> &str {
        let mut rng = thread_rng();
        self.user_agents
            .choose(&mut rng)
            .map(|s| s.as_str())
            .unwrap_or(DEFAULT_USER_AGENTS[0])
    }

    async fn backoff(&self, attempt: usize) {
        let pow: u32 = 1u32 << attempt.min(5);
        sleep(self.base_backoff * pow).await;
    }
}

fn parse_retry_after(resp: &reqwest::Response) -> Option<Duration> {
    if let Some(header) = resp.headers().get(header::RETRY_AFTER) {
        if let Ok(secs) = header.to_str().ok()?.parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
    }
    None
}
