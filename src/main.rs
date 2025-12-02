mod cleaner;
mod fetcher;
mod images;
mod parser;
mod queue;
mod robots;
mod store;

use std::{
    env,
    error::Error,
    fs,
    io::Write,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use chrono::{DateTime, Utc};
use dashmap::DashSet;
use futures::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use serde_json::json;
use tokio::{signal, sync::Mutex};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;
use url::Url;

use cleaner::{clean_text, normalize_url};
use fetcher::{FetchError, Fetcher};
use images::ImageStore;
use parser::parse_html;
use queue::{QueueError, UrlQueue};
use robots::{RobotsError, RobotsManager};
use store::{DataStore, StoreError, build_record};

const DEFAULT_USER_AGENT: &str = "ryth-tempest/0.1";
const DEFAULT_CONCURRENCY: usize = 128;
const DEFAULT_SEEDS: &[&str] = &[
    "https://example.com/",
    "https://www.rust-lang.org/",
    "https://blog.rust-lang.org/",
    "https://en.wikipedia.org/wiki/Rust_(programming_language)",
    "https://news.ycombinator.com/",
];

#[derive(thiserror::Error, Debug)]
enum CrawlerError {
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Robots(#[from] RobotsError),
}

#[derive(Debug, Clone, Deserialize)]
struct Config {
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
    #[serde(default = "default_seeds_file")]
    seeds_file: PathBuf,
    #[serde(default = "default_aftermath_dir")]
    aftermath_dir: PathBuf,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_user_agent")]
    user_agent: String,
    #[serde(default = "default_save_images")]
    save_images: bool,
    #[serde(default = "default_image_mime_whitelist")]
    image_mime_whitelist: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            seeds_file: default_seeds_file(),
            aftermath_dir: default_aftermath_dir(),
            concurrency: DEFAULT_CONCURRENCY,
            user_agent: default_user_agent(),
            save_images: default_save_images(),
            image_mime_whitelist: default_image_mime_whitelist(),
        }
    }
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_seeds_file() -> PathBuf {
    PathBuf::from("sites.csv")
}

fn default_aftermath_dir() -> PathBuf {
    PathBuf::from("aftermath")
}

fn default_concurrency() -> usize {
    DEFAULT_CONCURRENCY
}

fn default_user_agent() -> String {
    DEFAULT_USER_AGENT.to_string()
}

fn default_save_images() -> bool {
    true
}

fn default_image_mime_whitelist() -> Vec<String> {
    vec![
        "image/png".to_string(),
        "image/jpeg".to_string(),
        "image/jpg".to_string(),
        "image/gif".to_string(),
        "image/webp".to_string(),
    ]
}

#[derive(Clone)]
struct SeedFile {
    path: PathBuf,
    seen: Arc<DashSet<String>>,
    file: Arc<Mutex<fs::File>>,
}

impl SeedFile {
    pub fn open(path: PathBuf) -> Result<Self, std::io::Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        if !path.exists() {
            fs::File::create(&path)?;
        }

        let seen = DashSet::new();
        let contents = fs::read_to_string(&path)?;
        let mut cleaned_lines: Vec<String> = Vec::new();

        for (idx, line) in contents.lines().enumerate() {
            let url = line.split(',').next().unwrap_or("").trim();

            let is_header = idx == 0 && url.eq_ignore_ascii_case("url");
            if url.is_empty() || url.starts_with('#') || is_header {
                if is_header {
                    cleaned_lines.push(line.to_string());
                }
                continue;
            }

            if seen.insert(url.to_string()) {
                cleaned_lines.push(line.to_string());
            }
        }

        let reconstructed = if cleaned_lines.is_empty() {
            String::new()
        } else {
            let mut joined = cleaned_lines.join("\n");
            joined.push('\n');
            joined
        };

        if reconstructed != contents {
            fs::write(&path, reconstructed)?;
        }

        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            seen: Arc::new(seen),
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn entries(&self) -> Vec<String> {
        self.seen.iter().map(|u| u.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    pub async fn ensure_defaults(&self, defaults: &[&str]) -> Result<(), std::io::Error> {
        if !self.is_empty() {
            return Ok(());
        }

        for seed in defaults {
            if let Ok(url) = Url::parse(seed) {
                let _ = self.record(&url, &url).await?;
            }
        }

        Ok(())
    }

    pub async fn record(&self, url: &Url, source: &Url) -> Result<bool, std::io::Error> {
        let url_string = url.as_str().to_string();
        if !self.seen.insert(url_string.clone()) {
            return Ok(false);
        }

        let timestamp = Utc::now().to_rfc3339();

        let mut file = self.file.lock().await;
        writeln!(file, "{},{},{}", url_string, source.as_str(), timestamp)?;
        file.flush()?;

        Ok(true)
    }
}

#[derive(Clone)]
struct CrawlStats {
    started_at: DateTime<Utc>,
    fetched: Arc<AtomicUsize>,
    stored: Arc<AtomicUsize>,
    enqueued: Arc<AtomicUsize>,
    new_sites: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
    recent: Arc<Mutex<Vec<String>>>,
}

impl CrawlStats {
    pub fn new() -> Self {
        Self {
            started_at: Utc::now(),
            fetched: Arc::new(AtomicUsize::new(0)),
            stored: Arc::new(AtomicUsize::new(0)),
            enqueued: Arc::new(AtomicUsize::new(0)),
            new_sites: Arc::new(AtomicUsize::new(0)),
            failed: Arc::new(AtomicUsize::new(0)),
            recent: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn record_fetch(&self) {
        self.fetched.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_store(&self, stored: bool) {
        if stored {
            self.stored.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_enqueue(&self) {
        self.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_new_site(&self) {
        self.new_sites.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn push_recent(&self, url: &Url) {
        const MAX_RECENT: usize = 50;
        let mut recent = self.recent.lock().await;
        if recent.len() >= MAX_RECENT {
            recent.remove(0);
        }
        recent.push(url.as_str().to_string());
    }

    pub async fn snapshot(&self, queue_len: usize, seen_len: usize) -> StatsSnapshot {
        let ended_at = Utc::now();
        let recent_urls = self.recent.lock().await.clone();

        StatsSnapshot {
            started_at: self.started_at,
            ended_at,
            duration_secs: ended_at
                .signed_duration_since(self.started_at)
                .num_seconds()
                .max(0),
            fetched: self.fetched.load(Ordering::Relaxed),
            stored: self.stored.load(Ordering::Relaxed),
            enqueued: self.enqueued.load(Ordering::Relaxed),
            new_sites: self.new_sites.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            queue_len,
            seen_len,
            recent_urls,
        }
    }
}

struct StatsSnapshot {
    started_at: DateTime<Utc>,
    ended_at: DateTime<Utc>,
    duration_secs: i64,
    fetched: usize,
    stored: usize,
    enqueued: usize,
    new_sites: usize,
    failed: usize,
    queue_len: usize,
    seen_len: usize,
    recent_urls: Vec<String>,
}

fn load_config() -> Result<Config, Box<dyn Error>> {
    let mut args = env::args().skip(1).peekable();
    let mut config_path: Option<PathBuf> = None;
    let mut concurrency_override: Option<usize> = None;
    let mut seeds_override: Option<PathBuf> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if let Some(path) = args.next() {
                    config_path = Some(PathBuf::from(path));
                }
            }
            "--concurrency" => {
                if let Some(val) = args.next() {
                    concurrency_override = val.parse::<usize>().ok();
                }
            }
            "--seeds-file" => {
                if let Some(path) = args.next() {
                    seeds_override = Some(PathBuf::from(path));
                }
            }
            _ => {}
        }
    }

    let resolved_path = config_path.unwrap_or_else(|| PathBuf::from("config.yml"));
    let mut config = if resolved_path.exists() {
        let contents = fs::read_to_string(&resolved_path)?;
        serde_yaml::from_str::<Config>(&contents)?
    } else {
        Config::default()
    };

    if let Some(concurrency) = concurrency_override {
        config.concurrency = concurrency;
    }

    if let Some(seeds_file) = seeds_override {
        config.seeds_file = seeds_file;
    }

    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();

    let config = load_config()?;
    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.aftermath_dir)?;

    let seed_file = Arc::new(SeedFile::open(config.seeds_file.clone())?);
    seed_file.ensure_defaults(DEFAULT_SEEDS).await?;

    let queue = Arc::new(UrlQueue::open(config.data_dir.join("url_queue.db"))?);
    let store = DataStore::open(&config.data_dir)?;
    let image_store = if config.save_images {
        Some(Arc::new(ImageStore::open(
            &config.data_dir,
            &config.image_mime_whitelist,
        )?))
    } else {
        None
    };
    let fetcher = Arc::new(Fetcher::new(Some(vec![config.user_agent.clone()]))?);
    let robots = RobotsManager::new();
    let stats = CrawlStats::new();

    seed_queue(&queue, &seed_file.entries(), &stats)?;
    info!(
        "starting crawl with {} urls in queue (concurrency={})",
        queue.len(),
        config.concurrency
    );

    tokio::select! {
        res = crawl(
            queue.clone(),
            robots.clone(),
            store.clone(),
            fetcher.clone(),
            seed_file.clone(),
            stats.clone(),
            config.concurrency,
            config.user_agent.clone(),
            image_store.clone(),
        ) => {
            if let Err(err) = res {
                error!("crawl error: {err}");
            }
        }
        _ = signal::ctrl_c() => {
            info!("received ctrl+c; shutting down");
        }
    }

    write_aftermath(&config, &stats, &queue, &seed_file).await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn crawl(
    queue: Arc<UrlQueue>,
    robots: RobotsManager,
    store: DataStore,
    fetcher: Arc<Fetcher>,
    seed_file: Arc<SeedFile>,
    stats: CrawlStats,
    concurrency: usize,
    user_agent: String,
    image_store: Option<Arc<ImageStore>>,
) -> Result<(), CrawlerError> {
    let mut in_flight: FuturesUnordered<_> = FuturesUnordered::new();

    loop {
        while in_flight.len() < concurrency {
            match queue.dequeue()? {
                Some(url) => {
                    let fetcher = fetcher.clone();
                    let robots = robots.clone();
                    let store = store.clone();
                    let queue_clone = queue.clone();
                    let stats_clone = stats.clone();
                    let user_agent_clone = user_agent.clone();
                    let image_store_clone = image_store.clone();
                    in_flight.push(tokio::spawn(async move {
                        let links = handle_url(
                            url.clone(),
                            fetcher.clone(),
                            robots.clone(),
                            store,
                            stats_clone.clone(),
                            image_store_clone,
                            &user_agent_clone,
                        )
                        .await;
                        (url, links, queue_clone, stats_clone)
                    }));
                }
                None => break,
            }
        }

        if in_flight.is_empty() {
            break;
        }

        if let Some(join_result) = in_flight.next().await {
            match join_result {
                Ok((source_url, Ok(outcome), queue_handle, stats_handle)) => {
                    for link in outcome.links {
                        if let Some(normalized) = normalize_url(&link) {
                            match queue_handle.enqueue(&normalized) {
                                Ok(true) => {
                                    stats_handle.record_enqueue();
                                    match seed_file.record(&normalized, &source_url).await {
                                        Ok(true) => stats_handle.record_new_site(),
                                        Ok(false) => {}
                                        Err(err) => error!(
                                            "failed appending {} to {}: {err}",
                                            normalized,
                                            seed_file.path.display()
                                        ),
                                    }
                                }
                                Ok(false) => {}
                                Err(err) => error!("enqueue error for {}: {err}", normalized),
                            }
                        }
                    }
                    info!(
                        "finished {} -> {} (stored_record={})",
                        source_url, outcome.final_url, outcome.stored
                    );
                }
                Ok((url, Err(err), _, stats_handle)) => {
                    stats_handle.record_failed();
                    error!("error processing {}: {err}", url)
                }
                Err(join_err) => error!("task join error: {join_err}"),
            }
        }
    }

    Ok(())
}

struct HandleOutcome {
    links: Vec<Url>,
    stored: bool,
    final_url: Url,
}

async fn handle_url(
    url: Url,
    fetcher: Arc<Fetcher>,
    robots: RobotsManager,
    store: DataStore,
    stats: CrawlStats,
    image_store: Option<Arc<ImageStore>>,
    user_agent: &str,
) -> Result<HandleOutcome, CrawlerError> {
    if !robots.allows(fetcher.as_ref(), &url, user_agent).await? {
        info!("robots disallow {}", url);
        return Ok(HandleOutcome {
            links: Vec::new(),
            stored: false,
            final_url: url,
        });
    }

    let response = match fetcher.get(&url).await {
        Ok(resp) => resp,
        Err(FetchError::Status(status, target)) => {
            info!("skipping non-success {} for {}", status, target);
            return Ok(HandleOutcome {
                links: Vec::new(),
                stored: false,
                final_url: target,
            });
        }
        Err(FetchError::Client(err)) => {
            info!("network error for {}: {err}", url);
            return Ok(HandleOutcome {
                links: Vec::new(),
                stored: false,
                final_url: url,
            });
        }
        Err(e) => return Err(CrawlerError::Fetch(e)),
    };
    stats.record_fetch();
    stats.push_recent(&response.final_url).await;
    if let Some(ct) = response.content_type.as_deref() {
        if !ct.contains("text/html") && !ct.contains("text/plain") {
            info!(
                "skipping non-html content-type {} for {}",
                ct, response.final_url
            );
            return Ok(HandleOutcome {
                links: Vec::new(),
                stored: false,
                final_url: response.final_url,
            });
        }
    }
    let fetched_at = Utc::now();
    let body = String::from_utf8_lossy(&response.body).to_string();
    let content_length = response.body.len();
    let parsed = parse_html(&body, &response.final_url);
    if let Some(store) = image_store {
        for image_url in parsed.images.iter() {
            if let Err(err) = store.save_image(fetcher.as_ref(), image_url).await {
                error!("image download failed for {}: {err}", image_url);
            }
        }
    }
    let cleaned = clean_text(&parsed.content);

    if cleaned.is_empty() {
        return Ok(HandleOutcome {
            links: Vec::new(),
            stored: false,
            final_url: response.final_url,
        });
    }

    let word_count = cleaned.split_whitespace().count();
    let record = build_record(
        &url,
        &response.final_url,
        parsed.title.clone(),
        cleaned,
        response.status.as_u16(),
        response.content_type.clone(),
        fetched_at,
        content_length,
        word_count,
        parsed.links.len(),
    );
    let stored = store.write_record(&record).await?;
    stats.record_store(stored);

    Ok(HandleOutcome {
        links: parsed.links,
        stored,
        final_url: response.final_url,
    })
}

async fn write_aftermath(
    config: &Config,
    stats: &CrawlStats,
    queue: &UrlQueue,
    seed_file: &SeedFile,
) -> Result<(), Box<dyn Error>> {
    let snapshot = stats.snapshot(queue.len(), queue.seen_len()).await;
    fs::create_dir_all(&config.aftermath_dir)?;
    let filename = format!("session-{}.json", Utc::now().format("%Y%m%dT%H%M%SZ"));
    let path = config.aftermath_dir.join(filename);

    let report = json!({
        "started_at": snapshot.started_at.to_rfc3339(),
        "ended_at": snapshot.ended_at.to_rfc3339(),
        "duration_secs": snapshot.duration_secs,
        "concurrency": config.concurrency,
        "data_paths": {
            "data_dir": config.data_dir.to_string_lossy().to_string(),
            "content_db": config.data_dir.join("content.db").to_string_lossy().to_string(),
            "output_jsonl": config.data_dir.join("output.jsonl").to_string_lossy().to_string(),
            "url_queue": config.data_dir.join("url_queue.db").to_string_lossy().to_string(),
            "seeds_file": seed_file.path.to_string_lossy().to_string(),
        },
        "stats": {
            "fetched": snapshot.fetched,
            "stored_records": snapshot.stored,
            "enqueued": snapshot.enqueued,
            "new_sites_appended": snapshot.new_sites,
            "failed": snapshot.failed,
            "queue_len": snapshot.queue_len,
            "seen_urls": snapshot.seen_len,
            "known_sites_file": seed_file.len(),
        },
        "recent_urls": snapshot.recent_urls,
    });

    fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    info!("wrote aftermath summary to {}", path.display());
    Ok(())
}

fn seed_queue(queue: &UrlQueue, seeds: &[String], stats: &CrawlStats) -> Result<(), QueueError> {
    if queue.len() > 0 {
        return Ok(());
    }

    let seeds_iter: Vec<String> = if seeds.is_empty() {
        DEFAULT_SEEDS
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
    } else {
        seeds.to_vec()
    };

    for seed in seeds_iter.into_iter() {
        let url = Url::parse(&seed).map_err(|_| QueueError::InvalidUrl(seed.to_string()))?;
        if queue.enqueue(&url)? {
            stats.record_enqueue();
        }
    }

    Ok(())
}
