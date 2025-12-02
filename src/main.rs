mod cleaner;
mod fetcher;
mod images;
mod parser;
mod queue;
mod robots;
mod store;

use std::{
    collections::{BTreeMap, HashSet},
    env,
    error::Error,
    fs,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use chrono::{DateTime, Utc};
use dashmap::DashSet;
use futures::{StreamExt, stream::FuturesUnordered};
use serde::Deserialize;
use serde_json::json;
use tokio::{
    signal,
    sync::Mutex,
    time::{Duration, MissedTickBehavior},
};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use url::Url;
use whatlang::{Lang, detect};

use cleaner::{clean_text, normalize_url};
use fetcher::{FetchError, Fetcher};
use images::ImageStore;
use parser::parse_html;
use queue::{QueueError, UrlQueue};
use robots::{RobotsError, RobotsManager};
use store::{DataStore, PageRecord, StoreError, build_record};

const DEFAULT_USER_AGENT: &str = "ryth-tempest/0.1";
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_SEEDS: &[&str] = &[
    "https://example.com/",
    "https://www.rust-lang.org/",
    "https://blog.rust-lang.org/",
    "https://news.ycombinator.com/",
    "https://lobste.rs/",
    "https://dev.to/",
    "https://medium.com/tag/programming",
    "https://medium.com/tag/machine-learning",
];
const DEFAULT_MIN_WORD_COUNT: usize = 150;
const DEFAULT_LANGUAGE_CONFIDENCE: f64 = 0.75;
const DEFAULT_FILTER_ENGLISH: bool = true;
const DEFAULT_FOCUS_KEYWORDS: &[&str] = &[
    "code",
    "coding",
    "developer",
    "api",
    "sdk",
    "python",
    "rust",
    "go",
    "java",
    "javascript",
    "kubernetes",
    "docker",
    "database",
    "postgresql",
    "mysql",
    "mongodb",
    "redis",
    "security",
    "vulnerability",
    "exploit",
    "threat",
    "cybersecurity",
    "analysis",
    "incident",
    "response",
];
const DEFAULT_MIN_KEYWORD_MATCHES: usize = 1;
const DEFAULT_REQUIRE_KEYWORD_MATCH: bool = false;
const DEFAULT_STATS_ENABLED: bool = true;
const DEFAULT_REPORT_LANGUAGES: bool = true;
const DEFAULT_REPORT_WORD_COUNT: bool = true;
const DEFAULT_REPORT_DATASET_SIZE: bool = true;
const DEFAULT_REPORT_RECORD_COUNT: bool = true;
const DEFAULT_STATS_LANGUAGE_CONFIDENCE: f64 = 0.5;
const DEFAULT_BYPASS_ROBOTS: bool = false;

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
    #[serde(default)]
    ai_preferences: AiPreferences,
    #[serde(default = "default_seed_list")]
    default_seeds: Vec<String>,
    #[serde(default)]
    stats: StatsConfig,
    #[serde(default = "default_bypass_robots")]
    bypass_robots: bool,
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
            ai_preferences: AiPreferences::default(),
            default_seeds: default_seed_list(),
            stats: StatsConfig::default(),
            bypass_robots: default_bypass_robots(),
        }
    }
}

fn default_bypass_robots() -> bool {
    DEFAULT_BYPASS_ROBOTS
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

fn default_seed_list() -> Vec<String> {
    DEFAULT_SEEDS.iter().map(|seed| seed.to_string()).collect()
}

fn default_min_word_count() -> usize {
    DEFAULT_MIN_WORD_COUNT
}

fn default_filter_english() -> bool {
    DEFAULT_FILTER_ENGLISH
}

fn default_language_confidence() -> f64 {
    DEFAULT_LANGUAGE_CONFIDENCE
}

fn default_focus_keywords() -> Vec<String> {
    DEFAULT_FOCUS_KEYWORDS
        .iter()
        .map(|s| s.to_string())
        .collect()
}

fn default_min_keyword_matches() -> usize {
    DEFAULT_MIN_KEYWORD_MATCHES
}

fn default_require_keyword_match() -> bool {
    DEFAULT_REQUIRE_KEYWORD_MATCH
}

fn default_stats_enabled() -> bool {
    DEFAULT_STATS_ENABLED
}

fn default_report_languages() -> bool {
    DEFAULT_REPORT_LANGUAGES
}

fn default_report_word_count() -> bool {
    DEFAULT_REPORT_WORD_COUNT
}

fn default_report_dataset_size() -> bool {
    DEFAULT_REPORT_DATASET_SIZE
}

fn default_report_record_count() -> bool {
    DEFAULT_REPORT_RECORD_COUNT
}

fn default_stats_language_confidence() -> f64 {
    DEFAULT_STATS_LANGUAGE_CONFIDENCE
}

#[derive(Debug, Clone, Deserialize)]
struct StatsConfig {
    #[serde(default = "default_stats_enabled")]
    enabled: bool,
    #[serde(default)]
    dataset_file: Option<PathBuf>,
    #[serde(default = "default_report_languages")]
    languages: bool,
    #[serde(default = "default_report_word_count")]
    word_count: bool,
    #[serde(default = "default_report_dataset_size")]
    dataset_size: bool,
    #[serde(default = "default_report_record_count")]
    record_count: bool,
    #[serde(default = "default_stats_language_confidence")]
    language_confidence: f64,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            enabled: default_stats_enabled(),
            dataset_file: None,
            languages: default_report_languages(),
            word_count: default_report_word_count(),
            dataset_size: default_report_dataset_size(),
            record_count: default_report_record_count(),
            language_confidence: default_stats_language_confidence(),
        }
    }
}

#[derive(Debug)]
struct CliOptions {
    config_path: Option<PathBuf>,
    concurrency_override: Option<usize>,
    seeds_override: Option<PathBuf>,
    stats_mode: bool,
}

fn parse_cli_options() -> CliOptions {
    let mut args = env::args().skip(1).peekable();
    let mut opts = CliOptions {
        config_path: None,
        concurrency_override: None,
        seeds_override: None,
        stats_mode: false,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                if let Some(path) = args.next() {
                    opts.config_path = Some(PathBuf::from(path));
                }
            }
            "--concurrency" => {
                if let Some(val) = args.next() {
                    opts.concurrency_override = val.parse::<usize>().ok();
                }
            }
            "--seeds-file" => {
                if let Some(path) = args.next() {
                    opts.seeds_override = Some(PathBuf::from(path));
                }
            }
            "--stats" => {
                opts.stats_mode = true;
            }
            _ => {}
        }
    }

    opts
}

#[derive(Debug, Clone, Deserialize)]
struct AiPreferences {
    #[serde(default = "default_min_word_count")]
    min_word_count: usize,
    #[serde(default = "default_filter_english")]
    filter_english: bool,
    #[serde(default = "default_language_confidence")]
    min_language_confidence: f64,
    #[serde(default = "default_focus_keywords")]
    focus_keywords: Vec<String>,
    #[serde(default = "default_min_keyword_matches")]
    min_keyword_matches: usize,
    #[serde(default = "default_require_keyword_match")]
    require_keyword_match: bool,
}

impl Default for AiPreferences {
    fn default() -> Self {
        Self {
            min_word_count: default_min_word_count(),
            filter_english: default_filter_english(),
            min_language_confidence: default_language_confidence(),
            focus_keywords: default_focus_keywords(),
            min_keyword_matches: default_min_keyword_matches(),
            require_keyword_match: default_require_keyword_match(),
        }
    }
}

impl AiPreferences {
    fn matches_focus_keywords(&self, text: &str) -> bool {
        if self.focus_keywords.is_empty() {
            return true;
        }
        let lower = text.to_lowercase();
        let matches = self
            .focus_keywords
            .iter()
            .filter(|kw| lower.contains(&kw.to_lowercase()))
            .count();
        matches >= self.min_keyword_matches
    }
}

fn canonical_url(url: &Url) -> Url {
    normalize_url(url).unwrap_or_else(|| url.clone())
}

#[derive(Clone)]
struct SeedFile {
    path: PathBuf,
    seen: Arc<DashSet<String>>,
    file: Arc<Mutex<fs::File>>,
    last_modified: Arc<StdMutex<Option<SystemTime>>>,
    io_lock: Arc<Mutex<()>>,
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
            let mut parts = line.split(',');
            let url_field = parts.next().unwrap_or("").trim();

            let is_header = idx == 0 && url_field.eq_ignore_ascii_case("url");
            if is_header {
                cleaned_lines.push(line.to_string());
                continue;
            }

            if url_field.is_empty() {
                continue;
            }

            if url_field.starts_with('#') {
                cleaned_lines.push(line.to_string());
                continue;
            }

            let source = parts
                .next()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("manual");
            let discovered = parts
                .next()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("bootstrap");

            match Url::parse(url_field) {
                Ok(parsed) => {
                    let canonical = canonical_url(&parsed);
                    let canonical_string = canonical.as_str().to_string();
                    if seen.insert(canonical_string.clone()) {
                        cleaned_lines
                            .push(format!("{},{},{}", canonical_string, source, discovered));
                    }
                }
                Err(_) => {
                    cleaned_lines.push(line.to_string());
                }
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

        let metadata = fs::metadata(&path)?;
        let modified = metadata.modified().ok();

        Ok(Self {
            path,
            seen: Arc::new(seen),
            file: Arc::new(Mutex::new(file)),
            last_modified: Arc::new(StdMutex::new(modified)),
            io_lock: Arc::new(Mutex::new(())),
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

    pub async fn ensure_defaults(&self, defaults: &[String]) -> Result<(), std::io::Error> {
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
        let canonical = canonical_url(url);
        let url_string = canonical.as_str().to_string();
        if !self.seen.insert(url_string.clone()) {
            return Ok(false);
        }

        let timestamp = Utc::now().to_rfc3339();

        let _io_guard = self.io_lock.lock().await;
        let mut file = self.file.lock().await;
        writeln!(file, "{},{},{}", url_string, source.as_str(), timestamp)?;
        file.flush()?;

        if let Ok(metadata) = fs::metadata(&self.path) {
            if let Ok(modified) = metadata.modified() {
                let mut guard = self.last_modified.lock().unwrap();
                *guard = Some(modified);
            }
        }

        Ok(true)
    }

    pub async fn remove(&self, url: &Url) -> Result<bool, std::io::Error> {
        let canonical = canonical_url(url);
        let canonical_string = canonical.as_str().to_string();
        let _io_guard = self.io_lock.lock().await;

        let path = self.path.clone();
        let target = canonical_string.clone();
        let handler_result =
            tokio::task::spawn_blocking(move || rewrite_site_without(path, target))
                .await
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        let removed = handler_result?;

        if removed {
            self.seen.remove(&canonical_string);
            if let Ok(metadata) = fs::metadata(&self.path) {
                if let Ok(modified) = metadata.modified() {
                    let mut guard = self.last_modified.lock().unwrap();
                    *guard = Some(modified);
                }
            }
            let mut file_guard = self.file.lock().await;
            *file_guard = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?;
        }

        Ok(removed)
    }

    pub async fn refresh(&self) -> Result<Vec<Url>, std::io::Error> {
        let _io_guard = self.io_lock.lock().await;
        let path = self.path.clone();
        let seen = self.seen.clone();
        let last_modified = self.last_modified.clone();
        let result =
            tokio::task::spawn_blocking(move || refresh_from_disk(path, seen, last_modified))
                .await
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        drop(_io_guard);
        result
    }
}

fn refresh_from_disk(
    path: PathBuf,
    seen: Arc<DashSet<String>>,
    last_modified: Arc<StdMutex<Option<SystemTime>>>,
) -> Result<Vec<Url>, std::io::Error> {
    let metadata = fs::metadata(&path)?;
    let modified = metadata.modified().ok();

    {
        let mut guard = last_modified.lock().unwrap();
        if guard.as_ref() == modified.as_ref() {
            return Ok(Vec::new());
        }
        *guard = modified.clone();
    }

    let contents = fs::read_to_string(&path)?;
    let mut new_urls = Vec::new();
    let mut file_urls = HashSet::new();
    for (idx, line) in contents.lines().enumerate() {
        let mut parts = line.split(',');
        let url_field = parts.next().unwrap_or("").trim();

        let is_header = idx == 0 && url_field.eq_ignore_ascii_case("url");
        if is_header {
            continue;
        }

        if url_field.is_empty() || url_field.starts_with('#') {
            continue;
        }

        if let Ok(parsed) = Url::parse(url_field) {
            let canonical = canonical_url(&parsed);
            let canonical_string = canonical.as_str().to_string();
            file_urls.insert(canonical_string.clone());
            if seen.insert(canonical_string.clone()) {
                new_urls.push(canonical);
            }
        }
    }

    let mut to_remove = Vec::new();
    for entry in seen.iter() {
        let entry_str = entry.clone();
        if !file_urls.contains(&entry_str) {
            to_remove.push(entry_str);
        }
    }
    for entry in to_remove {
        seen.remove(&entry);
    }

    Ok(new_urls)
}

fn rewrite_site_without(path: PathBuf, target: String) -> Result<bool, std::io::Error> {
    let contents = fs::read_to_string(&path)?;
    let mut cleaned_lines = Vec::new();
    let mut removed = false;

    for (idx, line) in contents.lines().enumerate() {
        let mut parts = line.split(',');
        let url_field = parts.next().unwrap_or("").trim();

        let is_header = idx == 0 && url_field.eq_ignore_ascii_case("url");
        if is_header {
            cleaned_lines.push(line.to_string());
            continue;
        }

        if url_field.is_empty() || url_field.starts_with('#') {
            cleaned_lines.push(line.to_string());
            continue;
        }

        if let Ok(parsed) = Url::parse(url_field) {
            let canonical = canonical_url(&parsed);
            if canonical.as_str() == target {
                removed = true;
                continue;
            }
        }

        cleaned_lines.push(line.to_string());
    }

    if removed {
        let reconstructed = if cleaned_lines.is_empty() {
            String::new()
        } else {
            let mut joined = cleaned_lines.join("\n");
            joined.push('\n');
            joined
        };
        fs::write(path, reconstructed)?;
    }

    Ok(removed)
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

fn load_config(config_path: Option<PathBuf>) -> Result<Config, Box<dyn Error>> {
    let resolved_path = config_path.unwrap_or_else(|| PathBuf::from("config.yml"));
    let config = if resolved_path.exists() {
        let contents = fs::read_to_string(&resolved_path)?;
        serde_yaml::from_str::<Config>(&contents)?
    } else {
        Config::default()
    };

    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();

    let cli_options = parse_cli_options();
    let mut config = load_config(cli_options.config_path.clone())?;
    if let Some(concurrency) = cli_options.concurrency_override {
        config.concurrency = concurrency;
    }
    if let Some(seeds_file) = cli_options.seeds_override {
        config.seeds_file = seeds_file;
    }

    if cli_options.stats_mode {
        run_dataset_stats(&config, &config.stats)?;
        return Ok(());
    }

    fs::create_dir_all(&config.data_dir)?;
    fs::create_dir_all(&config.aftermath_dir)?;

    let seed_file = Arc::new(SeedFile::open(config.seeds_file.clone())?);
    seed_file.ensure_defaults(&config.default_seeds).await?;

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
    let ai_preferences = config.ai_preferences.clone();

    seed_queue(&queue, &seed_file.entries(), &config.default_seeds, &stats)?;
    let _seed_refresh_handle = tokio::spawn(seed_refresh_loop(
        seed_file.clone(),
        queue.clone(),
        stats.clone(),
    ));
    if config.bypass_robots {
        warn!(
            "robots.txt checks disabled via config (bypass_robots=true); crawler will ignore site restrictions"
        );
    }
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
            ai_preferences.clone(),
            config.bypass_robots,
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
    ai_preferences: AiPreferences,
    bypass_robots: bool,
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
                    let ai_preferences_clone = ai_preferences.clone();
                    let bypass_robots_flag = bypass_robots;
                    in_flight.push(tokio::spawn(async move {
                        let links = handle_url(
                            url.clone(),
                            fetcher.clone(),
                            robots.clone(),
                            store,
                            stats_clone.clone(),
                            image_store_clone,
                            &user_agent_clone,
                            ai_preferences_clone,
                            bypass_robots_flag,
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
                    error!("error processing {}: {err}", url);
                    match seed_file.remove(&url).await {
                        Ok(true) => info!("dropped {} from seeds after repeated crawl error", url),
                        Ok(false) => {}
                        Err(remove_err) => {
                            error!("failed removing {} from seeds.csv: {remove_err}", url)
                        }
                    }
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
    ai_preferences: AiPreferences,
    bypass_robots: bool,
) -> Result<HandleOutcome, CrawlerError> {
    if !bypass_robots && !robots.allows(fetcher.as_ref(), &url, user_agent).await? {
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
    let final_url = response.final_url.clone();
    if let Some(ct) = response.content_type.as_deref() {
        if !ct.contains("text/html") && !ct.contains("text/plain") {
            info!("skipping non-html content-type {} for {}", ct, final_url);
            return Ok(HandleOutcome {
                links: Vec::new(),
                stored: false,
                final_url: final_url.clone(),
            });
        }
    }
    let fetched_at = Utc::now();
    let body = String::from_utf8_lossy(&response.body).to_string();
    let content_length = response.body.len();
    let parsed = parse_html(&body, &final_url);
    let raw_html = body.clone();

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
            final_url: final_url.clone(),
        });
    }

    let word_count = cleaned.split_whitespace().count();
    if word_count < ai_preferences.min_word_count {
        return Ok(HandleOutcome {
            links: Vec::new(),
            stored: false,
            final_url: final_url.clone(),
        });
    }

    if ai_preferences.filter_english {
        match detect(&cleaned) {
            Some(info)
                if info.lang() == Lang::Eng
                    && info.confidence() >= ai_preferences.min_language_confidence =>
            {
                // acceptable English text
            }
            _ => {
                return Ok(HandleOutcome {
                    links: Vec::new(),
                    stored: false,
                    final_url: final_url.clone(),
                });
            }
        }
    }

    let matches_keywords = ai_preferences.matches_focus_keywords(&cleaned);
    if ai_preferences.require_keyword_match && !matches_keywords {
        info!(
            "skipping {} because it lacks required AI training keywords (still exploring links)",
            final_url
        );
        return Ok(HandleOutcome {
            links: parsed.links,
            stored: false,
            final_url: final_url.clone(),
        });
    }
    let record = build_record(
        &url,
        &final_url,
        parsed.title.clone(),
        cleaned,
        raw_html,
        response.status.as_u16(),
        response.content_type.clone(),
        fetched_at,
        content_length,
        word_count,
        parsed.links.len(),
        matches_keywords,
        parsed.code_snippets.clone(),
        true,
    );

    let stored = store.write_record(&record).await?;
    stats.record_store(stored);

    Ok(HandleOutcome {
        links: parsed.links,
        stored,
        final_url,
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

fn run_dataset_stats(config: &Config, stats_config: &StatsConfig) -> Result<(), Box<dyn Error>> {
    if !stats_config.enabled {
        println!("Dataset stats reporting disabled in config");
        return Ok(());
    }

    let dataset_path = stats_config
        .dataset_file
        .clone()
        .unwrap_or_else(|| config.data_dir.join("output.jsonl"));

    if !dataset_path.exists() {
        return Err(format!("dataset file {} not found", dataset_path.display()).into());
    }

    let file = fs::File::open(&dataset_path)?;
    let reader = BufReader::new(file);

    let mut record_count = 0usize;
    let mut total_words = 0usize;
    let mut languages: BTreeMap<String, usize> = BTreeMap::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let record: PageRecord = serde_json::from_str(&line)?;
        record_count += 1;
        if stats_config.word_count {
            total_words += record.word_count;
        }
        if stats_config.languages {
            let lang_key = match detect(&record.content) {
                Some(info) if info.confidence() >= stats_config.language_confidence => {
                    info.lang().code().to_string()
                }
                Some(info) => format!("{}_low_conf", info.lang().code()),
                None => "unknown".to_string(),
            };
            *languages.entry(lang_key).or_insert(0) += 1;
        }
    }

    let metadata = fs::metadata(&dataset_path)?;
    let size_bytes = metadata.len();
    let size_gb = size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("Dataset stats for {}", dataset_path.display());
    if stats_config.record_count {
        println!("Records: {}", record_count);
    }
    if stats_config.word_count {
        println!("Total words: {}", total_words);
    }
    if stats_config.dataset_size {
        println!("Dataset size: {:.4} GB ({size_bytes} bytes)", size_gb);
    }
    if stats_config.languages {
        println!(
            "Languages (counted when confidence >= {:.2}):",
            stats_config.language_confidence
        );
        let mut sorted_langs: Vec<_> = languages.into_iter().collect();
        sorted_langs.sort_by(|a, b| b.1.cmp(&a.1));
        if sorted_langs.is_empty() {
            println!("  none detected");
        } else {
            for (lang, count) in sorted_langs {
                println!("  {lang}: {count}");
            }
        }
    }

    Ok(())
}

async fn seed_refresh_loop(seed_file: Arc<SeedFile>, queue: Arc<UrlQueue>, stats: CrawlStats) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;

        match seed_file.refresh().await {
            Ok(new_urls) if !new_urls.is_empty() => {
                for url in new_urls {
                    match queue.enqueue(&url) {
                        Ok(true) => {
                            stats.record_enqueue();
                            stats.record_new_site();
                            info!("added seed from refreshed {}", url);
                        }
                        Ok(false) => {}
                        Err(err) => error!("failed enqueuing refreshed seed {}: {err}", url),
                    }
                }
            }
            Ok(_) => {}
            Err(err) => {
                error!("failed reloading {}: {err}", seed_file.path.display());
            }
        }
    }
}

fn seed_queue(
    queue: &UrlQueue,
    seeds: &[String],
    defaults: &[String],
    stats: &CrawlStats,
) -> Result<(), QueueError> {
    if queue.len() > 0 {
        return Ok(());
    }

    let seeds_iter: Vec<String> = if seeds.is_empty() && !defaults.is_empty() {
        defaults.to_vec()
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
