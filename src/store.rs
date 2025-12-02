use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use dashmap::DashSet;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sled::{self, IVec};
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("db error: {0}")]
    Db(#[from] sled::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageRecord {
    pub requested_url: String,
    pub final_url: String,
    pub title: Option<String>,
    pub content: String,
    pub content_length: usize,
    pub word_count: usize,
    pub status: u16,
    pub content_type: Option<String>,
    pub fetched_at: DateTime<Utc>,
    pub outbound_links: usize,
}

#[derive(Clone)]
pub struct DataStore {
    hashes: Arc<DashSet<String>>,
    hash_tree: sled::Tree,
    output: Arc<Mutex<File>>,
}

impl DataStore {
    pub fn open(base: impl AsRef<Path>) -> Result<Self, StoreError> {
        let base = base.as_ref();
        std::fs::create_dir_all(base)?;

        let db = sled::open(base.join("content.db"))?;
        let hash_tree = db.open_tree("hashes")?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(base.join("output.jsonl"))?;

        let hashes = Arc::new(DashSet::new());
        for entry in hash_tree.iter() {
            if let Ok((key, _)) = entry {
                hashes.insert(bytes_to_hex(&key));
            }
        }

        Ok(Self {
            hashes,
            hash_tree,
            output: Arc::new(Mutex::new(file)),
        })
    }

    pub async fn write_record(&self, record: &PageRecord) -> Result<bool, StoreError> {
        let hash = hash_content(&record.content);

        if !self.hashes.insert(hash.clone()) {
            return Ok(false);
        }

        if let Err(err) = self.hash_tree.insert(hash.as_bytes(), IVec::from(&[])) {
            self.hashes.remove(&hash);
            return Err(err.into());
        }

        let mut file = self.output.lock().await;
        if let Err(err) = (|| -> Result<(), StoreError> {
            serde_json::to_writer(&mut *file, record)?;
            file.write_all(b"\n")?;
            file.flush()?;
            Ok(())
        })() {
            self.hashes.remove(&hash);
            self.hash_tree.remove(hash.as_bytes())?;
            return Err(err);
        }
        Ok(true)
    }
}

pub fn hash_content(content: &str) -> String {
    hash_bytes(content.as_bytes())
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let result = hasher.finalize();
    hex::encode(result)
}

fn bytes_to_hex(bytes: &IVec) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

pub fn build_record(
    requested_url: &Url,
    final_url: &Url,
    title: Option<String>,
    content: String,
    status: u16,
    content_type: Option<String>,
    fetched_at: DateTime<Utc>,
    content_length: usize,
    word_count: usize,
    outbound_links: usize,
) -> PageRecord {
    PageRecord {
        requested_url: requested_url.to_string(),
        final_url: final_url.to_string(),
        title,
        content,
        content_length,
        word_count,
        status,
        content_type,
        fetched_at,
        outbound_links,
    }
}
