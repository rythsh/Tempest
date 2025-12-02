use std::path::Path;

use dashmap::DashSet;
use sled::{self, IVec};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Db(#[from] sled::Error),
    #[error("invalid url {0}")]
    InvalidUrl(String),
    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

#[derive(Clone)]
pub struct UrlQueue {
    db: sled::Db,
    queue: sled::Tree,
    seen: sled::Tree,
    in_memory_seen: DashSet<String>,
}

impl UrlQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, QueueError> {
        std::fs::create_dir_all(path.as_ref())?;
        let db = sled::open(path)?;
        let queue = db.open_tree("queue")?;
        let seen = db.open_tree("seen")?;

        let in_memory_seen = DashSet::new();
        for entry in seen.iter() {
            let (_, val) = entry?;
            if let Ok(url) = String::from_utf8(val.to_vec()) {
                in_memory_seen.insert(url);
            }
        }

        Ok(Self {
            db,
            queue,
            seen,
            in_memory_seen,
        })
    }

     pub fn enqueue(&self, url: &Url) -> Result<bool, QueueError> {
        if url.scheme() != "http" && url.scheme() != "https" {
            return Ok(false);
        }

        if url.host_str().is_none() {
            return Ok(false);
        }

         let mut url_clone = url.clone();
        url_clone.set_fragment(None);
        let url_string = url_clone.as_str().to_string();
        if !self.in_memory_seen.insert(url_string.clone()) {
            return Ok(false);
        }

        let id = self.db.generate_id()?;
        self.queue
            .insert(id.to_be_bytes(), IVec::from(url_string.as_bytes()))?;
        self.seen
            .insert(url_string.as_bytes(), IVec::from(url_string.as_bytes()))?;
        Ok(true)
    }

    pub fn dequeue(&self) -> Result<Option<Url>, QueueError> {
        let next = self.queue.iter().next().transpose()?;
        if let Some((key, value)) = next {
            self.queue.remove(key)?;
            let url_str = String::from_utf8(value.to_vec())?;
            let parsed = Url::parse(&url_str).map_err(|_| QueueError::InvalidUrl(url_str))?;
            return Ok(Some(parsed));
        }

        Ok(None)
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn seen_len(&self) -> usize {
        self.in_memory_seen.len()
    }
}
