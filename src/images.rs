use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::{
    fetcher::{FetchError, Fetcher},
    store::hash_bytes,
};
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum ImageError {
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct ImageStore {
    base: PathBuf,
    allowed_mimes: Vec<String>,
}

impl ImageStore {
    pub fn open(base: impl AsRef<Path>, allowed_mimes: &[String]) -> Result<Self, std::io::Error> {
        let path = base.as_ref().join("images");
        fs::create_dir_all(&path)?;
        let allowed = allowed_mimes
            .iter()
            .map(|m| m.to_ascii_lowercase())
            .collect::<Vec<_>>();
        Ok(Self {
            base: path,
            allowed_mimes: allowed,
        })
    }

    pub async fn save_image(
        &self,
        fetcher: &Fetcher,
        url: &Url,
    ) -> Result<Option<PathBuf>, ImageError> {
        let result = fetcher.get(url).await?;
        if !self.is_allowed(result.content_type.as_deref(), url) {
            return Ok(None);
        }
        let domain = url.host_str().unwrap_or("unknown").replace(':', "_");
        let dir = self.base.join(&domain);
        fs::create_dir_all(&dir)?;
        let hash = hash_bytes(&result.body);
        let ext = Self::image_extension(url, result.content_type.as_deref());
        let file_name = format!("{domain}_{hash}.{ext}");
        let path = dir.join(file_name);
        if !path.exists() {
            fs::write(&path, &result.body)?;
        }
        Ok(Some(path))
    }

    fn is_allowed(&self, content_type: Option<&str>, url: &Url) -> bool {
        if let Some(ct) = content_type {
            let mime = ct
                .split(';')
                .next()
                .unwrap_or(ct)
                .trim()
                .to_ascii_lowercase();
            if self.allowed_mimes.contains(&mime) {
                return true;
            }
        }
        if let Some(last_segment) = url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
        {
            if let Some(ext) = last_segment.split('.').last() {
                let candidate = format!("image/{}", ext.to_ascii_lowercase());
                if self.allowed_mimes.contains(&candidate) {
                    return true;
                }
            }
        }
        false
    }

    fn image_extension(url: &Url, content_type: Option<&str>) -> String {
        if let Some(last_segment) = url
            .path_segments()
            .and_then(|mut segments| segments.next_back())
        {
            if let Some(ext) = last_segment.split('.').last() {
                let cleaned = Self::clean_extension(ext);
                if !cleaned.is_empty() {
                    return cleaned;
                }
            }
        }

        if let Some(ct) = content_type {
            let mime = ct.split(';').next().unwrap_or(ct);
            if let Some(candidate) = mime.split('/').nth(1) {
                let cleaned = Self::clean_extension(candidate);
                if !cleaned.is_empty() {
                    return cleaned;
                }
            }
        }

        "bin".to_string()
    }

    fn clean_extension(value: &str) -> String {
        let trimmed = value
            .trim()
            .trim_matches(|c: char| !c.is_ascii_alphanumeric());
        trimmed.to_ascii_lowercase()
    }
}
