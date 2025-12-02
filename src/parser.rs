use scraper::{ElementRef, Html, Selector};
use std::collections::HashSet;
use url::Url;

#[derive(Debug, Clone)]
pub struct ParsedPage {
    pub title: Option<String>,
    pub content: String,
    pub links: Vec<Url>,
    pub code_snippets: Vec<String>,
    pub images: Vec<Url>,
}

pub fn parse_html(document: &str, base_url: &Url) -> ParsedPage {
    let html = Html::parse_document(document);
    let title_sel = Selector::parse("title").unwrap();
    let text_selectors = vec!["main", "article", "section", "div", "h1", "h2", "h3", "h4", "p", "li"];
    let link_sel = Selector::parse("a[href]").unwrap();
    let image_sel = Selector::parse("img[src]").unwrap();
    let pre_sel = Selector::parse("pre").unwrap();
    let code_sel = Selector::parse("code").unwrap();

    let title = html
        .select(&title_sel)
        .next()
        .and_then(|el| Some(el.text().collect::<Vec<_>>().join(" ").trim().to_string()))
        .filter(|t| !t.is_empty());

    let mut content_chunks = Vec::new();
    for selector in text_selectors {
        if let Ok(sel) = Selector::parse(selector) {
            for node in html.select(&sel) {
                let text = node.text().collect::<Vec<_>>().join(" ");
                let cleaned = text.trim();
                if !cleaned.is_empty() {
                    content_chunks.push(cleaned.to_string());
                }
            }
        }
    }

    let mut code_snippets = Vec::new();
    let mut seen_snippets = HashSet::new();
    for el in html.select(&pre_sel) {
        if let Some(snippet) = extract_clean_text(el) {
            if seen_snippets.insert(snippet.clone()) {
                code_snippets.push(snippet);
            }
        }
    }
    for el in html.select(&code_sel) {
        if let Some(snippet) = extract_clean_text(el) {
            if seen_snippets.insert(snippet.clone()) {
                code_snippets.push(snippet);
            }
        }
    }

    let mut links = Vec::new();
    for el in html.select(&link_sel) {
        if let Some(href) = el.value().attr("href") {
            if let Ok(url) = base_url.join(href) {
                if !url.scheme().starts_with("http") {
                    continue;
                }
                if let Some(host) = url.host_str() {
                    let host = host.to_lowercase();
                    if host.ends_with("wikipedia.org") || host.ends_with("wikimedia.org") {
                        if !url.path().starts_with("/wiki/Category:")
                            && !url.path().starts_with("/wiki/Portal:")
                            && !url.path().starts_with("/wiki/Template:")
                        {
                            continue;
                        }
                    }
                    if host.contains("facebook.com")
                        || host.contains("twitter.com")
                        || host.contains("x.com")
                        || host.contains("instagram.com")
                        || host.contains("tiktok.com")
                        || host.contains("linkedin.com")
                    {
                        continue;
                    }
                }
                links.push(url);
            }
        }
    }

    let mut images = Vec::new();
    for el in html.select(&image_sel) {
        if let Some(src) = el.value().attr("src") {
            if let Ok(url) = base_url.join(src) {
                if url.scheme().starts_with("http") {
                    images.push(url);
                }
            }
        }
    }

    ParsedPage {
        title,
        content: content_chunks.join("\n"),
        links,
        code_snippets,
        images,
    }
}

fn extract_clean_text(node: ElementRef) -> Option<String> {
    let text = node.text().collect::<Vec<_>>().join(" ");
    let cleaned = text.trim();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned.to_string())
    }
}
