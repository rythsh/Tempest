use url::Url;

const TRACKING_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
];

pub fn clean_text(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn normalize_url(url: &Url) -> Option<Url> {
    let mut normalized = url.clone();
    normalized.set_fragment(None);

    let mut query_pairs: Vec<(String, String)> = normalized
        .query_pairs()
        .filter(|(k, _)| !TRACKING_PARAMS.contains(&k.as_ref()))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    query_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    if query_pairs.is_empty() {
        normalized.set_query(None);
    } else {
        normalized
            .query_pairs_mut()
            .clear()
            .extend_pairs(query_pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }

    Some(normalized)
}
