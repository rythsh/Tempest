# Ryth Tempest

A small Rust crawler that collects HTML/text from configured seeds, records metadata, and maintains
the queue and output in `data/`.

## Clone

```sh
git clone https://github.com/ryth-inc/tempest.git
cd tempest
```

## Start

Ensure `sites.csv` lists the URLs you want to seed and adjust `config.yml` if you need to tune the
concurrency, user agent, or directories. Then run:

```sh
cargo run --release
```

The crawler writes to `data/output.jsonl` and stores its queues/databases under `data/`.
Remove or edit `sites.csv` before running if you want to reset the seed list.

## Config tweaks

- `save_images` can be set to `false` to skip fetching assets entirely.
- `image_mime_whitelist` lets you limit saved files to particular MIME types by default (`image/png`, `image/jpeg`, `image/jpg`, `image/gif`, `image/webp`).
