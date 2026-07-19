# SPXW Exposure Surface Site

This directory serves the live, read-only SPXW exposure-surface projection. It
does not mount the repository, account data, order state, or the general
`latest/` directory.

## Access

The nginx sidecar shares the existing code-server network namespace and listens
on port `18082`:

`https://code.zh3nyu.com/proxy/18082/`

The browser polls the relative endpoint `api/v1/snapshot` every five seconds.
The endpoint maps only to the dedicated publisher output:

`/srv/data/spx-spark/data/published/spxw-surface/snapshot.json`

The host directory, rather than the JSON inode, is mounted because the publisher
uses atomic rename. The file remains owner-only (`0600`); nginx runs as the same
configurable UID/GID and does not require broader permissions.

## Run

The producer must publish at least one snapshot before the endpoint can return
200. Then start the sidecar:

```bash
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml up -d
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml ps
```

For a non-production manual QA directory, set `SPXW_SURFACE_PUBLISH_DIR` before
starting Compose. Test fixtures must never be copied into `public/`; when the
snapshot is missing or unavailable, the UI intentionally shows an empty state.
