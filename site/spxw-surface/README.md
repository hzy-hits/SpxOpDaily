# SPXW Notification Image Entry

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../../docs/README.md)（目录核对：2026-10-05）。

The Live Surface and Session Replay websites and their projection/replay code
are deleted. Production only retains this fixed notification-image entry.

The remaining `spxw-surface-entry` container exists only for four fixed,
account-free notification images:

- `https://spx.zh3nyu.com/oi/latest.png`
- `https://spx.zh3nyu.com/strategy-risk/latest.png`
- `https://spx.zh3nyu.com/strategy-risk/gth-latest.png`
- `https://spx.zh3nyu.com/flow/latest.png`

Each exact-match route exposes one atomically replaced PNG. Parent directories,
JSON projections, account data, order state and directory listings remain
unavailable. The strategy-risk sheet is advisory-only and cannot change
strategy authority or enable automatic ordering.

Dashboard aliases `/`, `/live`, `/live/`, `/replay`, `/replay/`, `/sessions`
and `/friday` return HTTP 410. All other unknown routes return 404.

The managed-tunnel ingress remains path-free:

```yaml
- hostname: spx.zh3nyu.com
  service: http://127.0.0.1:18084
```

## Deployment

```bash
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml \
  up -d --remove-orphans spxw-surface-entry
docker compose -f /home/ubuntu/spx-spark/site/spxw-surface/compose.yaml ps
```

Verify the four images return 200, retired dashboard paths return 410, and the
entry container is healthy. Historical surface and replay data are deliberately
retained under `/srv/data/spx-spark/data/published/spxw-surface`; retirement does
not authorize deleting those artifacts.
