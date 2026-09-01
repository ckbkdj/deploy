# Contributing

1. Create a feature branch.
2. Run `gofmt`, `go test ./...`, `bash -n scripts/*.sh`, and `node --check internal/webui/static/app.js`.
3. Add tests for behavior changes, especially 555 normalization, audit decisions, routing, retention, and failure modes.
4. Never commit `.env`, provider keys, database dumps, Kafka client properties, certificates, or production request payloads.
5. Keep event schema changes backward compatible and document them in `docs/EVENT_SCHEMA.md`.
