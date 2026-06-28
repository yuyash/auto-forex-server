# Server Package Guide

`server` is the backend AutoForexV2 process that coordinates automated trading
tasks.

## Responsibilities

- Run and schedule trading/backtesting tasks.
- Use `core` for domain logic, `snowball` for Snowball strategy logic, and
  `oanda` for OANDA communication.
- Expose gRPC services consumed by `api`.
- Treat `protobuf` as the source of truth for gRPC contracts.

## Boundaries

- Do not expose frontend-facing REST endpoints here; use `api`.
- Do not define `.proto` files here; update `protobuf`.
- Do not implement Snowball strategy internals here; use `snowball`.
- Keep frontend/OpenAPI concerns out of this package.

## Commands

```bash
uv sync
uv run ruff check .
uv run ruff format .
uv run ty check
```
