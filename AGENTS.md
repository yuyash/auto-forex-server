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

## Compatibility Policy

- Do not preserve backward compatibility in this package at this stage.
- Do not add compatibility aliases, deprecated wrappers, legacy shims, or
  duplicate old/new APIs.
- When an API changes, update all call sites and tests to the new API and remove
  the old implementation outright.

## Type Policy

- Prefer domain objects, enums, and structured models over accepting both an
  object and its serialized `str` form.
- Do not type public or internal APIs as `SomeObject | str` unless the function
  is explicitly a parser/factory at a serialization boundary, or the value is
  inherently textual such as an external ID, file path, protocol field, or log
  field.
- When removing `str` inputs, update all call sites and tests to construct the
  object before calling the API.

## Commands

```bash
uv sync
uv run ruff check .
uv run ruff format .
uv run ty check
```
