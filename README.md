# obconnector-go

Clean-room OceanBase driver experiments for Go's `database/sql`.

The first milestone is a pure-Go PoC that can connect to an OceanBase Oracle tenant and run:

```sql
select 1 from dual
```

This repository intentionally does not depend on OBCI, Oracle Instant Client, LibOBClient, cgo, or dynamic OceanBase client libraries.

## Current State

- Registers `database/sql` driver names `oceanbase` and `oboracle`.
- Implements a minimal MySQL-compatible wire path with OceanBase Oracle tenant handshake extensions.
- Supports `mysql_native_password`, `COM_QUERY`, streaming text rows, server errors, basic transactions, `Prepare` compatibility, client-side `?` parameter interpolation, connection-pool lifecycle hooks, and basic column type metadata.
- Provides `cmd/obping` for connection experiments.
- Records protocol observations and open questions in `docs/protocol-notes.md`.

The implementation is not production-ready. It is designed to support packet captures and connection experiments without copying LGPL/MariaDB/OBConnector-C source code.

## Usage

```bash
go run ./cmd/obping \
  -host 127.0.0.1 \
  -port 2881 \
  -user 'user@tenant#cluster' \
  -password 'secret' \
  -trace
```

Or pass a DSN directly:

```bash
go run ./cmd/obping -dsn 'oceanbase://user:secret@127.0.0.1:2881/?timeout=5s'
```

Custom connection attributes can be added with `attr.<name>` query parameters:

```text
oceanbase://user:pass@127.0.0.1:2881/?attr._client_name=obconnector-go
```

The CLI also exposes experiment switches:

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:secret@127.0.0.1:2881/?timeout=5s' \
  -trace \
  -query 'select 1 as one from dual' \
  -max-rows 20 \
  -attr '_client_name=obconnector-go' \
  -cap-add 0x0 \
  -cap-drop 0x0 \
  -init 'select 1 from dual'
```

Basic transaction validation:

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:secret@127.0.0.1:2881/?timeout=5s' \
  -tx-test
```

DDL/DML `ExecContext` validation:

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:secret@127.0.0.1:2881/?timeout=5s' \
  -exec-test
```

Client-side `?` parameter validation:

```bash
go run ./cmd/obping \
  -dsn 'oceanbase://user:secret@127.0.0.1:2881/?timeout=5s' \
  -param-test
```

DSN query parameters:

- `trace=true` prints high-level handshake and query details to stderr.
- `attr.<key>=<value>` sends or overrides a connection attribute.
- `cap.add=<uint32>` forces extra capability bits on.
- `cap.drop=<uint32>` forces capability bits off.
- `collation=<uint8>` changes the collation byte in the handshake response.
- `init=<sql>` runs one or more SQL statements immediately after authentication.
- `preset=<name>` changes the client identity preset. The default preset sends the OceanBase Connector/C-style Oracle tenant attributes discovered during the PoC.

For packet capture experiments, see `docs/protocol-notes.md`.

## License

Apache-2.0. See `LICENSE`.
