# YBDistributedConnector — Architecture (Day 1)

## Overview
YBDistributedConnector is a C++ library that provides a clean abstraction for connecting to YugabyteDB (PostgreSQL-compatible), executing SQL for data retrieval and storage, and handling distributed database errors gracefully. It is built using Poco C++ Libraries: Poco::Data, Poco::Data::PostgreSQL, Poco::Net, and Poco::JSON.

## Architecture Diagram (Textual)

Components:
- YBConnectionManager: Manages a Poco::Data::Session lifecycle to YugabyteDB.
- YBQueryExecutor: Executes SQL queries/commands and maps results.
- YBResultSet: Lightweight, tabular in-memory result container.
- YBErrorHandler: Centralized classification and formatting of errors.

Data flow (typical):
YBQueryExecutor -> YBConnectionManager.session() -> PostgreSQL Driver -> YugabyteDB Cluster

## Responsibilities
- YBConnectionManager
  - Open/close connections using PostgreSQL connector DSN.
  - Provide connection status and access to the Session.
- YBQueryExecutor
  - Provide executeQuery(sql) and executeCommand(sql).
  - Convert rows to YBResultSet entries.
- YBResultSet
  - Store rows as key/value pairs; basic iteration and size checks.
- YBErrorHandler
  - Categorize errors (Connection, Timeout, Serialization, Syntax, Unknown).
  - Return readable error messages for logs and clients.

## Error Handling and Retry Strategy (Day 2 additions)
- Parameterized connect(host,port,db,user,password) with bounded retry (default 3 attempts, small delay).
- Exceptions are classified into categories; last error is exposed for diagnostics without leaking credentials.
- Authentication failures produce a structured `YBError` (category + message) and are not retried aggressively.
- Future: SQLSTATE-based mapping, exponential backoff, circuit breaker, idempotency awareness.

## Data Retrieval and Query Flow (Day 3 additions)
- Application → YBQueryExecutor → Poco::Statement → Poco::RecordSet → YBResultSet → Application
- `executeSelect(sql)` builds a Poco::Statement, executes immediately, wraps a `RecordSet`, and returns `YBResultSet` with cursor-style iteration (`next`, `getString`, `getInt`, `getDouble`).
- `executeUpdate(sql)` executes DML and returns affected rows; on failure it logs via `logQueryError` and attempts rollback if a transaction is active.
- `YBResultSet` supports both legacy row-vector access and cursor-based typed getters for efficient iteration.
- Errors from Poco exceptions are classified and formatted by `YBErrorHandler`; query-specific errors are logged with the SQL text.

## Data Storage and Modification Flow (Day 4 additions)
- Application → YBQueryExecutor → Poco::Statement → YugabyteDB
- Write methods: `executeInsert(sql)`, `executeUpdate(sql)`, `executeDelete(sql)` return affected-row counts, using the same `Session` from `YBConnectionManager`.
- Prefer prepared statements with placeholders and `use(...)` bindings to avoid SQL injection and to enable driver optimizations.
- On failures, `YBErrorHandler::handleModificationError(op, sql, message)` logs operation type, SQL, and cause; a rollback is attempted if a transaction is active.
- Considerations for future iterations: explicit transactions for multi-statement units, retry on transient conflicts, idempotent writes, and batch/bulk operations.

## Final Architecture Flow (Day 5)
Application → YBConnectionManager → YBQueryExecutor → YBResultSet / YBErrorHandler → YugabyteDB Cluster

- Connection Manager ensures sessions are created and reused where possible.
- Query Executor centralizes all SQL execution (selects and DML) and error logging.
- Result Set provides efficient, typed access to result data.
- Error Handler normalizes exceptions and provides op-aware logging for debugging and recovery.

## Performance Optimizations
- Prepared Statements: Use `use(...)` bindings and positional placeholders ($1, $2, ...) to reduce parse/plan cost for repeated queries.
- Connection Reuse: Keep a single `Session` per process/thread as appropriate; future work includes pooling and shard-aware routing.
- Retries/Backoff: Only for transient errors; avoid retrying non-idempotent writes unless designed for idempotence (e.g., UPSERTs).
- Logging Levels: Prefer configurable log levels to reduce noise in production while preserving critical failures.

## Error Recovery Flow
- Catch driver exceptions at execution boundaries.
- Classify (connection, timeout, serialization, syntax) and format messages.
- Attempt rollback if a transaction is active.
- Surface structured `YBError` for callers to implement compensations.
