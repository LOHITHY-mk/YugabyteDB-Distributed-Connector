# Observability & How to See What the Connector Is Doing

Use these steps to watch connections, queries, inserts/updates/deletes, and overall flow.

## 1) Console Output (Recommended Starting Point)
Run the tests and watch printed steps/results:

- connection_test_run.exe — shows connect/auth
- query_test.exe — prints selected rows
- storage_test_run.exe — shows inserted/updated/deleted counts
- integration_test_run.exe — shows the full flow

Build targets:
```powershell
cmake --build build --config Release --target connection_test
cmake --build build --config Release --target query_test
cmake --build build --config Release --target storage_test
cmake --build build --config Release --target integration_test
```
Run binaries (paths may vary if output is redirected):
```powershell
build\Release\connection_test_run.exe
build\Release\query_test.exe
build\Release\storage_test_run.exe
build\Release\integration_test_run.exe
# or (if redirected)
build\tests\bin\Release\storage_test_run.exe
build\tests\bin\Release\integration_test_run.exe
```

## 2) YugabyteDB Web UIs
Start the local DB (see RUNBOOK.md), then open:
- Master UI: http://localhost:7000
- TServer UI: http://localhost:9000

Use the Statements/Queries pages to see SQL activity, latencies, and details.

## 3) Inspect/Trace Queries from the DB
Confirm writes and measure queries directly via ysqlsh inside the container:
```powershell
docker exec -it yb ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d yugabyte -c "select * from employees_store;"
```
Investigate performance:
```powershell
docker exec -it yb ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d yugabyte -c "EXPLAIN ANALYZE select * from employees_store;"
```

## 4) Add Verbose Logging in the App
Use built-in hooks to log failures and queries:
- `YBErrorHandler::logQueryError(query, message)`
- `YBErrorHandler::handleModificationError(op, query, message)`

For deeper visibility, add INFO logs before/after each execution:
- Print the SQL (with placeholders) before `Statement` executes
- Print affected row counts and duration after execution

## 5) See End-to-End Process Flow
- docs/Architecture.md → Final Architecture Flow; Day 3/Day 4 sections for query/storage flows
- RUNBOOK.md → Expected Outputs (what the console should show)

## 6) OS-Level Visibility (Optional)
Windows:
- Use Task Manager (Details tab) or Resource Monitor to view running test processes and I/O
- Verify port usage for YSQL (default 5433):
```powershell
netstat -ano | findstr 5433
```

## Notes
- If re-running storage tests, either delete existing rows or use `ON CONFLICT` upserts to avoid duplicate key errors.
- If Windows blocks overwriting test EXEs (LNK1104), close any open EXE previews, stop the process, or build to a different output path.
