# YBDistributedConnector — Runbook (From Scratch)

This guide walks you from a clean machine to running the connector, example app, and tests.

## 1) Prerequisites
- CMake 3.15+
- C++17 toolchain (MSVC/Clang/GCC)
- Git
- Docker (recommended to run a local YugabyteDB)

## 2) Install Dependencies (Windows via vcpkg)
1. Install vcpkg:
   ```powershell
   git clone https://github.com/microsoft/vcpkg C:\vcpkg
   C:\vcpkg\bootstrap-vcpkg.bat
   ```
2. Install Poco and libpq:
   ```powershell
   C:\vcpkg\vcpkg.exe install poco[net,json,data,postgresql] libpq
   ```

Linux/macOS (example Ubuntu):
```bash
sudo apt update
sudo apt install -y cmake g++ libpoco-dev libpq-dev docker.io
```

## 3) Start YugabyteDB Locally (Docker)
PowerShell (single line):
```powershell
docker run -d --name yb -p 7000:7000 -p 9000:9000 -p 5433:5433 -p 9042:9042 yugabytedb/yugabyte:latest bin/yugabyted start --daemon=false
```
Check UIs:
- Master: http://localhost:7000
- TServer: http://localhost:9000

Optional SQL check:
```powershell
docker exec -it yb ysqlsh -h 127.0.0.1 -p 5433 -U yugabyte -d yugabyte -c "select version();"
```

## 4) Configure and Build
From repository root (the folder containing `YBDistributedConnector/`):

Windows (vcpkg toolchain):
```powershell
cmake -B build -S YBDistributedConnector -DCMAKE_TOOLCHAIN_FILE="C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
cmake --build build --config Release
```

Linux/macOS:
```bash
cmake -B build -S YBDistributedConnector
cmake --build build --config Release
```

## 5) Run Example
```powershell
build\Release\yb_connector_example.exe
```
Expected: prints the YugabyteDB version.

## 6) Run Tests
Build targets:
```powershell
cmake --build build --config Release --target connection_test
cmake --build build --config Release --target query_test
cmake --build build --config Release --target storage_test
cmake --build build --config Release --target integration_test
```
Run binaries:
```powershell
# Connection
build\Release\connection_test_run.exe

# Query
build\Release\query_test.exe

# Storage (output may be redirected under tests/bin)
build\Release\storage_test_run.exe
# or
build\tests\bin\Release\storage_test_run.exe

# Integration (output may be redirected under tests/bin)
build\Release\integration_test_run.exe
# or
build\tests\bin\Release\integration_test_run.exe
```

## 7) Configuration Notes
- Connection parameters are set in `main.cpp` and tests (host, port=5433, db, user, password). Adjust for your environment.
- PostgreSQL prepared statements use positional placeholders: `$1, $2, ...`.

## 8) Troubleshooting (Windows)
- LNK1104 cannot open file (EXE): File is locked by an editor or antivirus.
  - Close IDE tabs that preview the EXE; stop the running process:
    ```powershell
    Get-Process <name> -ErrorAction SilentlyContinue | Stop-Process -Force
    Remove-Item .\build\Release\<name>.exe -Force -ErrorAction SilentlyContinue
    cmake --build build --config Release --target <target> --clean-first -- /m:1
    ```
- Poco DataPostgreSQL missing PostgreSQL target:
  - Ensure `find_package(PostgreSQL REQUIRED)` is present (it is) and `libpq` is installed (vcpkg installs it).

## 9) Expected Outputs
- Example: prints `version: PostgreSQL ... YB-<version> ...`
- Connection test: `[Valid] PASS` and expected failure for invalid creds.
- Query test: prints rows from demo table.
- Storage test: prints inserted/updated/deleted row counts (idempotent with `ON CONFLICT`).
- Integration test: prints success messages for each operation and final completion.

## 10) Next Steps
- Configure connection pooling and add transaction helpers.
- Add CI to build and run tests against a containerized YugabyteDB.
- Enable structured logging and metrics.
