# YBDistributedConnector

C++ connector library for YugabyteDB (PostgreSQL-compatible) built on Poco C++ Libraries. Day 1 provides the project skeleton, CMake configuration, core class stubs, and architecture documentation. Day 2 adds connection/auth with retries and a basic test.

## Features 
- CMake-based build with Poco::Data, Poco::Data::PostgreSQL, Poco::Net, Poco::JSON
- Core components: YBConnectionManager, YBQueryExecutor, YBResultSet, YBErrorHandler
- Parameterized connect(host, port, db, user, password) with bounded retries
- Structured error object (`YBError`) and simple logging API
- Minimal example `main.cpp` that prints a startup message and runs a simple query
- Basic connection test in `tests/connection_test.cpp`

## Prerequisites
- CMake 3.15+
- A C++17 compiler (MSVC, Clang, or GCC)
- Poco C++ Libraries with PostgreSQL connector
- PostgreSQL client libraries (libpq) required by Poco::Data::PostgreSQL

### Installing Dependencies (Windows via vcpkg)
1. Install vcpkg and integrate with your toolchain.
2. Install Poco and libpq:
   ```powershell
   vcpkg install poco[net,json,data,postgresql] libpq
   ```
3. Point CMake to vcpkg toolchain:
   ```powershell
   cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE="C:/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake"
   cmake --build build --config Release
   ```

### Installing Dependencies (Linux/macOS)
- Using system packages (example Ubuntu):
  ```bash
  sudo apt update
  sudo apt install cmake g++ libpoco-dev libpq-dev
  ```
- Or build Poco with PostgreSQL enabled from source.

### Manual Linking Notes
- The CMake project uses `find_package(Poco REQUIRED COMPONENTS Data DataPostgreSQL Net JSON)` and links against:
  - `Poco::Data`, `Poco::DataPostgreSQL`, `Poco::Net`, `Poco::JSON`
- Ensure `libpq` is available on the system path/loader path at runtime.

## Build
From the repository root:
```bash
cmake -B build -S .
cmake --build build
```

## Run Example
From the build folder:
```bash
./yb_connector_example
```
`main.cpp` prints:
```
Initializing YugabyteDB Distributed Connector...
```
It uses the parameterized `connect(host, port, db, user, password)`.

## Run Tests
Build targets:
```powershell
cmake --build build --config Release --target connection_test
cmake --build build --config Release --target query_test
cmake --build build --config Release --target storage_test
cmake --build build --config Release --target integration_test
```
Run binaries:
```powershell
# connection
build\Release\connection_test_run.exe

# query
build\Release\query_test.exe

# storage (may be under tests/bin if output was redirected)
build\Release\storage_test_run.exe
# or
build\tests\bin\Release\storage_test_run.exe

# integration (may be under tests/bin if output was redirected)
build\Release\integration_test_run.exe
# or
build\tests\bin\Release\integration_test_run.exe
```
Expected integration output (example):
```
✅ Connected to YugabyteDB successfully!
✅ Data inserted.
✅ Query executed.
✅ Data updated.
✅ Data deleted.
✅ Integration test completed successfully!
```

## Project Layout
```
/YBDistributedConnector
├── CMakeLists.txt
├── include/
│   ├── YBConnectionManager.h
│   ├── YBQueryExecutor.h
│   ├── YBResultSet.h
│   └── YBErrorHandler.h
├── src/
│   ├── YBConnectionManager.cpp
│   ├── YBQueryExecutor.cpp
│   ├── YBResultSet.cpp
│   └── YBErrorHandler.cpp
├── docs/
│   └── Architecture.md
├── tests/
│   ├── connection_test.cpp
│   ├── query_test.cpp
│   ├── storage_test.cpp
│   └── integration_test.cpp
├── README.md
└── main.cpp
```

## Notes
- Ensure the PostgreSQL port matches your YugabyteDB tserver SQL port (default 5433).
- For production use, add connection pooling, exponential backoff, SQLSTATE-based error mapping, and prepared statements with parameter binding.
