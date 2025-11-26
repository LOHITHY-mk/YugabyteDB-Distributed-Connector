#pragma once

#include <string>

namespace yb::connector {

class YBConnectionManager;
class YBResultSet;

// Executes SQL against YugabyteDB, returning lightweight results
class YBQueryExecutor {
public:
    explicit YBQueryExecutor(YBConnectionManager& connectionManager);

    YBResultSet executeQuery(const std::string& sql);
    void executeCommand(const std::string& sql);

    // Day 3 APIs
    YBResultSet executeSelect(const std::string& query);
    int executeUpdate(const std::string& query);

    // Day 4 APIs
    int executeInsert(const std::string& query);
    int executeDelete(const std::string& query);

private:
    YBConnectionManager& _connectionManager;
};

} // namespace yb::connector


