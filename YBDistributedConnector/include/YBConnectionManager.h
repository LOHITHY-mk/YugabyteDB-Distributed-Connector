#pragma once

#include <memory>
#include <string>
#include <Poco/Data/Session.h>
#include <optional>

#include "YBErrorHandler.h"

namespace yb::connector {

// Manages a PostgreSQL-compatible YugabyteDB connection using Poco::Data
class YBConnectionManager {
public:
    explicit YBConnectionManager(const std::string& connectionString);

    void connect();
    bool connect(const std::string& host, int port, const std::string& dbName,
                 const std::string& user, const std::string& password,
                 int maxRetries = 3, int retryDelayMs = 200);
    void disconnect();
    bool isConnected() const;

    Poco::Data::Session& session();

    std::optional<YBError> lastError() const { return _lastError; }

private:
    std::string _connectionString;
    std::unique_ptr<Poco::Data::Session> _session;
    std::optional<YBError> _lastError;
};

} // namespace yb::connector


