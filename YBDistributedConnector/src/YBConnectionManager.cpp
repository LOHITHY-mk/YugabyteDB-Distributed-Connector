#include "YBConnectionManager.h"

#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/PostgreSQL/Connector.h>
#include <Poco/Exception.h>
#include <thread>
#include <chrono>

using Poco::Data::Session;
using Poco::Data::SessionFactory;

namespace yb::connector {

YBConnectionManager::YBConnectionManager(const std::string& connectionString)
    : _connectionString(connectionString) {}

void YBConnectionManager::connect() {
    if (_session) return;
    // Poco PostgreSQL connector DSN example:
    // "host=127.0.0.1;port=5433;user=yugabyte;password=yugabyte;db=yugabyte"
    _session = std::make_unique<Session>(SessionFactory::instance().create("PostgreSQL", _connectionString));
}

bool YBConnectionManager::connect(const std::string& host, int port, const std::string& dbName,
                 const std::string& user, const std::string& password,
                 int maxRetries, int retryDelayMs) {
    using namespace std::chrono_literals;
    _lastError.reset();
    if (_session && _session->isConnected()) return true;

    Poco::Data::PostgreSQL::Connector::registerConnector();

    // Do not log credentials. Keep them out of logs entirely.
    const std::string connStr =
        std::string("host=") + host +
        " port=" + std::to_string(port) +
        " dbname=" + dbName +
        " user=" + user +
        " password=" + password;

    int attempt = 0;
    while (attempt < maxRetries) {
        try {
            ++attempt;
            _session = std::make_unique<Session>("PostgreSQL", connStr);
            if (_session->isConnected()) {
                return true;
            }
        } catch (const Poco::Exception& ex) {
            _lastError = YBErrorHandler::fromException(ex);
            if (attempt >= maxRetries) {
                YBErrorHandler::logError("Connection failed after retries.");
                return false;
            }
        } catch (const std::exception& ex) {
            _lastError = YBErrorHandler::fromException(ex);
            if (attempt >= maxRetries) {
                YBErrorHandler::logError("Connection failed after retries.");
                return false;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retryDelayMs));
    }
    return false;
}

void YBConnectionManager::disconnect() {
    if (_session) {
        _session->close();
        _session.reset();
    }
}

bool YBConnectionManager::isConnected() const {
    return _session && _session->isConnected();
}

Session& YBConnectionManager::session() {
    return *_session;
}

} // namespace yb::connector


