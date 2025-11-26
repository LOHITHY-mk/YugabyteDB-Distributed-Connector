#include "include/YBConnectionManager.h"
#include "include/YBQueryExecutor.h"
#include "include/YBResultSet.h"
#include "include/YBErrorHandler.h"

#include <iostream>

using namespace yb::connector;

int main() {
    std::cout << "Initializing YugabyteDB Distributed Connector..." << std::endl;
    // Example YugabyteDB (PostgreSQL) connection string for Poco::Data::PostgreSQL
    // Adjust host, port, user, password, and db as needed.
    const std::string host = "127.0.0.1";
    const int port = 5433;
    const std::string db = "yugabyte";
    const std::string user = "yugabyte";
    const std::string password = "yugabyte";

    try {
        YBConnectionManager manager("");
        if (!manager.connect(host, port, db, user, password)) {
            auto err = manager.lastError();
            if (err) {
                std::cerr << err->message << "\n";
            }
            return 1;
        }

        YBQueryExecutor executor(manager);
        // Simple sanity query; replace with an existing table if needed
        YBResultSet rs = executor.executeQuery("select version();");

        for (const auto& row : rs.rows()) {
            for (const auto& kv : row) {
                std::cout << kv.first << ": " << kv.second << "\n";
            }
        }

        manager.disconnect();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << YBErrorHandler::format(ex) << "\n";
        return 1;
    }
}


