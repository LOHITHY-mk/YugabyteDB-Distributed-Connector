#include "../include/YBConnectionManager.h"
#include "../include/YBQueryExecutor.h"
#include "../include/YBResultSet.h"
#include "../include/YBErrorHandler.h"

#include <iostream>

using namespace yb::connector;

int main() {
    std::cout << "[Integration] Starting..." << std::endl;

    YBConnectionManager mgr("");
    if (!mgr.connect("127.0.0.1", 5433, "yugabyte", "yugabyte", "yugabyte")) {
        std::cerr << "❌ Connection failed!" << std::endl;
        if (auto err = mgr.lastError()) std::cerr << err->message << std::endl;
        return 1;
    }
    std::cout << "✅ Connected to YugabyteDB successfully!" << std::endl;

    YBQueryExecutor exec(mgr);
    try {
        exec.executeUpdate("CREATE TABLE IF NOT EXISTS employees_int(id INT PRIMARY KEY, name TEXT);");
        exec.executeUpdate("DELETE FROM employees_int WHERE id IN (201);");

        int ins = exec.executeInsert("INSERT INTO employees_int (id, name) VALUES (201, 'Sita') ON CONFLICT (id) DO UPDATE SET name='Sita';");
        std::cout << "✅ Data inserted. Rows: " << ins << std::endl;

        auto rs = exec.executeSelect("SELECT id, name FROM employees_int ORDER BY id;");
        std::cout << "✅ Query executed." << std::endl;
        while (rs.next()) {
            std::cout << rs.getInt("id") << " - " << rs.getString("name") << std::endl;
        }

        int upd = exec.executeUpdate("UPDATE employees_int SET name='Sita Devi' WHERE id=201;");
        std::cout << "✅ Data updated. Rows: " << upd << std::endl;

        int del = exec.executeDelete("DELETE FROM employees_int WHERE id=201;");
        std::cout << "✅ Data deleted. Rows: " << del << std::endl;

        std::cout << "✅ Integration test completed successfully!" << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << YBErrorHandler::format(ex) << std::endl;
        mgr.disconnect();
        return 1;
    }

    mgr.disconnect();
    return 0;
}


