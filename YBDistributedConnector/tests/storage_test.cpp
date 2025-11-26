#include "../include/YBConnectionManager.h"
#include "../include/YBQueryExecutor.h"
#include "../include/YBErrorHandler.h"

#include <iostream>

using namespace yb::connector;

int main() {
    std::cout << "[Test] Storage test starting..." << std::endl;

    YBConnectionManager mgr("");
    if (!mgr.connect("127.0.0.1", 5433, "yugabyte", "yugabyte", "yugabyte")) {
        auto err = mgr.lastError();
        if (err) std::cerr << err->message << std::endl;
        return 1;
    }

    YBQueryExecutor exec(mgr);

    try {
        exec.executeUpdate("CREATE TABLE IF NOT EXISTS employees_store(id INT PRIMARY KEY, name TEXT);");
        // Ensure idempotent runs
        exec.executeUpdate("DELETE FROM employees_store WHERE id IN (101,102);");

        // Parameterized insert example (preferred)
        {
            int id = 101;
            std::string name = "Ravi";
            Poco::Data::Statement stmt(mgr.session());
            // PostgreSQL uses positional placeholders: $1, $2, ...
            stmt << "INSERT INTO employees_store (id, name) VALUES($1, $2) "
                    "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
                Poco::Data::Keywords::use(id),
                Poco::Data::Keywords::use(name);
            std::cout << "Inserted rows (param): " << (int)stmt.execute() << std::endl;
        }

        int inserted = exec.executeInsert("INSERT INTO employees_store (id, name) VALUES (102, 'Ravi') ON CONFLICT (id) DO UPDATE SET name='Ravi';");
        std::cout << "Inserted rows: " << inserted << std::endl;

        int updated = exec.executeUpdate("UPDATE employees_store SET name='Ravi Kumar' WHERE id=102;");
        std::cout << "Updated rows: " << updated << std::endl;

        int deleted = exec.executeDelete("DELETE FROM employees_store WHERE id=102;");
        std::cout << "Deleted rows: " << deleted << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << YBErrorHandler::format(ex) << std::endl;
        mgr.disconnect();
        return 1;
    }

    mgr.disconnect();
    std::cout << "[Test] Storage test completed." << std::endl;
    return 0;
}


