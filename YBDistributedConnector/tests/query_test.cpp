#include "../include/YBConnectionManager.h"
#include "../include/YBQueryExecutor.h"
#include "../include/YBResultSet.h"
#include "../include/YBErrorHandler.h"

#include <iostream>

using namespace yb::connector;

int main() {
    std::cout << "[Test] Query test starting..." << std::endl;

    YBConnectionManager mgr("");
    if (!mgr.connect("127.0.0.1", 5433, "yugabyte", "yugabyte", "yugabyte")) {
        auto err = mgr.lastError();
        if (err) std::cerr << err->message << std::endl;
        return 1;
    }

    YBQueryExecutor exec(mgr);
    try {
        exec.executeUpdate("DROP TABLE IF EXISTS employees_demo;");
        exec.executeUpdate("CREATE TABLE employees_demo(id INT PRIMARY KEY, name TEXT);");
        exec.executeUpdate("INSERT INTO employees_demo(id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');");

        auto rs = exec.executeSelect("SELECT id, name FROM employees_demo ORDER BY id;");
        while (rs.next()) {
            std::cout << rs.getInt("id") << " - " << rs.getString("name") << std::endl;
        }
    } catch (const std::exception& ex) {
        std::cerr << YBErrorHandler::format(ex) << std::endl;
        mgr.disconnect();
        return 1;
    }

    mgr.disconnect();
    std::cout << "[Test] Query test completed." << std::endl;
    return 0;
}


