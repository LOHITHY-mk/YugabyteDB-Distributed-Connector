#include "../include/YBConnectionManager.h"
#include "../include/YBErrorHandler.h"

#include <iostream>

using namespace yb::connector;

int main() {
    std::cout << "[Test] Starting connection tests..." << std::endl;

    // Valid credentials test (adjust to your environment)
    {
        YBConnectionManager mgr("");
        bool ok = mgr.connect("127.0.0.1", 5433, "yugabyte", "yugabyte", "yugabyte");
        std::cout << (ok ? "[Valid] PASS" : "[Valid] FAIL") << std::endl;
        mgr.disconnect();
    }

    // Invalid credentials test
    {
        YBConnectionManager mgr("");
        bool ok = mgr.connect("127.0.0.1", 5433, "yugabyte", "wrong", "wrong");
        if (!ok) {
            auto err = mgr.lastError();
            if (err) std::cout << "[Invalid] Expected failure: " << err->message << std::endl;
            else std::cout << "[Invalid] Expected failure (no error captured)" << std::endl;
        } else {
            std::cout << "[Invalid] Unexpected success" << std::endl;
        }
        mgr.disconnect();
    }

    return 0;
}


