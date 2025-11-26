#pragma once

#include <exception>
#include <string>
#include <iostream>

namespace yb::connector {

enum class YBErrorCategory {
    None,
    Connection,
    Timeout,
    Serialization,
    Syntax,
    Unknown
};

struct YBError {
    YBErrorCategory category{YBErrorCategory::Unknown};
    std::string message;
};

// Centralized helper for classifying and formatting errors
class YBErrorHandler {
public:
    static YBErrorCategory classify(const std::exception& ex) noexcept;
    static std::string format(const std::exception& ex) noexcept;
    static inline void logError(const std::string& message) noexcept {
        std::cerr << message << "\n";
    }
    static inline void logQueryError(const std::string& query, const std::string& errorMsg) noexcept {
        std::cerr << "Query error: " << errorMsg << " | SQL: " << query << "\n";
    }
    static inline void handleModificationError(const std::string& op,
                                               const std::string& query,
                                               const std::string& errorMsg) noexcept {
        std::cerr << op << " failed: " << errorMsg << " | SQL: " << query << "\n";
    }
    static inline YBError fromException(const std::exception& ex) noexcept {
        return YBError{ classify(ex), format(ex) };
    }
};

} // namespace yb::connector


