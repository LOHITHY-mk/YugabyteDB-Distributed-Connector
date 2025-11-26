#include "YBErrorHandler.h"

#include <Poco/Exception.h>
#include <string>

namespace yb::connector {

YBErrorCategory YBErrorHandler::classify(const std::exception& ex) noexcept {
    // Skeleton classification; refine with specific Yugabyte/PostgreSQL error checks later
    std::string message = ex.what();
    if (const auto* pex = dynamic_cast<const Poco::Exception*>(&ex)) {
        message = pex->displayText();
    }
    if (message.find("timeout") != std::string::npos) return YBErrorCategory::Timeout;
    if (message.find("connection") != std::string::npos) return YBErrorCategory::Connection;
    if (message.find("serialization") != std::string::npos) return YBErrorCategory::Serialization;
    if (message.find("syntax") != std::string::npos) return YBErrorCategory::Syntax;
    return YBErrorCategory::Unknown;
}

std::string YBErrorHandler::format(const std::exception& ex) noexcept {
    if (const auto* pex = dynamic_cast<const Poco::Exception*>(&ex)) {
        return std::string("YB Error: ") + pex->displayText();
    }
    return std::string("YB Error: ") + ex.what();
}

} // namespace yb::connector


