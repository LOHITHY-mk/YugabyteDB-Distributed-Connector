#include "YBQueryExecutor.h"
#include "YBConnectionManager.h"
#include "YBResultSet.h"
#include "YBErrorHandler.h"

#include <Poco/Data/RecordSet.h>
#include <Poco/Data/Session.h>
#include <Poco/Data/Statement.h>
#include <Poco/Data/DataException.h>

using Poco::Data::RecordSet;
using Poco::Data::Statement;

namespace yb::connector {

YBQueryExecutor::YBQueryExecutor(YBConnectionManager& connectionManager)
    : _connectionManager(connectionManager) {}

YBResultSet YBQueryExecutor::executeQuery(const std::string& sql) {
    YBResultSet result;
    try {
        Statement select(_connectionManager.session());
        select << sql, Poco::Data::Keywords::now;

        RecordSet rs(select);
        bool more = rs.moveFirst();
        while (more) {
            YBResultSet::Row row;
            for (std::size_t i = 0; i < rs.columnCount(); ++i) {
                const std::string colName = rs.columnName(static_cast<unsigned int>(i));
                const std::string value = rs.value(static_cast<unsigned int>(i)).convert<std::string>();
                row[colName] = value;
            }
            result.addRow(row);
            more = rs.moveNext();
        }
    } catch (const std::exception&) {
        throw; // Let caller handle via YBErrorHandler
    }
    return result;
}

void YBQueryExecutor::executeCommand(const std::string& sql) {
    try {
        Statement cmd(_connectionManager.session());
        cmd << sql, Poco::Data::Keywords::now;
    } catch (const std::exception&) {
        throw;
    }
}

YBResultSet YBQueryExecutor::executeSelect(const std::string& query) {
    try {
        Statement stmt(_connectionManager.session());
        stmt << query, Poco::Data::Keywords::now;
        auto rs = std::make_shared<RecordSet>(stmt);
        return YBResultSet(rs);
    } catch (const Poco::Data::DataException& ex) {
        YBErrorHandler::logQueryError(query, ex.displayText());
        throw;
    } catch (const Poco::Exception& ex) {
        YBErrorHandler::logQueryError(query, ex.displayText());
        throw;
    } catch (const std::exception& ex) {
        YBErrorHandler::logQueryError(query, ex.what());
        throw;
    }
}

int YBQueryExecutor::executeUpdate(const std::string& query) {
    try {
        Statement stmt(_connectionManager.session());
        stmt << query, Poco::Data::Keywords::now;
        // Statement::execute returns rows extracted/affected depending on statement
        return static_cast<int>(stmt.execute());
    } catch (const Poco::Data::DataException& ex) {
        YBErrorHandler::handleModificationError("UPDATE", query, ex.displayText());
        // Attempt rollback if a transaction is in progress
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const Poco::Exception& ex) {
        YBErrorHandler::handleModificationError("UPDATE", query, ex.displayText());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const std::exception& ex) {
        YBErrorHandler::handleModificationError("UPDATE", query, ex.what());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    }
}

int YBQueryExecutor::executeInsert(const std::string& query) {
    try {
        Statement stmt(_connectionManager.session());
        stmt << query, Poco::Data::Keywords::now;
        return static_cast<int>(stmt.execute());
    } catch (const Poco::Data::DataException& ex) {
        YBErrorHandler::handleModificationError("INSERT", query, ex.displayText());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const Poco::Exception& ex) {
        YBErrorHandler::handleModificationError("INSERT", query, ex.displayText());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const std::exception& ex) {
        YBErrorHandler::handleModificationError("INSERT", query, ex.what());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    }
}

int YBQueryExecutor::executeDelete(const std::string& query) {
    try {
        Statement stmt(_connectionManager.session());
        stmt << query, Poco::Data::Keywords::now;
        return static_cast<int>(stmt.execute());
    } catch (const Poco::Data::DataException& ex) {
        YBErrorHandler::handleModificationError("DELETE", query, ex.displayText());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const Poco::Exception& ex) {
        YBErrorHandler::handleModificationError("DELETE", query, ex.displayText());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    } catch (const std::exception& ex) {
        YBErrorHandler::handleModificationError("DELETE", query, ex.what());
        try { if (_connectionManager.session().isTransaction()) _connectionManager.session().rollback(); } catch (...) {}
        throw;
    }
}

} // namespace yb::connector


