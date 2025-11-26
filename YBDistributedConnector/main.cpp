#include "include/YBConnectionManager.h"
#include "include/YBQueryExecutor.h"
#include "include/YBResultSet.h"
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <iostream>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/StreamCopier.h>

using namespace yb::connector;

class QueryHandler : public Poco::Net::HTTPRequestHandler {
public:
    QueryHandler(YBConnectionManager& manager, YBQueryExecutor& executor)
        : _manager(manager), _executor(executor) {}

    void handleRequest(Poco::Net::HTTPServerRequest& request, Poco::Net::HTTPServerResponse& response) override {
        response.setContentType("application/json");
        Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
        try {
            Poco::JSON::Parser parser;
            std::istream& istr = request.stream();
            auto parsed = parser.parse(istr).extract<Poco::JSON::Object::Ptr>();
            std::string sql = parsed->getValue<std::string>("query");
            YBResultSet rs = _executor.executeQuery(sql);
            Poco::JSON::Array::Ptr rows = new Poco::JSON::Array;
            for (const auto& row : rs.rows()) {
                Poco::JSON::Object::Ptr rowObj = new Poco::JSON::Object;
                for (const auto& kv : row) {
                    rowObj->set(kv.first, kv.second);
                }
                rows->add(rowObj);
            }
            result->set("rows", rows);
        } catch (const std::exception& ex) {
            result->set("error", ex.what());
        }
        std::ostream& ostr = response.send();
        Poco::JSON::Stringifier::stringify(result, ostr);
    }
private:
    YBConnectionManager& _manager;
    YBQueryExecutor& _executor;
};

class ConnectHandler : public Poco::Net::HTTPRequestHandler {
public:
    ConnectHandler(YBConnectionManager& manager) : _manager(manager) {}
    void handleRequest(Poco::Net::HTTPServerRequest& request, Poco::Net::HTTPServerResponse& response) override {
        response.setContentType("application/json");
        Poco::JSON::Object::Ptr result = new Poco::JSON::Object;
        try {
            Poco::JSON::Parser parser;
            std::istream& istr = request.stream();
            auto parsed = parser.parse(istr).extract<Poco::JSON::Object::Ptr>();
            std::string host = parsed->getValue<std::string>("host");
            int port = parsed->getValue<int>("port");
            std::string db = parsed->getValue<std::string>("db");
            std::string user = parsed->getValue<std::string>("user");
            std::string password = parsed->getValue<std::string>("password");
            bool connected = _manager.connect(host, port, db, user, password);
            result->set("connected", connected);
        } catch (const std::exception& ex) {
            result->set("error", ex.what());
        }
        std::ostream& ostr = response.send();
        Poco::JSON::Stringifier::stringify(result, ostr);
    }
private:
    YBConnectionManager& _manager;
};

class HandlerFactory : public Poco::Net::HTTPRequestHandlerFactory {
public:
    HandlerFactory(YBConnectionManager& manager, YBQueryExecutor& executor)
        : _manager(manager), _executor(executor) {}
    Poco::Net::HTTPRequestHandler* createRequestHandler(const Poco::Net::HTTPServerRequest& request) override {
        if (request.getURI() == "/connect")
            return new ConnectHandler(_manager);
        if (request.getURI() == "/query")
            return new QueryHandler(_manager, _executor);
        return nullptr;
    }
private:
    YBConnectionManager& _manager;
    YBQueryExecutor& _executor;
};

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
        manager.connect(host, port, db, user, password);
        YBQueryExecutor executor(manager);

        Poco::Net::ServerSocket svs(8080); // HTTP on port 8080
        Poco::Net::HTTPServer srv(new HandlerFactory(manager, executor), svs, new Poco::Net::HTTPServerParams);
        std::cout << "HTTP API server started at http://localhost:8080" << std::endl;
        srv.start();
        std::cout << "Press Enter to quit." << std::endl;
        std::cin.get();
        srv.stop();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << YBErrorHandler::format(ex) << "\n";
        return 1;
    }
}


