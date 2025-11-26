#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

namespace Poco { namespace Data { class RecordSet; } }

namespace yb::connector {

// Lightweight result holder for tabular data
class YBResultSet {
public:
    using Row = std::unordered_map<std::string, std::string>;

    YBResultSet() = default;

    // Construct from Poco::Data::RecordSet
    explicit YBResultSet(std::shared_ptr<Poco::Data::RecordSet> recordSet);

    void addRow(const Row& row);
    const std::vector<Row>& rows() const;
    bool empty() const;
    std::size_t size() const;

    // Day 3: cursor-style iteration and typed getters
    bool next();
    std::string getString(const std::string& column) const;
    int getInt(const std::string& column) const;
    double getDouble(const std::string& column) const;

private:
    std::vector<Row> _rows;
    std::shared_ptr<Poco::Data::RecordSet> _recordSet;
    std::size_t _cursorIndex{static_cast<std::size_t>(-1)};
};

} // namespace yb::connector


