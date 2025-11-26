#include "YBResultSet.h"

#include <Poco/Data/RecordSet.h>
#include <stdexcept>

namespace yb::connector {

YBResultSet::YBResultSet(std::shared_ptr<Poco::Data::RecordSet> recordSet)
    : _recordSet(std::move(recordSet)) {}

void YBResultSet::addRow(const Row& row) {
    _rows.push_back(row);
}

const std::vector<YBResultSet::Row>& YBResultSet::rows() const {
    return _rows;
}

bool YBResultSet::empty() const {
    if (_recordSet) return _recordSet->rowCount() == 0;
    return _rows.empty();
}

std::size_t YBResultSet::size() const {
    if (_recordSet) return _recordSet->rowCount();
    return _rows.size();
}

bool YBResultSet::next() {
    if (_recordSet) {
        if (_cursorIndex == static_cast<std::size_t>(-1)) {
            bool hasFirst = _recordSet->moveFirst();
            _cursorIndex = hasFirst ? 0 : 0; // index not used directly with RecordSet
            return hasFirst;
        }
        return _recordSet->moveNext();
    }
    // Fallback to vector-based iteration
    if (_rows.empty()) return false;
    if (_cursorIndex == static_cast<std::size_t>(-1)) {
        _cursorIndex = 0;
        return true;
    }
    if (_cursorIndex + 1 < _rows.size()) {
        ++_cursorIndex;
        return true;
    }
    return false;
}

std::string YBResultSet::getString(const std::string& column) const {
    if (_recordSet) {
        return _recordSet->value(column).convert<std::string>();
    }
    if (_cursorIndex == static_cast<std::size_t>(-1) || _cursorIndex >= _rows.size()) {
        throw std::out_of_range("No current row");
    }
    auto it = _rows[_cursorIndex].find(column);
    if (it == _rows[_cursorIndex].end()) throw std::out_of_range("Column not found");
    return it->second;
}

int YBResultSet::getInt(const std::string& column) const {
    if (_recordSet) {
        return _recordSet->value(column).convert<int>();
    }
    return std::stoi(getString(column));
}

double YBResultSet::getDouble(const std::string& column) const {
    if (_recordSet) {
        return _recordSet->value(column).convert<double>();
    }
    return std::stod(getString(column));
}

} // namespace yb::connector


