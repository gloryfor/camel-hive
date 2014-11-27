package org.apache.camel.component.hive;

import org.apache.camel.RuntimeCamelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * based on camel-jdbc ResultSetIterator
 */
public class ResultSetIterator implements Iterator<Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(ResultSetIterator.class);

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final Column[] columns;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ResultSetIterator(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.statement = this.resultSet.getStatement();
        this.connection = this.statement.getConnection();

        ResultSetMetaData metaData = resultSet.getMetaData();
        columns = new Column[metaData.getColumnCount()];
        for (int i = 0; i < columns.length; i++) {
            int columnNumber = i + 1;
            String columnName = getColumnName(metaData, columnNumber);
            int columnType = metaData.getColumnType(columnNumber);
            if (columnType == Types.CLOB || columnType == Types.BLOB) {
                columns[i] = new BlobColumn(columnName, columnNumber);
            } else {
                columns[i] = new DefaultColumn(columnName, columnNumber);
            }
        }

        loadNext();
    }

    @Override
    public boolean hasNext() {
        return !closed.get();
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        try {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (Column column : columns) {
                row.put(column.getName(), column.getValue(resultSet));
            }
            loadNext();
            return row;
        } catch (SQLException e) {
            close();
            throw new RuntimeCamelException("Cannot process result", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Cannot remove from a database result");
    }

    public Set<String> getColumnNames() {
        // New copy each time in order to ensure immutability
        Set<String> columnNames = new HashSet<String>(columns.length);
        for (Column column : columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            safeCloseResultSet();
            safeCloseStatement();
        }
    }

    public void closeConnection() {
        safeCloseConnection();
    }

    private void loadNext() throws SQLException {
        boolean hasNext = resultSet.next();
        if (!hasNext) {
            close();
        }
    }

    private void safeCloseResultSet() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            LOG.warn("Error by closing result set: " + e, e);
        }
    }

    private void safeCloseStatement() {
        try {
            statement.close();
        } catch (SQLException e) {
            LOG.warn("Error by closing statement: " + e, e);
        }
    }

    private void safeCloseConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("Error by closing connection: " + e, e);
        }
    }

    private static String getColumnName(ResultSetMetaData metaData, int columnNumber) throws SQLException {
        return metaData.getColumnLabel(columnNumber);
    }

    private interface Column {

        String getName();

        Object getValue(ResultSet resultSet) throws SQLException;
    }

    private static final class DefaultColumn implements Column {
        private final int columnNumber;
        private final String name;

        private DefaultColumn(String name, int columnNumber) {
            this.name = name;
            this.columnNumber = columnNumber;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Object getValue(ResultSet resultSet) throws SQLException {
            return resultSet.getObject(columnNumber);
        }
    }

    private static final class BlobColumn implements Column {
        private final int columnNumber;
        private final String name;

        private BlobColumn(String name, int columnNumber) {
            this.name = name;
            this.columnNumber = columnNumber;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Object getValue(ResultSet resultSet) throws SQLException {
            return resultSet.getString(columnNumber);
        }
    }
}
