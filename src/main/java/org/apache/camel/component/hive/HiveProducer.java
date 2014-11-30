package org.apache.camel.component.hive;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.spi.Synchronization;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class HiveProducer extends DefaultProducer {

    private static final Logger LOG = LoggerFactory.getLogger(HiveProducer.class);
    private DataSource dataSource;

    public HiveProducer(Endpoint endpoint, DataSource dataSource) throws Exception{
        super(endpoint);
        this.dataSource = dataSource;
    }

    @Override
    public HiveEndpoint getEndpoint() {
        return (HiveEndpoint) super.getEndpoint();
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String sqlScript = exchange.getIn().getBody(String.class);
        List<String> sqlList = Arrays.asList(sqlScript.split(";"));
        Connection connection = null;
        boolean shouldCloseResources = true;

        try {
            connection = dataSource.getConnection();
            shouldCloseResources = createAndExecuteSqlStatement(exchange, sqlList, connection);
        } catch (Exception e) {
            LOG.error("Error occured during execute sql script", e);
        } finally {
            if (shouldCloseResources) {
                closeQuietly(connection);
            }
        }
    }

    private boolean createAndExecuteSqlStatement(Exchange exchange, List<String> sqlList, Connection connection) {
        Statement statement = null;
        ResultSet resultSet = null;
        boolean shouldCloseResources = true;

        try {
            statement = connection.createStatement();
            boolean statementExecutionResult;

            if (sqlList != null) {
                for (String sql : sqlList) {
                    if (StringUtils.isEmpty(sql)) {
                        continue;
                    }
                    statementExecutionResult = statement.execute(StringUtils.strip(sql));
                    if (statementExecutionResult) {
                        resultSet = statement.getResultSet();
                        shouldCloseResources = setResultSet(exchange, resultSet);
                    }
                }
            }

            exchange.getOut().getHeaders().putAll(exchange.getIn().getHeaders());
        } catch (SQLException e) {
            LOG.error("Error occured execute sql statement.", e);
        } finally {
            if (shouldCloseResources) {
                closeQuietly(resultSet);
                closeQuietly(statement);
            }
        }

        return shouldCloseResources;
    }

    protected boolean setResultSet(Exchange exchange, ResultSet resultSet) throws SQLException {
        boolean answer = true;
        ResultSetIterator iterator = new ResultSetIterator(resultSet);
        LOG.info("output column names : {}", iterator.getColumnNames());

        if (HiveOutputType.StreamList.equals(getEndpoint().getOutputType())) {
            exchange.getOut().setBody(iterator);
            exchange.addOnCompletion(new ResultSetIteratorCompletion(iterator));
            answer = false;
        }

        return answer;
    }


    private void closeQuietly(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.error("Error by closing result set : " + resultSet, e);
            }
        }
    }

    private void closeQuietly(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.error("Error by closing statement : " + statement, e);
            }
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.error("Error by closing connection : " + connection, e);
            }
        }
    }

    private static final class ResultSetIteratorCompletion implements Synchronization {
        private final ResultSetIterator iterator;

        private ResultSetIteratorCompletion(ResultSetIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public void onComplete(Exchange exchange) {
            iterator.close();
            iterator.closeConnection();
        }

        @Override
        public void onFailure(Exchange exchange) {
            iterator.close();
            iterator.closeConnection();
        }
    }

}
