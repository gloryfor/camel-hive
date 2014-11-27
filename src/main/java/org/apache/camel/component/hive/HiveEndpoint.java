package org.apache.camel.component.hive;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;

import javax.sql.DataSource;

@UriEndpoint(scheme = "hive")
public class HiveEndpoint extends DefaultEndpoint {

    @UriParam
    private DataSource dataSource;

    public HiveEndpoint(String endpointUri, Component component, DataSource dataSource) {
        super(endpointUri, component);
        this.dataSource = dataSource;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new HiveProducer(this, dataSource);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("Not supported operation");
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    protected String createEndpointUri() {
        return "hive";
    }
}
