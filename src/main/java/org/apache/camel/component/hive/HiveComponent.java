package org.apache.camel.component.hive;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.UriEndpointComponent;
import org.apache.camel.util.CamelContextHelper;

import javax.sql.DataSource;
import java.util.Map;

public class HiveComponent extends UriEndpointComponent {

    public HiveComponent() {
        super(HiveEndpoint.class);
    }

    public HiveComponent(CamelContext context) {
        super(context, HiveEndpoint.class);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        DataSource dataSource = CamelContextHelper.mandatoryLookup(getCamelContext(), remaining, DataSource.class);
        HiveEndpoint hiveEndpoint = new HiveEndpoint(uri, this, dataSource);
        setProperties(hiveEndpoint, parameters);
        return hiveEndpoint;
    }

}
