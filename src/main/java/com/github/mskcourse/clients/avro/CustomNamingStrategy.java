package com.github.mskcourse.clients.avro;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;

public class CustomNamingStrategy implements AWSSchemaNamingStrategy {
    
    public String getSchemaName(String transportName,
            Object data,
            boolean isKey) {
        return isKey ? getSchemaName(transportName) + "-key" : getSchemaName(transportName) + "-value" ;
    }

    /**
     * Returns the schemaName.
     *
     * @param transportName topic Name or stream name etc.
     * @return schema name.
     */
    @Override
    public String getSchemaName(String transportName) {
        return transportName;
    }

}