/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.sqlserver;


import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.connector.sqlserver.SqlServerConnector;

import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class SQLServerSource {
    private static final String DATABASE_SERVER_NAME = "sqlserver_binlog_source";

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        private int port = 1433;
        private String hostname;
        private String database;
        private String username;
        private String password;
        private String[] tableList;
        private Properties dbzProperties;
        private DebeziumDeserializationSchema<T> deserializer;

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /**
         * Integer port number of the MySQL database server.
         */
        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        /**
         * An optional list of regular expressions that match database names to be monitored;
         * any database name not included in the whitelist will be excluded from monitoring.
         * By default all databases will be monitored.
         */
        public Builder<T> databaseList(String database) {
            this.database = database;
            return this;
        }

        /**
         * An optional list of regular expressions that match fully-qualified table identifiers
         * for tables to be monitored; any table not included in the list will be excluded from
         * monitoring. Each identifier is of the form databaseName.tableName.
         * By default the connector will monitor every non-system table in each monitored database.
         */
        public Builder<T> tableList(String... tableList) {
            this.tableList = tableList;
            return this;
        }

        /**
         * Name of the MySQL database to use when connecting to the MySQL database server.
         */
        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        /**
         * Password to use when connecting to the MySQL database server.
         */
        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        /**
         * The Debezium MySQL connector properties. For example, "snapshot.mode".
         */
        public Builder<T> debeziumProperties(Properties properties) {
            this.dbzProperties = properties;
            return this;
        }

        /**
         * The deserializer used to convert from consumed {@link org.apache.kafka.connect.source.SourceRecord}.
         */
        public Builder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public DebeziumSourceFunction<T> build() {
            Properties props = new Properties();
            props.setProperty("connector.class", SqlServerConnector.class.getCanonicalName());
            // hard code server name, because we don't need to distinguish it, docs:
            // Logical name that identifies and provides a namespace for the particular MySQL database
            // server/cluster being monitored. The logical name should be unique across all other connectors,
            // since it is used as a prefix for all Kafka topic names emanating from this connector.
            // Only alphanumeric characters and underscores should be used.
            props.setProperty("database.server.name", DATABASE_SERVER_NAME);
            props.setProperty("database.hostname", checkNotNull(hostname));
            props.setProperty("database.user", checkNotNull(username));
            props.setProperty("database.password", checkNotNull(password));
            props.setProperty("database.port", String.valueOf(port));
            props.setProperty("database.dbname", checkNotNull(database));

            if (tableList != null) {
                props.setProperty("table.include.list", String.join(",", tableList));
            }

            if (dbzProperties != null) {
                dbzProperties.forEach(props::put);
            }

            return new DebeziumSourceFunction<>(
                    deserializer,
                    props,
                    null);
        }
    }

}
